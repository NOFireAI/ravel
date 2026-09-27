//! The group-commit audit pipeline (ADR-0062 decision 2).
//!
//! Every query surface must durably record one audit event before its response
//! is released, and the trail must be non-lossy: no acknowledged query may ever
//! lack a durable audit record. Writing one RLOG object plus one commit record
//! per query (the [`crate::query_audit`] path) satisfies durability but at two
//! PUTs per query forever. [`AuditPipeline`] keeps the exact
//! durability guarantee while decoupling the PUT rate from the query rate: it
//! batches submitted [`AuditEvent`]s and flushes on `max_batch` records or
//! `max_age` (default 25 ms), whichever comes first, as one
//! [`write_audit_batch`] call - one object, one commit record, per tenant
//! represented in the batch.
//!
//! # Tenancy
//!
//! One pipeline serves every tenant the process answers queries for, and the
//! tenant is carried on each [`AuditEvent`], never fixed when the pipeline is
//! constructed. A flush groups its batch by that field and issues one write
//! per group, so every record lands under the audit prefix of the tenant whose
//! query produced it. This is not a cosmetic detail: the `audit` SQL table
//! resolves per tenant hash, so a record filed under the wrong tenant's prefix
//! discloses its `query.text` to that other tenant.
//!
//! # Non-lossy by construction
//!
//! The only buffer is the in-memory batch, and nothing in it is ever
//! acknowledged: [`AuditPipeline::submit`] does not return until the batch
//! containing its event has actually flushed to object storage. A crash before
//! flush destroys the buffered records *and* the un-responded queries together,
//! so no acknowledged query is ever left without a durable audit record. This
//! is the strongest property a system can offer without a local durable spool
//! (rejected: durability may not depend on local disk).
//!
//! # Failure posture
//!
//! [`AuditMode`] governs every way a submission can fail to observe a real,
//! durable flush of its own batch, not only a live flush call that itself
//! returned an error: a flush failure, the flush task having already exited
//! (panicked, or drained and stopped), or `submit` being called after
//! [`AuditPipeline::shutdown`]/`Drop` have signaled the pipeline closed. In
//! [`AuditMode::Required`] (the default) every one of those is returned to
//! the submitter, so its response fails closed (HTTP 503 / Flight
//! `Unavailable`). In [`AuditMode::BestEffort`] - the explicit, documented
//! opt-out - every one of them instead resolves `Ok(())`, trading complete
//! audit coverage for availability: a dead or draining pipeline is exactly
//! the kind of audit-plane failure `BestEffort` exists to survive, not a
//! separate class of error exempt from the mode.
//!
//! # Shape and shutdown
//!
//! `submit` sends the event and a `oneshot` completion channel down a bounded
//! `mpsc` to a single background flush task ([`tokio::spawn`]ed at
//! construction) that owns the batch-accumulate-then-flush loop; the flush task
//! signals each submitter's `oneshot` with the batch's outcome. Shutdown is
//! explicit: [`AuditPipeline::shutdown`] signals the flush task to drain and
//! flush whatever is buffered, then awaits it. This does not guarantee every
//! submission enqueued before the signal lands in that final flush: a
//! submission racing the drain, or one still mid-accumulate when the inner
//! loop's own stop path returns without a further drain, instead has its
//! `oneshot` dropped and resolves to an error (or, in [`AuditMode::BestEffort`],
//! `Ok(())`) rather than silently vanishing -- the non-lossy guarantee is that
//! a submitter is never left uninformed, not that every submission is always
//! swept into the last batch. `Drop` also signals the task to drain and flush,
//! but cannot await it, so a caller that needs the final flush observed must
//! call [`AuditPipeline::shutdown`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ravel_commit::{RngSource, SystemRng};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

use crate::audit_write::{AuditRecord, write_audit_batch_with_rng};
use crate::config::{AuditMode, AuditPipelineConfig};
use crate::error::{MaintainError, Result};

/// The submitter-facing event of the audit pipeline: exactly the record content
/// a query surface hands over, including the tenant it is attributed to. The
/// pipeline owns the shard (from its config), mints one `record_id` per
/// per-tenant group of a batch, and performs the flush, so a submitter
/// supplies only the record's own fields. This is [`AuditRecord`] under the
/// pipeline's name.
pub use crate::audit_write::AuditRecord as AuditEvent;

/// Sink every query surface submits one audit event through before releasing
/// its response (ADR-0062 §2a). Mirrors the `QueryCostRecorder` seam in
/// `ravel-types`, except the method is `async` and awaited: submission must not
/// return until the event is durable (or, in [`AuditMode::BestEffort`], until
/// the pipeline has decided to release it anyway).
///
/// The seam lives in `ravel-maintain`, not `ravel-types`, because an audit
/// event turns into an RLOG object and a commit record, which need
/// `ravel-logseg`, `ravel-commit`, and `ravel-object-store` -- three crates
/// `ravel-types` has no dependency on by design. `QueryCostRecorder` can live
/// in `ravel-types` because folding counters needs none of them; this sink
/// cannot. `ravel-query` and `ravel-sql` already depend on `ravel-maintain`, so
/// the sink adds no new dependency edge.
///
/// A caller holds an `Arc<dyn QueryAuditSink>`. A deployment installs an
/// [`AuditPipeline`]; a test or library-only embedding with no pipeline uses
/// [`NoopQueryAuditSink`], so no call site needs an `Option` branch.
#[async_trait::async_trait]
pub trait QueryAuditSink: Send + Sync {
    /// Submit one audit event and await its durability. Returns `Ok(())` once
    /// the event's batch is durable in object storage; in
    /// [`AuditMode::Required`] returns the flush error if the batch failed, and
    /// in [`AuditMode::BestEffort`] returns `Ok(())` even then.
    async fn submit(&self, event: AuditEvent) -> Result<()>;
}

/// A [`QueryAuditSink`] that durably records nothing and always succeeds. It
/// lets a query path with no pipeline configured (every test, any library-only
/// embedding) hold a sink unconditionally instead of an `Option`, mirroring
/// `NoopQueryCostRecorder`. Using it means queries run **unaudited**; it is a
/// test/default stand-in, not a production audit posture.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopQueryAuditSink;

#[async_trait::async_trait]
impl QueryAuditSink for NoopQueryAuditSink {
    async fn submit(&self, _event: AuditEvent) -> Result<()> {
        Ok(())
    }
}

/// One submission travelling from [`AuditPipeline::submit`] to the flush task:
/// the event plus the `oneshot` the flush task signals with the batch outcome.
struct Submission {
    record: AuditRecord,
    done: oneshot::Sender<Result<()>>,
}

/// Group-commit audit pipeline (ADR-0062 §2b). Holds a background flush task
/// and the bounded channel [`submit`](Self::submit) enqueues onto. See the
/// [module docs](self) for the non-lossy guarantee and the shutdown contract.
pub struct AuditPipeline {
    tx: mpsc::Sender<Submission>,
    /// Signals the flush task to drain, flush, and exit. Notified by both
    /// [`shutdown`](Self::shutdown) and `Drop`.
    shutdown: Arc<Notify>,
    /// The flush task's handle, taken by the first [`shutdown`](Self::shutdown).
    join: Mutex<Option<JoinHandle<()>>>,
    /// Fast-path guard so a `submit` after shutdown fails immediately rather
    /// than enqueuing onto a channel no one will drain.
    stopped: Arc<AtomicBool>,
    /// Count of flushes that failed while in [`AuditMode::BestEffort`], for
    /// observability and tests: a best-effort failure is otherwise invisible to
    /// the released query.
    flush_failures: Arc<AtomicU64>,
    /// Count of individual PUT attempts retried after a transient
    /// object-store error (ADR-0062 amendment, 2026-09-27), across every
    /// tenant group this pipeline has flushed. A nonzero count means the
    /// store is degraded even though [`Self::flush_failures`] may still be
    /// zero: the retries absorbed the errors before they reached a submitter.
    put_retries: Arc<AtomicU64>,
    /// A copy of `config.audit_mode`, kept alongside the config the flush task
    /// owns so `submit` can honor the configured failure posture on its own
    /// error paths (pipeline stopped, flush task gone), not only on a flush
    /// call that itself returned an error inside the flush task.
    audit_mode: AuditMode,
}

impl AuditPipeline {
    /// Spawn the flush task and return a pipeline writing to `store`, jittering
    /// its retry backoff (ADR-0062 amendment, 2026-09-27) from OS entropy via
    /// [`SystemRng`]. Every tenant the process serves shares this one
    /// pipeline; each batch is grouped by the tenant carried on its events, so
    /// the pipeline itself holds no tenant. The task runs until
    /// [`shutdown`](Self::shutdown) is called or the pipeline is dropped.
    pub fn spawn(store: Arc<dyn ObjectStoreBackend>, config: AuditPipelineConfig) -> Self {
        Self::spawn_with_rng(store, config, Arc::new(SystemRng))
    }

    /// [`Self::spawn`] with the backoff-jitter source injected (ADR-0068
    /// decision 2), mirroring `ravel_commit::publish`'s `publish`/
    /// `publish_with_rng` split: the simulation harness calls this directly
    /// with a seeded source so retry timing replays deterministically from
    /// its master seed.
    pub fn spawn_with_rng(
        store: Arc<dyn ObjectStoreBackend>,
        config: AuditPipelineConfig,
        rng: Arc<dyn RngSource>,
    ) -> Self {
        let audit_mode = config.audit_mode;
        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));
        let shutdown = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let flush_failures = Arc::new(AtomicU64::new(0));
        let put_retries = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_flush_loop(
            rx,
            store,
            config,
            shutdown.clone(),
            flush_failures.clone(),
            put_retries.clone(),
            rng,
        ));
        AuditPipeline {
            tx,
            shutdown,
            join: Mutex::new(Some(handle)),
            stopped,
            flush_failures,
            put_retries,
            audit_mode,
        }
    }

    /// Submit one event and await the durability of the batch it lands in. See
    /// [`QueryAuditSink::submit`].
    ///
    /// Every error path here -- the pipeline already stopped, the flush task
    /// gone, or the flush task exiting before flushing this event -- is
    /// resolved through [`Self::resolve`], so [`AuditMode::BestEffort`]
    /// releases the submitter with `Ok(())` on all of them, exactly as it does
    /// for a flush call that itself failed. A dead or draining pipeline is an
    /// audit-plane failure like any other, not a separate class exempt from
    /// the configured mode.
    pub async fn submit(&self, event: AuditEvent) -> Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return self.resolve(Err(MaintainError::AuditFlush(
                "audit pipeline is stopped".to_string(),
            )));
        }
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .tx
            .send(Submission {
                record: event,
                done: done_tx,
            })
            .await
            .is_err()
        {
            return self.resolve(Err(MaintainError::AuditFlush(
                "audit pipeline flush task is gone".to_string(),
            )));
        }
        match done_rx.await {
            // The flush task's own result is already mode-resolved (see
            // `flush_batch`): `Ok` in Required-flush-failed became `Err`
            // there, and `Ok` in BestEffort-flush-failed became `Ok` there.
            // Nothing further to do here.
            Ok(result) => result,
            // The flush task dropped our `oneshot` without sending: it exited
            // (shutdown/close) before flushing this event. The event was never
            // acknowledged.
            Err(_) => self.resolve(Err(MaintainError::AuditFlush(
                "audit pipeline stopped before this event flushed".to_string(),
            ))),
        }
    }

    /// Apply [`AuditMode`] to an error this pipeline itself produced (as
    /// opposed to one already resolved by the flush task): `Required` returns
    /// it unchanged, `BestEffort` counts it in [`Self::flush_failures`], logs
    /// it, and releases the caller with `Ok(())`.
    fn resolve(&self, result: Result<()>) -> Result<()> {
        match (self.audit_mode, result) {
            (AuditMode::Required, result) => result,
            (AuditMode::BestEffort, Ok(())) => Ok(()),
            (AuditMode::BestEffort, Err(e)) => {
                self.flush_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "audit pipeline: releasing submitter in best-effort mode after a pipeline-level failure");
                Ok(())
            }
        }
    }

    /// Stop the pipeline cleanly: signal the flush task to drain and flush
    /// whatever is buffered, then await its exit. Idempotent, but only the
    /// first caller actually awaits the flush task's exit -- it takes the
    /// task's `JoinHandle` out of `self.join`, so a concurrent second call
    /// finds it already taken and returns `Ok(())` immediately without
    /// waiting. Callers that must know the flush task has actually finished
    /// (not just that some `shutdown` call returned) should not rely on a
    /// concurrent second call for that; only the call that actually awaited
    /// the handle observed it.
    pub async fn shutdown(&self) -> Result<()> {
        self.stopped.store(true, Ordering::SeqCst);
        self.shutdown.notify_one();
        let handle = self.join.lock().await.take();
        if let Some(handle) = handle {
            handle.await.map_err(|e| {
                MaintainError::AuditFlush(format!("audit flush task panicked: {e}"))
            })?;
        }
        Ok(())
    }

    /// Number of flushes that failed while in [`AuditMode::BestEffort`] and were
    /// released anyway. Zero in [`AuditMode::Required`] (a failure there is
    /// returned to the submitter, not counted here).
    pub fn flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Relaxed)
    }

    /// Number of individual PUT attempts retried after a transient
    /// object-store error, across every batch this pipeline has flushed. A
    /// nonzero count means the store is degraded even though
    /// [`Self::flush_failures`] may still be zero: the retries absorbed the
    /// errors before they reached a submitter. Exported on `/metrics` as
    /// `ravel_audit_put_retries_total`.
    pub fn put_retries(&self) -> u64 {
        self.put_retries.load(Ordering::Relaxed)
    }
}

impl Drop for AuditPipeline {
    fn drop(&mut self) {
        // Signal the flush task so a pipeline dropped without an explicit
        // `shutdown()` still drains and flushes its buffer rather than leaking
        // it. We cannot await the task here; a caller that must observe the
        // final flush result calls `shutdown().await` first. Dropping `tx`
        // (after this body) also closes the channel, which the task treats as a
        // drain-and-exit signal, so either path flushes the buffer.
        self.stopped.store(true, Ordering::SeqCst);
        self.shutdown.notify_one();
    }
}

#[async_trait::async_trait]
impl QueryAuditSink for AuditPipeline {
    async fn submit(&self, event: AuditEvent) -> Result<()> {
        // Resolves to the inherent method (inherent methods win over trait
        // methods in resolution); this is the trait-object entry point.
        AuditPipeline::submit(self, event).await
    }
}

/// The background flush loop: accumulate a batch until `max_batch` records or
/// `max_age`, flush it, repeat, until a stop signal or the channel closing.
async fn run_flush_loop(
    mut rx: mpsc::Receiver<Submission>,
    store: Arc<dyn ObjectStoreBackend>,
    config: AuditPipelineConfig,
    shutdown: Arc<Notify>,
    flush_failures: Arc<AtomicU64>,
    put_retries: Arc<AtomicU64>,
    rng: Arc<dyn RngSource>,
) {
    loop {
        // Wait for the first submission of a new batch, or a stop signal.
        let first = tokio::select! {
            biased;
            _ = shutdown.notified() => {
                // Drain anything already queued and flush it before exiting, so
                // a clean stop never discards buffered records.
                let mut batch = Vec::new();
                while let Ok(submission) = rx.try_recv() {
                    batch.push(submission);
                }
                if !batch.is_empty() {
                    flush_batch(store.as_ref(), &config, &flush_failures, &put_retries, rng.as_ref(), batch).await;
                }
                return;
            }
            recv = rx.recv() => match recv {
                Some(submission) => submission,
                // All senders dropped and the buffer is empty: clean exit.
                None => return,
            },
        };

        let mut batch = vec![first];
        let deadline = Instant::now() + config.max_age;
        let mut stop = false;

        while batch.len() < config.max_batch {
            tokio::select! {
                biased;
                _ = shutdown.notified() => {
                    while let Ok(submission) = rx.try_recv() {
                        batch.push(submission);
                        if batch.len() >= config.max_batch {
                            break;
                        }
                    }
                    stop = true;
                    break;
                }
                recv = rx.recv() => match recv {
                    Some(submission) => batch.push(submission),
                    // Senders dropped mid-batch: flush what we have, then exit.
                    None => {
                        stop = true;
                        break;
                    }
                },
                _ = sleep_until(deadline) => break,
            }
        }

        flush_batch(
            store.as_ref(),
            &config,
            &flush_failures,
            &put_retries,
            rng.as_ref(),
            batch,
        )
        .await;
        if stop {
            return;
        }
    }
}

/// Flush one accumulated batch and signal every submitter with the outcome of
/// its own write, per the configured [`AuditMode`].
///
/// The batch is grouped by each event's tenant and written one group at a
/// time, each as a single object+commit pair under that tenant's own audit
/// prefix with its own fresh `record_id`. A group's outcome reaches only the
/// submitters whose events were in it, so one tenant's failed write neither
/// fails nor silently releases another tenant's queries.
async fn flush_batch(
    store: &dyn ObjectStoreBackend,
    config: &AuditPipelineConfig,
    flush_failures: &AtomicU64,
    put_retries: &AtomicU64,
    rng: &dyn RngSource,
    batch: Vec<Submission>,
) {
    // `BTreeMap` rather than a hash map so a multi-tenant batch flushes in a
    // deterministic tenant order, which keeps a test's PUT sequence stable.
    let mut groups: std::collections::BTreeMap<TenantHash, (Vec<AuditRecord>, Vec<_>)> =
        std::collections::BTreeMap::new();
    for submission in batch {
        let entry = groups.entry(submission.record.tenant).or_default();
        entry.0.push(submission.record);
        entry.1.push(submission.done);
    }

    for (tenant, (records, dones)) in groups {
        let record_id = Uuid::new_v4();
        let outcome = write_audit_batch_with_rng(
            store,
            config.shard,
            record_id,
            records,
            Some(put_retries),
            rng,
        )
        .await;

        match outcome {
            Ok(()) => {
                for done in dones {
                    let _ = done.send(Ok(()));
                }
            }
            Err(error) => {
                let message = error.to_string();
                match config.audit_mode {
                    AuditMode::Required => {
                        tracing::error!(
                            error = %message,
                            tenant = %tenant.to_hex(),
                            batch_size = dones.len(),
                            "audit batch flush failed; failing every awaiting query (audit_mode=required)"
                        );
                        for done in dones {
                            let _ = done.send(Err(MaintainError::AuditFlush(message.clone())));
                        }
                    }
                    AuditMode::BestEffort => {
                        flush_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            error = %message,
                            tenant = %tenant.to_hex(),
                            batch_size = dones.len(),
                            "audit batch flush failed; releasing every awaiting query anyway (audit_mode=best-effort)"
                        );
                        for done in dones {
                            let _ = done.send(Ok(()));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta,
        ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError, list_all,
    };
    use ravel_types::Signal;
    use ravel_types::logstream::log_stream_id;

    use crate::query_audit::QUERY_AUDIT_SHARD;

    /// `t/<hex>/u/l0/<shard>/` - every L0 data object for the query-audit shard.
    /// No public builder for this prefix exists in `ravel-commit`, so it is
    /// constructed here from the same pieces `keys::data_key` uses (Audit's
    /// signal prefix is `u`, shards are 4-digit zero-padded).
    fn audit_l0_data_prefix(tenant: &TenantHash) -> String {
        format!("t/{}/u/l0/{:04}/", tenant.to_hex(), QUERY_AUDIT_SHARD)
    }

    /// A distinct log stream per `seed`, so a batch of these has a known
    /// distinct-series count.
    fn test_stream(seed: u32) -> (LogStreamId, Vec<u8>) {
        let resource = vec![(
            "ravel.record_type".to_string(),
            AttrValue::Str(format!("audit_test_{seed}")),
        )];
        let id = log_stream_id(&resource, "ravel.audit_test", "1", &[]);
        let blob = stream_attrs_bytes(&resource, "ravel.audit_test", "1", &[]);
        (id, blob)
    }

    fn test_event(tenant: TenantHash, now_ns: i64, stream_seed: u32) -> AuditEvent {
        let (stream_id, stream_attrs) = test_stream(stream_seed);
        AuditEvent {
            tenant,
            now_ns,
            stream_id,
            stream_attrs,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "audit test".to_string(),
            attrs: vec![("kind".to_string(), AttrValue::Str("query".into()))],
        }
    }

    /// Number of commit records on the query-audit shard: one per flushed batch.
    async fn commit_record_count(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> usize {
        let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, QUERY_AUDIT_SHARD).unwrap();
        list_all(store, &prefix).await.unwrap().len()
    }

    /// Number of L0 data objects on the query-audit shard: one per flushed batch.
    async fn data_object_count(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> usize {
        list_all(store, &audit_l0_data_prefix(tenant))
            .await
            .unwrap()
            .len()
    }

    fn pipeline_config(max_batch: usize, max_age: Duration) -> AuditPipelineConfig {
        AuditPipelineConfig {
            max_batch,
            max_age,
            shard: QUERY_AUDIT_SHARD,
            audit_mode: AuditMode::Required,
            channel_capacity: 1024,
        }
    }

    /// A backend whose first `put` matching `key_contains` applies a
    /// different payload for real (as if an unrelated writer had already
    /// occupied the key) and reports the caller's own attempt as a
    /// transient error, then passes every later call straight through.
    /// `FaultStore`'s scripted faults cannot express this: every one of them
    /// either leaves the wrapped backend untouched or applies the caller's
    /// own bytes, never a third party's, so a genuine collision (as opposed
    /// to a duplicate delivery of the caller's own write) needs this
    /// purpose-built wrapper instead.
    struct DivergentRetryStore<S> {
        inner: S,
        key_contains: &'static str,
        fired: AtomicBool,
    }

    impl<S> DivergentRetryStore<S> {
        fn new(inner: S, key_contains: &'static str) -> Self {
            DivergentRetryStore {
                inner,
                key_contains,
                fired: AtomicBool::new(false),
            }
        }

        fn fired(&self) -> bool {
            self.fired.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for DivergentRetryStore<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> std::result::Result<PutOutcome, StoreError> {
            if key.contains(self.key_contains) && !self.fired.swap(true, Ordering::SeqCst) {
                self.inner
                    .put(
                        key,
                        Bytes::from_static(b"an unrelated commit record from a different writer"),
                        PutOptions::create_if_absent(),
                    )
                    .await?;
                return Err(StoreError::Transient(
                    "fault: simulated ack loss after a divergent write landed under the same key"
                        .into(),
                ));
            }
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> std::result::Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> std::result::Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn pipeline_flushes_on_reaching_max_batch_before_max_age() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([1u8; 16]);
        // max_batch=3, a very long max_age: only reaching the count can flush.
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        // Submit exactly max_batch events concurrently; the batch fills and
        // flushes, so all three submits return without the timer elapsing.
        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 1_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle.await.expect("submit task").expect("submit ok");
        }

        assert_eq!(
            data_object_count(store.as_ref(), &tenant).await,
            1,
            "exactly one data object for the one flushed batch"
        );
        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "exactly one commit record for the one flushed batch"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn pipeline_flushes_on_max_age_with_a_partial_batch() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([2u8; 16]);
        // Large max_batch, short max_age: one event must flush on the timer
        // without waiting for more events.
        let config = pipeline_config(1000, Duration::from_millis(20));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        pipeline
            .submit(test_event(tenant, 5_000, 7))
            .await
            .expect("submit flushes on max_age");

        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "the lone event flushed on the age deadline"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn every_submit_in_a_failed_batch_errors_in_required_mode() {
        let mem = MemoryStore::new();
        // Fail every data-object PUT (the l0 data write is the first PUT of a
        // flush), so the whole batch's flush fails.
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Put, ScriptedFault::Timeout).with_key_contains("/l0/"));
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([3u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 9_000 + i, 7)).await
            }));
        }
        for handle in handles {
            let result = handle.await.expect("submit task");
            assert!(
                matches!(result, Err(MaintainError::AuditFlush(_))),
                "every submit in a failed batch must observe the flush error in required mode, got {result:?}"
            );
        }
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            3,
            "every attempt (1 try + 2 retries) hit the always-on fault before failing closed"
        );
        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            0,
            "no commit record when the flush's data PUT failed"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn best_effort_releases_queries_despite_a_failed_flush() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Put, ScriptedFault::Timeout).with_key_contains("/l0/"));
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([4u8; 16]);
        let config = AuditPipelineConfig {
            max_batch: 3,
            max_age: Duration::from_secs(3600),
            shard: QUERY_AUDIT_SHARD,
            audit_mode: AuditMode::BestEffort,
            channel_capacity: 1024,
        };
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 11_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("best-effort releases the query with Ok despite the failed flush");
        }
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            3,
            "every attempt (1 try + 2 retries) hit the always-on fault before failing closed"
        );
        assert_eq!(
            pipeline.flush_failures(),
            1,
            "the best-effort failure was counted, not silently vanished"
        );
        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            0,
            "best-effort released the queries but nothing durable landed"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn shutdown_flushes_still_buffered_events() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([5u8; 16]);
        // Large max_batch and max_age, so nothing flushes on its own: only the
        // shutdown drain can flush the two buffered events.
        let config = pipeline_config(1000, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        // Both events must be queued before the shutdown drain runs, or the
        // drain has nothing to flush and this test asserts nothing. `submit`
        // sends into the flush task's channel and only then awaits its
        // completion oneshot, so polling each future exactly once puts both
        // events in the queue and leaves both callers pending -- the state
        // this test needs, established by construction rather than by giving
        // two spawned tasks 50 ms of wall clock to get there.
        let mut submissions: Vec<_> = (0..2)
            .map(|i| Box::pin(pipeline.submit(test_event(tenant, 13_000 + i, 7))))
            .collect();
        for submission in &mut submissions {
            let first = std::future::poll_fn(|cx| Poll::Ready(submission.as_mut().poll(cx))).await;
            assert!(
                first.is_pending(),
                "a buffered submit stays pending until its batch flushes"
            );
        }

        pipeline
            .shutdown()
            .await
            .expect("shutdown drains and flushes");

        for submission in submissions {
            submission
                .await
                .expect("a buffered event is flushed by shutdown, not discarded");
        }
        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "shutdown flushed the buffered events as one batch"
        );
        assert_eq!(
            data_object_count(store.as_ref(), &tenant).await,
            1,
            "one data object for the shutdown-flushed batch"
        );
    }

    #[tokio::test]
    async fn submit_after_shutdown_errors() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([6u8; 16]);
        let pipeline = AuditPipeline::spawn(
            store.clone(),
            pipeline_config(1000, Duration::from_secs(3600)),
        );
        pipeline.shutdown().await.expect("shutdown");
        let result = pipeline.submit(test_event(tenant, 1, 7)).await;
        assert!(
            matches!(result, Err(MaintainError::AuditFlush(_))),
            "a submit after shutdown must error, got {result:?}"
        );
    }

    /// The Stage 4 checkpoint's blocking finding: `submit`'s own error paths
    /// (pipeline already stopped, flush task gone) must honor `audit_mode`
    /// exactly as a flush-call failure does, not fail closed unconditionally.
    /// A dead or draining pipeline is an audit-plane failure `BestEffort`
    /// exists to survive, same as a failed flush.
    #[tokio::test]
    async fn best_effort_survives_submit_after_shutdown() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([7u8; 16]);
        let config = AuditPipelineConfig {
            max_batch: 1000,
            max_age: Duration::from_secs(3600),
            shard: QUERY_AUDIT_SHARD,
            audit_mode: AuditMode::BestEffort,
            channel_capacity: 1024,
        };
        let pipeline = AuditPipeline::spawn(store.clone(), config);
        pipeline.shutdown().await.expect("shutdown");

        let result = pipeline.submit(test_event(tenant, 1, 7)).await;
        assert!(
            result.is_ok(),
            "best-effort must release a submit after shutdown with Ok, not fail closed \
             like required mode; got {result:?}"
        );
        assert_eq!(
            pipeline.flush_failures(),
            1,
            "the post-shutdown release must still be counted, not silently vanished"
        );
    }

    #[tokio::test]
    async fn noop_sink_is_object_safe_and_always_ok() {
        let sink: Arc<dyn QueryAuditSink> = Arc::new(NoopQueryAuditSink);
        sink.submit(test_event(TenantHash([8u8; 16]), 1, 7))
            .await
            .expect("noop is ok");
    }

    /// The blocking finding this fix round closes: one pipeline serves every
    /// tenant, so a batch that mixes two tenants' events must produce one
    /// object under each tenant's own prefix, never both under one. Filed
    /// wrongly, tenant one's `query.text` becomes readable by tenant two,
    /// because the `audit` SQL table resolves per tenant hash.
    #[tokio::test]
    async fn one_batch_spanning_two_tenants_writes_one_object_under_each() {
        let store = Arc::new(MemoryStore::new());
        let one = TenantHash([31u8; 16]);
        let two = TenantHash([32u8; 16]);
        // max_batch=4 with a long max_age: the four events below fill one
        // batch and flush together, so the grouping runs on a genuinely mixed
        // batch rather than on four separate single-tenant flushes.
        let config = pipeline_config(4, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for (i, tenant) in [one, two, one, two].into_iter().enumerate() {
            let pipeline = pipeline.clone();
            let now_ns = 17_000 + i as i64;
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, now_ns, 7)).await
            }));
        }
        for handle in handles {
            handle.await.expect("submit task").expect("submit ok");
        }

        for tenant in [one, two] {
            assert_eq!(
                data_object_count(store.as_ref(), &tenant).await,
                1,
                "exactly one data object under {}'s own audit prefix",
                tenant.to_hex()
            );
            assert_eq!(
                commit_record_count(store.as_ref(), &tenant).await,
                1,
                "exactly one commit record under {}'s own audit prefix",
                tenant.to_hex()
            );
        }
        // Two objects total, so neither tenant's group leaked into the other's
        // prefix and no third prefix was written.
        assert_eq!(
            list_all(store.as_ref(), "t/").await.unwrap().len(),
            4,
            "two data objects and two commit records, one pair per tenant"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// A per-tenant group's flush outcome reaches only that group's
    /// submitters: one tenant's failed write must not fail the other tenant's
    /// query, and must not release its own.
    #[tokio::test]
    async fn a_failed_group_fails_only_its_own_tenants_submitters() {
        let one = TenantHash([33u8; 16]);
        let two = TenantHash([34u8; 16]);
        // Fail only the data PUT under tenant `one`'s prefix. Keyed on that
        // tenant's hex prefix, so tenant `two`'s group in the same batch
        // writes normally.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains(format!("t/{}/u/l0/", one.to_hex())),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let config = pipeline_config(2, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for (i, tenant) in [one, two].into_iter().enumerate() {
            let pipeline = pipeline.clone();
            let now_ns = 19_000 + i as i64;
            handles.push(tokio::spawn(async move {
                (tenant, pipeline.submit(test_event(tenant, now_ns, 7)).await)
            }));
        }
        let mut failed = 0usize;
        let mut released = 0usize;
        for handle in handles {
            let (tenant, result) = handle.await.expect("submit task");
            if tenant == one {
                assert!(
                    matches!(result, Err(MaintainError::AuditFlush(_))),
                    "the tenant whose write was faulted must fail closed, got {result:?}"
                );
                failed += 1;
            } else {
                result.expect("the other tenant's group wrote successfully");
                released += 1;
            }
        }
        assert_eq!(failed, 1, "exactly one submitter failed closed");
        assert_eq!(released, 1, "exactly one submitter was released");

        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            3,
            "every attempt (1 try + 2 retries) on the faulted tenant's data PUT hit the \
             always-on fault before failing closed"
        );
        assert_eq!(
            commit_record_count(store.as_ref(), &one).await,
            0,
            "the faulted tenant has no commit record"
        );
        assert_eq!(
            commit_record_count(store.as_ref(), &two).await,
            1,
            "the other tenant's record is durable despite the sibling group's failure"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// ADR-0062 amendment (2026-09-27): a single transient timeout on the
    /// data-object PUT must not fail the batch, because the object store's
    /// own client-side retry does not cover a conditional (`create_if_absent`)
    /// PUT. Only the very first attempt fails, via `Occurrence::Nth(1)`; the
    /// retry succeeds and the batch completes normally.
    #[tokio::test]
    async fn a_transient_timeout_on_the_first_data_put_is_retried_and_the_batch_succeeds() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("/l0/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([41u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 21_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("a retried transient timeout must not fail the batch");
        }

        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "exactly one commit record for the one flushed, retried batch"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            1,
            "the injected fault fired exactly once (only the first attempt)"
        );
        assert_eq!(
            pipeline.put_retries(),
            1,
            "the one retry against the data-object PUT was counted"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// Same as above, but the transient timeout hits the commit-record PUT
    /// instead of the data-object PUT.
    #[tokio::test]
    async fn a_transient_timeout_on_the_first_commit_put_is_retried_and_the_batch_succeeds() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("/u/c/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([42u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 22_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("a retried transient timeout must not fail the batch");
        }

        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "exactly one commit record for the one flushed, retried batch"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Timeout),
            1,
            "the injected fault fired exactly once (only the first attempt)"
        );
        assert_eq!(
            pipeline.put_retries(),
            1,
            "the one retry against the commit-record PUT was counted"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// [`AuditPipeline::spawn_with_rng`] threads a seeded source all the way
    /// down to [`write_audit_batch_with_rng`]'s jitter: this only proves the
    /// seam is wired end to end (the retry still succeeds), not any specific
    /// timing, since a `SeededRng`'s exact draw sequence is not part of this
    /// test's contract.
    #[tokio::test]
    async fn spawn_with_rng_threads_a_seeded_rng_through_the_retry_backoff() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("/l0/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([44u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn_with_rng(
            store.clone(),
            config,
            Arc::new(ravel_commit::SeededRng::new(7)),
        ));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 23_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("a retried transient timeout must not fail the batch");
        }

        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "exactly one commit record for the one flushed, retried batch"
        );
        assert_eq!(
            pipeline.put_retries(),
            1,
            "the one retry against the data-object PUT was counted"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// The "landed then timed out" case: the object store applies the
    /// commit-record PUT for real but the caller never sees the
    /// acknowledgement (`ScriptedFault::DuplicateDelivery` calls the wrapped
    /// `put` for real, then reports `Transient` -- exactly what a client
    /// times out on after the request actually completed server-side). The
    /// retry's own `create_if_absent` attempt then sees `AlreadyExists` for
    /// its own earlier write, not a genuine collision: byte-comparing the
    /// existing object against what it was about to write must recognize
    /// they match and treat the retry as a success, landing exactly one
    /// commit record rather than erroring or double-writing.
    #[tokio::test]
    async fn a_lost_ack_on_the_commit_put_is_recognized_as_its_own_earlier_write_on_retry() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
                .with_key_contains("/u/c/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([43u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 23_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("a retry that finds its own earlier write must succeed, not error");
        }

        assert_eq!(
            commit_record_count(store.as_ref(), &tenant).await,
            1,
            "the commit record that landed on the first attempt, not a second write"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::DuplicateDelivery),
            1,
            "the injected lost-ack fault fired exactly once"
        );
        assert_eq!(
            pipeline.put_retries(),
            1,
            "the retry that discovered the earlier write's AlreadyExists was counted"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// A retry that finds a commit object under its key with DIFFERENT
    /// content is a genuine collision, not its own earlier write landing, and
    /// must fail closed rather than assume success.
    #[tokio::test]
    async fn a_retry_finding_a_different_commit_object_under_the_key_errors() {
        let backend = Arc::new(DivergentRetryStore::new(MemoryStore::new(), "/u/c/"));
        let tenant = TenantHash([44u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(backend.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 24_000 + i, 7)).await
            }));
        }
        for handle in handles {
            let result = handle.await.expect("submit task");
            match &result {
                Err(MaintainError::AuditFlush(message)) => {
                    assert!(
                        message.contains("after a retry"),
                        "must fail via the retry-divergence path, got: {message}"
                    );
                }
                other => {
                    panic!("a genuine collision found on retry must fail closed, got {other:?}")
                }
            }
        }
        assert!(
            backend.fired(),
            "the divergent write must actually have landed before the retry observed it"
        );
        pipeline.shutdown().await.expect("shutdown");
    }

    /// A non-retryable error (`AccessDenied`-class; `FaultStore` scripts this
    /// as `Permanent`, which is excluded from `StoreError::is_retryable()`
    /// the same as `AccessDenied`) must not be retried: exactly one attempt,
    /// and the batch fails closed.
    #[tokio::test]
    async fn a_non_retryable_error_is_not_retried_and_fails_the_batch() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("fault: access denied".into()),
            )
            .with_key_contains("/l0/"),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let tenant = TenantHash([45u8; 16]);
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(tenant, 25_000 + i, 7)).await
            }));
        }
        for handle in handles {
            let result = handle.await.expect("submit task");
            assert!(
                matches!(result, Err(MaintainError::AuditFlush(_))),
                "a non-retryable error must still fail the batch, got {result:?}"
            );
        }
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Permanent),
            1,
            "a non-retryable error must not be retried: exactly one attempt"
        );
        assert_eq!(
            pipeline.put_retries(),
            0,
            "no retry was counted for a non-retryable error"
        );
        pipeline.shutdown().await.expect("shutdown");
    }
}
