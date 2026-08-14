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
//! [`write_audit_batch`] call - one object, one commit record, for the whole
//! batch.
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

use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

use crate::audit_write::{AuditRecord, write_audit_batch};
use crate::config::{AuditMode, AuditPipelineConfig};
use crate::error::{MaintainError, Result};

/// The submitter-facing event of the audit pipeline: exactly the record content
/// a query surface hands over. The pipeline owns the shard (from its config),
/// mints one `record_id` per batch, and performs the flush, so a submitter
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
    /// A copy of `config.audit_mode`, kept alongside the config the flush task
    /// owns so `submit` can honor the configured failure posture on its own
    /// error paths (pipeline stopped, flush task gone), not only on a flush
    /// call that itself returned an error inside the flush task.
    audit_mode: AuditMode,
}

impl AuditPipeline {
    /// Spawn the flush task and return a pipeline writing every batch for
    /// `tenant` to `store`. The task runs until [`shutdown`](Self::shutdown) is
    /// called or the pipeline is dropped.
    pub fn spawn(
        store: Arc<dyn ObjectStoreBackend>,
        tenant: TenantHash,
        config: AuditPipelineConfig,
    ) -> Self {
        let audit_mode = config.audit_mode;
        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));
        let shutdown = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let flush_failures = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_flush_loop(
            rx,
            store,
            tenant,
            config,
            shutdown.clone(),
            flush_failures.clone(),
        ));
        AuditPipeline {
            tx,
            shutdown,
            join: Mutex::new(Some(handle)),
            stopped,
            flush_failures,
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
    tenant: TenantHash,
    config: AuditPipelineConfig,
    shutdown: Arc<Notify>,
    flush_failures: Arc<AtomicU64>,
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
                    flush_batch(store.as_ref(), &tenant, &config, &flush_failures, batch).await;
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

        flush_batch(store.as_ref(), &tenant, &config, &flush_failures, batch).await;
        if stop {
            return;
        }
    }
}

/// Flush one accumulated batch as a single object+commit write and signal every
/// submitter with the outcome, per the configured [`AuditMode`].
async fn flush_batch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    config: &AuditPipelineConfig,
    flush_failures: &AtomicU64,
    batch: Vec<Submission>,
) {
    if batch.is_empty() {
        return;
    }
    let mut records = Vec::with_capacity(batch.len());
    let mut dones = Vec::with_capacity(batch.len());
    for submission in batch {
        records.push(submission.record);
        dones.push(submission.done);
    }

    let record_id = Uuid::new_v4();
    let outcome = write_audit_batch(store, tenant, config.shard, record_id, records).await;

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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use std::time::Duration;

    use ravel_commit::keys;
    use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, list_all};
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

    fn test_event(now_ns: i64, stream_seed: u32) -> AuditEvent {
        let (stream_id, stream_attrs) = test_stream(stream_seed);
        AuditEvent {
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

    #[tokio::test]
    async fn pipeline_flushes_on_reaching_max_batch_before_max_age() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantHash([1u8; 16]);
        // max_batch=3, a very long max_age: only reaching the count can flush.
        let config = pipeline_config(3, Duration::from_secs(3600));
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), tenant, config));

        // Submit exactly max_batch events concurrently; the batch fills and
        // flushes, so all three submits return without the timer elapsing.
        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(1_000 + i, 7)).await
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
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), tenant, config));

        pipeline
            .submit(test_event(5_000, 7))
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
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), tenant, config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(9_000 + i, 7)).await
            }));
        }
        for handle in handles {
            let result = handle.await.expect("submit task");
            assert!(
                matches!(result, Err(MaintainError::AuditFlush(_))),
                "every submit in a failed batch must observe the flush error in required mode, got {result:?}"
            );
        }
        assert!(
            store.fault_count(Op::Put, FaultKind::Timeout) >= 1,
            "the injected data-object PUT fault must have fired"
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
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), tenant, config));

        let mut handles = Vec::new();
        for i in 0..3 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(11_000 + i, 7)).await
            }));
        }
        for handle in handles {
            handle
                .await
                .expect("submit task")
                .expect("best-effort releases the query with Ok despite the failed flush");
        }
        assert!(
            store.fault_count(Op::Put, FaultKind::Timeout) >= 1,
            "the injected data-object PUT fault must have fired"
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
        let pipeline = Arc::new(AuditPipeline::spawn(store.clone(), tenant, config));

        // Submit in the background; these stay buffered (below max_batch, before
        // max_age) so their `submit` futures are still pending.
        let mut handles = Vec::new();
        for i in 0..2 {
            let pipeline = pipeline.clone();
            handles.push(tokio::spawn(async move {
                pipeline.submit(test_event(13_000 + i, 7)).await
            }));
        }
        // Give the two submissions time to reach the flush task's buffer.
        tokio::time::sleep(Duration::from_millis(50)).await;

        pipeline
            .shutdown()
            .await
            .expect("shutdown drains and flushes");

        for handle in handles {
            handle
                .await
                .expect("submit task")
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
            tenant,
            pipeline_config(1000, Duration::from_secs(3600)),
        );
        pipeline.shutdown().await.expect("shutdown");
        let result = pipeline.submit(test_event(1, 7)).await;
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
        let pipeline = AuditPipeline::spawn(store.clone(), tenant, config);
        pipeline.shutdown().await.expect("shutdown");

        let result = pipeline.submit(test_event(1, 7)).await;
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
        sink.submit(test_event(1, 7)).await.expect("noop is ok");
    }
}
