//! Owns the log shard actors and fans writes out to them, the log-pipeline
//! counterpart of [`crate::router`] (docs/ingest.md "Structure").
//!
//! Unlike [`crate::router::IngestRouter`], which takes a `Signal` because the
//! metrics/remote-write paths reuse it, this router bakes in [`Signal::Logs`]:
//! it has exactly one caller shape, so an unused parameter would only invite a
//! wrong value.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_logseg::{Bitmap, ColumnarLogBatch, DynColumn};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::AttrValue;
use ravel_types::{CommitToken, TenantHash, shard_for_log};
use tokio::sync::{mpsc, oneshot};

use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};
use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed, load_generations};
use crate::indexed_fields::IndexedFieldsOverlay;
use crate::log_error::LogWriteError;
use crate::log_metrics::LogIngestMetrics;
use crate::log_shard::{LogShardActor, LogShardMsg, est_columnar_bytes, est_record_bytes};
use crate::router::WriteMode;
#[cfg(feature = "stage-timing")]
use crate::stage_timing::{LogStage, LogStageTimings};

/// Resolves the POSTINGS indexed-field list for a tenant at flush time
/// (ADR-0049 decision 3). The shard actor calls this once per
/// object, just before building the writer, and hands the result to
/// `RlogWriter::with_indexed_fields`.
///
/// It is a trait here so `ravel-ingest` does not depend on the server's
/// per-tenant configuration types: the server implements it for its
/// `IndexedFieldConfig`, and a deployment that wires no configuration gets
/// [`NoIndexedFields`], for which every object is unindexed (absence of a
/// POSTINGS section is always legal, ADR-0049 decision 5).
pub trait LogIndexedFields: Send + Sync {
    /// The indexed-field names for `tenant`, or an empty list to index nothing.
    fn fields_for(&self, tenant: &TenantHash) -> Vec<String>;
}

/// The default resolver: no tenant indexes any field, so the writer emits no
/// POSTINGS section. This is the behaviour of every call site that has not
/// wired per-tenant configuration, which is exactly what the writer did before
/// per-tenant configuration existed.
pub struct NoIndexedFields;

impl LogIndexedFields for NoIndexedFields {
    fn fields_for(&self, _tenant: &TenantHash) -> Vec<String> {
        Vec::new()
    }
}

/// One token per shard the request's records flushed through. Empty in
/// buffered mode, or if the request carried no records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogWriteReceipt {
    pub tokens: Vec<CommitToken>,
}

/// A duplicate of [`crate::router`]'s private `ShardHandle`: five fields, so
/// duplicating is cheaper than making the metrics module's struct
/// `pub(crate)` across an unrelated boundary for one shared shape.
struct LogShardHandle {
    tx: mpsc::Sender<LogShardMsg>,
    /// Set once the router first observes this shard's channel closed. The
    /// actor is never restarted, so this only flips false to true; it dedups
    /// the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
}

/// Routes log writes to generation-versioned shard-actor sets (ADR-0052), the
/// log-pipeline counterpart of [`crate::router::IngestRouter`]. The generation-0
/// set is spawned at construction; a reshard's activation spawns the new set
/// lazily via the [`GenerationSwitch`] factory while the old set drains.
pub struct LogIngestRouter {
    switch: GenerationSwitch<LogShardHandle>,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    metrics: Arc<LogIngestMetrics>,
    /// The durable-override indexed-field overlay (ADR-0079), shared by `Arc`
    /// with every shard's flush context (via the [`GenerationSwitch`] factory).
    /// The router holds its own clone only so the idle-tenant sweep can evict its
    /// per-tenant cache (ADR-0069 decision 2) alongside the generation views.
    indexed_fields: Arc<IndexedFieldsOverlay>,
    config: IngestConfig,
    /// Process-wide ingest buffer byte budget (ADR-0069 decision 1), shared by
    /// `Arc` with the metrics and span routers. Defaults to `Unlimited`;
    /// `services/ravel-server` installs the configured budget via
    /// [`LogIngestRouter::with_budget`].
    budget: Arc<IngestByteBudget>,
    /// Per-stage timing accumulator (ADR-0104 decision 1), shared by `Arc` with
    /// every shard actor and flush task so the seam records into one table the
    /// bench reporter reads via [`LogIngestRouter::stage_timings`]. Present only
    /// under the `stage-timing` feature; with it off this field, and every
    /// timing site, is compiled out.
    #[cfg(feature = "stage-timing")]
    stage_timings: Arc<LogStageTimings>,
}

impl LogIngestRouter {
    /// Builds a router whose shards index no POSTINGS field
    /// ([`NoIndexedFields`]). Use [`Self::new_with_indexed_fields`] to wire
    /// per-tenant configuration.
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::new_with_indexed_fields(
            config,
            store,
            clock,
            Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
        )
    }

    /// Like [`Self::new`], but every shard resolves each tenant's POSTINGS
    /// indexed-field list through `indexed_fields` at flush time (ADR-0049
    /// decision 3, ADR-0079). This is the production constructor; the server
    /// wraps its CLI-derived `IndexedFieldConfig` in an [`IndexedFieldsOverlay`]
    /// (so a durable `TenantConfig.indexed_fields` override is read without a
    /// restart) and passes it here.
    pub fn new_with_indexed_fields(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        indexed_fields: Arc<IndexedFieldsOverlay>,
    ) -> Self {
        // Production OS-entropy source for writer ids and PUT-retry jitter
        // (ADR-0068 decision 2). The log pipeline is not exercised by the
        // seeded simulation driver, so this router has no injected variant on
        // the production path; routing every draw through the seam still keeps
        // `rand::rng()` and `Uuid::new_v4()` off it.
        Self::with_rng(config, store, clock, indexed_fields, Arc::new(SystemRng))
    }

    /// Like [`Self::new_with_indexed_fields`] but with an injected
    /// [`RngSource`]. A [`ravel_commit::rng::SeededRng`] here makes writer ids
    /// deterministic, which the router-level byte-identity differential test
    /// (row vs columnar) needs so two routers over two stores stamp the same
    /// `ObjectIdentity` and object keys.
    pub(crate) fn with_rng(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        indexed_fields: Arc<IndexedFieldsOverlay>,
        rng: Arc<dyn RngSource>,
    ) -> Self {
        let metrics = Arc::new(LogIngestMetrics::new(config.shard_count));
        #[cfg(feature = "stage-timing")]
        let stage_timings = Arc::new(LogStageTimings::new());
        let factory = {
            let store = Arc::clone(&store);
            let clock = Arc::clone(&clock);
            let rng = Arc::clone(&rng);
            let metrics = Arc::clone(&metrics);
            let indexed_fields = Arc::clone(&indexed_fields);
            #[cfg(feature = "stage-timing")]
            let stage_timings = Arc::clone(&stage_timings);
            move |shard_count: u32| -> Vec<LogShardHandle> {
                let writer_id = rng.new_uuid();
                let epoch =
                    u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
                (0..shard_count)
                    .map(|shard| {
                        let (tx, rx) = mpsc::channel(config.channel_depth);
                        let actor = LogShardActor::new(
                            shard,
                            writer_id,
                            epoch,
                            Arc::clone(&store),
                            Arc::clone(&clock),
                            Arc::clone(&rng),
                            config,
                            Arc::clone(&metrics),
                            rx,
                            Arc::clone(&indexed_fields),
                            #[cfg(feature = "stage-timing")]
                            Arc::clone(&stage_timings),
                        );
                        tokio::spawn(actor.run());
                        LogShardHandle {
                            tx,
                            dead: AtomicBool::new(false),
                        }
                    })
                    .collect()
            }
        };
        let switch =
            GenerationSwitch::new(config.shard_count, DEFAULT_REFRESH_INTERVAL_NS, factory);

        LogIngestRouter {
            switch,
            store,
            clock,
            metrics,
            indexed_fields,
            config,
            budget: IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
            #[cfg(feature = "stage-timing")]
            stage_timings,
        }
    }

    /// The per-stage timing accumulator (ADR-0104 decision 1), for the bench
    /// reporter to read a snapshot after driving a write. Present only under the
    /// `stage-timing` feature.
    #[cfg(feature = "stage-timing")]
    pub fn stage_timings(&self) -> Arc<LogStageTimings> {
        Arc::clone(&self.stage_timings)
    }

    /// Installs the shared process-wide ingest buffer byte budget (ADR-0069).
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<IngestByteBudget>) -> Self {
        self.budget = budget;
        self
    }

    pub fn metrics(&self) -> &LogIngestMetrics {
        &self.metrics
    }

    /// Resolve the tenant's active shard-actor set for a write at `now_ns`,
    /// re-reading the provisioning record when the cached view is older than the
    /// refresh interval `C` (ADR-0052 section 3). When the re-read cannot
    /// complete, falls back to [`GenerationSwitch::try_grace_extend`]'s bounded
    /// grace window before failing closed: continuing on the
    /// last-known-good view is only safe while that method's horizon predicate
    /// holds, so a genuinely unknowable generation change still fails the flush
    /// exactly as before.
    async fn active_set(
        &self,
        tenant: ravel_types::TenantHash,
        now_ns: i64,
    ) -> Result<Arc<Vec<LogShardHandle>>, LogWriteError> {
        match self.switch.route_cached(tenant, now_ns) {
            Routed::Fresh(set) => Ok(set),
            Routed::Stale => {
                match load_generations(
                    self.store.as_ref(),
                    ravel_types::Signal::Logs,
                    &tenant,
                    self.switch.default_count(),
                )
                .await
                {
                    Ok(generations) => Ok(self.switch.refresh(tenant, generations, now_ns)),
                    Err(_) => match self.switch.try_grace_extend(tenant, now_ns) {
                        Some(set) => {
                            self.metrics.record_grace_extended_stale_flush();
                            Ok(set)
                        }
                        None => {
                            self.metrics.record_stale_provisioning_flush();
                            Err(LogWriteError::StaleProvisioningView)
                        }
                    },
                }
            }
        }
    }

    /// Update a tenant's cached shard-generation view (ADR-0052 section 2).
    pub fn refresh_generations(
        &self,
        tenant: ravel_types::TenantHash,
        generations: Vec<ravel_catalog::ShardGeneration>,
        now_ns: i64,
    ) {
        self.switch.refresh(tenant, generations, now_ns);
    }

    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
    }

    /// Evict every cached generation view last touched before `now_ns - ttl_ns`
    /// (ADR-0069 decision 2, idle-tenant state eviction). Returns the number of
    /// views dropped. Mirrors [`crate::IngestRouter::evict_idle_generation_views`];
    /// an evicted view is re-derived from the provisioning record on the
    /// tenant's next log write.
    pub fn evict_idle_generation_views(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.switch.evict_idle(now_ns, ttl_ns)
    }

    /// Evict every idle entry from the durable-override indexed-field cache
    /// (ADR-0079, wired into the same ADR-0069 decision 2 idle-tenant sweep as
    /// [`Self::evict_idle_generation_views`]). Returns the number of entries
    /// dropped; an evicted entry re-derives from `TenantConfig` on the tenant's
    /// next flush.
    pub fn evict_idle_indexed_field_cache(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.indexed_fields.evict_idle(now_ns, ttl_ns)
    }

    /// Groups `records` by `shard_for_log`, sends one `LogShardMsg::Write` per
    /// involved shard, and (in strict mode) awaits every involved shard's ack
    /// within `ack_deadline`. Sending blocks on a full channel: that
    /// backpressure is intentional (docs/ingest.md "Channel").
    pub async fn write(
        &self,
        tenant: ravel_types::TenantId,
        records: Vec<NormalizedLogRecord>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<LogWriteReceipt, LogWriteError> {
        if records.is_empty() {
            return Ok(LogWriteReceipt::default());
        }

        // Global ingest byte budget (ADR-0069 decision 1): charge the estimated
        // buffered bytes and shed at the ceiling before routing, so a shed
        // request touches no shard and mints no commit token. The charge is
        // cloned into every shard message below and refunded when the flush(es)
        // holding these bytes complete or fail; any early return from here on
        // drops the not-yet-handed-off clones, so nothing leaks.
        #[cfg(feature = "stage-timing")]
        let admit_start = std::time::Instant::now();
        let estimate: u64 = records
            .iter()
            .map(|r| est_record_bytes(r) as u64)
            .fold(0u64, u64::saturating_add);
        let charge = Arc::new(
            self.budget
                .try_charge(estimate)
                .map_err(|_| LogWriteError::BufferBudgetExceeded)?,
        );
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(LogStage::Admit, admit_start.elapsed());

        // Route against the tenant's current generation view, re-reading the
        // provisioning record when the cache is older than `C` and failing
        // closed if that read cannot complete (ADR-0052 section 3).
        #[cfg(feature = "stage-timing")]
        let route_start = std::time::Instant::now();
        let set = self.active_set(tenant.hash(), self.clock.now_ns()).await?;
        let shard_count = set.len() as u32;
        let mut by_shard: HashMap<u32, Vec<NormalizedLogRecord>> = HashMap::new();
        for record in records {
            let shard = shard_for_log(&record.stream_id, shard_count);
            by_shard.entry(shard).or_default().push(record);
        }
        if by_shard.is_empty() {
            return Ok(LogWriteReceipt::default());
        }

        let mut shard_ids: Vec<u32> = by_shard.keys().copied().collect();
        shard_ids.sort_unstable();

        // Parallel to `ack_rxs`: the shard each receiver belongs to, so a
        // closed ack channel is attributed to the right shard and counted as
        // that shard's death.
        let mut ack_shards = Vec::with_capacity(shard_ids.len());
        let mut ack_rxs = Vec::with_capacity(shard_ids.len());
        for shard in shard_ids {
            let records = by_shard.remove(&shard).unwrap_or_default();
            let ack = match mode {
                WriteMode::Strict => {
                    let (tx, rx) = oneshot::channel();
                    ack_shards.push(shard);
                    ack_rxs.push(rx);
                    Some(tx)
                }
                WriteMode::Buffered => None,
            };
            let msg = LogShardMsg::Write {
                tenant: tenant.clone(),
                records,
                ack,
                charge: Some(Arc::clone(&charge)),
            };
            if set[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the records
                // landed.
                self.mark_shard_dead(&set[shard as usize]);
                return Err(LogWriteError::ShardUnavailable);
            }
            // The message is now in the shard's channel (issue #865): count it
            // as enqueued. The actor counts it processed when it pulls it, so
            // enqueued-minus-processed is the shard's current queue depth.
            self.metrics.record_shard_enqueued(shard);
        }
        // Routing ends at dispatch: the strict-mode ack wait below is downstream
        // durability (merge/encode/PUT happen in the shard), not a router stage.
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(LogStage::Route, route_start.elapsed());

        if mode == WriteMode::Buffered {
            return Ok(LogWriteReceipt::default());
        }

        self.await_strict_acks(&set, ack_shards, ack_rxs, ack_deadline)
            .await
    }

    /// Awaits the strict-mode acks of a dispatched write and folds them into a
    /// receipt (or a classified error), shared verbatim by [`Self::write`] and
    /// [`Self::write_columnar`] so the issue #296 partial-failure accounting has
    /// exactly one implementation. `ack_shards[i]` is the shard `ack_rxs[i]`
    /// belongs to; both are in ascending shard order.
    async fn await_strict_acks(
        &self,
        set: &Arc<Vec<LogShardHandle>>,
        ack_shards: Vec<u32>,
        ack_rxs: Vec<oneshot::Receiver<Result<CommitToken, LogWriteError>>>,
        ack_deadline: Duration,
    ) -> Result<LogWriteReceipt, LogWriteError> {
        // `join_all` preserves input order, so `joined[i]` is `ack_shards[i]`.
        // On a deadline elapse the whole `join_all` future is dropped, so no
        // per-shard ack is observed: `AckTimeout` carries no recovered tokens
        // (a sibling that committed inside the elapsed window is unknowable
        // here, and reporting an unresolved ack as durable would be wrong).
        let joined = tokio::time::timeout(ack_deadline, futures::future::join_all(ack_rxs))
            .await
            .map_err(|_| LogWriteError::AckTimeout)?;

        // Every ack resolved. Scan them all: collect every shard that acked a
        // durable commit (issue #296), and record the first failure in shard
        // order so the returned classification and `mark_shard_dead` side
        // effect are exactly what the pre-fix early-return produced. A shard
        // whose ack failed to resolve (`RecvError`: the actor panicked
        // mid-flush) is NOT a durable write and contributes no token.
        let mut durable = Vec::with_capacity(joined.len());
        let mut first_error: Option<LogWriteError> = None;
        let mut dead_shard: Option<u32> = None;
        for (shard, result) in ack_shards.into_iter().zip(joined) {
            match result {
                Ok(Ok(token)) => durable.push(token),
                Ok(Err(shard_error)) => {
                    if first_error.is_none() {
                        first_error = Some(shard_error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(LogWriteError::ShardUnavailable);
                        dead_shard = Some(shard);
                    }
                }
            }
        }

        if let Some(inner) = first_error {
            // Preserve the exact failure semantics: the death is counted once,
            // only when the first failure in shard order is a dropped ack, and
            // only then (a resolved shard-level error never marked a death).
            if let Some(shard) = dead_shard {
                self.mark_shard_dead(&set[shard as usize]);
            }
            // Carry the durably-acked sibling tokens only when there are any;
            // a failure with no partial success surfaces as the bare variant,
            // unchanged from before this fix.
            let error = if durable.is_empty() {
                inner
            } else {
                LogWriteError::PartialWrite {
                    inner: Box::new(inner),
                    durable,
                }
            };
            return Err(error);
        }

        Ok(LogWriteReceipt { tokens: durable })
    }

    /// The columnar counterpart of [`Self::write`] (ADR-0109 decision 4): the
    /// same sequence -- charge the ADR-0069 byte budget, resolve the generation
    /// view, partition by shard, dispatch, await Strict acks -- with the
    /// partition step building a per-shard [`ColumnarLogBatch`] instead of a
    /// `Vec` per shard. Shard placement is `shard_for_log` over each row's
    /// stream id, exactly as [`Self::write`]. The commit protocol, object key
    /// layout, `WriteMode::Strict` ack contract, flush triggers, and RLOG format
    /// are unchanged; only the buffered input shape differs.
    ///
    /// Until #605 wires the Parquet bulk loader, no shipping binary constructs a
    /// [`ColumnarLogBatch`] to hand here; this is the router seam that loader
    /// will call, reachable today only from tests.
    pub async fn write_columnar(
        &self,
        tenant: ravel_types::TenantId,
        batch: ColumnarLogBatch,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<LogWriteReceipt, LogWriteError> {
        if batch.is_empty() {
            return Ok(LogWriteReceipt::default());
        }

        // Global ingest byte budget (ADR-0069 decision 1): the columnar estimate
        // equals the row path's `est_record_bytes` sum for the same records
        // exactly, so the shared ceiling means the same thing on both paths. As
        // in `write`, the charge is cloned into every shard message and refunded
        // when the flush(es) holding these bytes complete or fail.
        let estimate = est_columnar_bytes(&batch) as u64;
        let charge = Arc::new(
            self.budget
                .try_charge(estimate)
                .map_err(|_| LogWriteError::BufferBudgetExceeded)?,
        );

        // Route against the tenant's current generation view, exactly as `write`.
        let set = self.active_set(tenant.hash(), self.clock.now_ns()).await?;
        let shard_count = set.len() as u32;

        // Partition into per-shard column selections. `partition_columnar`
        // returns ascending-shard order and omits a shard with no rows, matching
        // `write`'s sorted `by_shard` keys.
        let by_shard = partition_columnar(&batch, shard_count);
        if by_shard.is_empty() {
            return Ok(LogWriteReceipt::default());
        }

        let mut ack_shards = Vec::with_capacity(by_shard.len());
        let mut ack_rxs = Vec::with_capacity(by_shard.len());
        for (shard, shard_batch) in by_shard {
            let ack = match mode {
                WriteMode::Strict => {
                    let (tx, rx) = oneshot::channel();
                    ack_shards.push(shard);
                    ack_rxs.push(rx);
                    Some(tx)
                }
                WriteMode::Buffered => None,
            };
            let msg = LogShardMsg::WriteColumnar {
                tenant: tenant.clone(),
                batch: Box::new(shard_batch),
                ack,
                charge: Some(Arc::clone(&charge)),
            };
            if set[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone; count the death once and surface the
                // typed error rather than acking as if the batch landed.
                self.mark_shard_dead(&set[shard as usize]);
                return Err(LogWriteError::ShardUnavailable);
            }
            // Enqueue-time, exactly as the row path above (issue #865). The
            // bulk loader drives this path (ADR-0109), so omitting it here would
            // leave the queue-depth figure blank for the one workload the
            // measurement exists for.
            self.metrics.record_shard_enqueued(shard);
        }

        if mode == WriteMode::Buffered {
            return Ok(LogWriteReceipt::default());
        }

        self.await_strict_acks(&set, ack_shards, ack_rxs, ack_deadline)
            .await
    }

    /// Records the first observation of a shard actor's death, deduped so a
    /// permanently dead shard is counted once no matter how many later writes
    /// route to it.
    fn mark_shard_dead(&self, handle: &LogShardHandle) {
        if !handle.dead.swap(true, Ordering::Relaxed) {
            self.metrics.record_shard_death();
        }
    }

    /// Forces every shard to flush all buffered tenants now, for tests and
    /// graceful shutdown paths that need durability without waiting on
    /// `max_flush_delay`.
    pub async fn flush_all(&self) {
        let sets = self.switch.all_sets();
        let mut dones = Vec::new();
        for set in &sets {
            for shard in set.iter() {
                let (tx, rx) = oneshot::channel();
                if shard
                    .tx
                    .send(LogShardMsg::FlushNow { done: tx })
                    .await
                    .is_ok()
                {
                    dones.push(rx);
                }
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }

    /// Flushes every live generation's log shard actors so a retiring
    /// generation's buffers drain too (ADR-0052 section 2). The detached actor
    /// tasks end on their own after the drain; the `done` acknowledgement fires
    /// after the flush, so durability holds without joining them.
    pub async fn shutdown(self) {
        let sets = self.switch.all_sets();
        let mut dones = Vec::new();
        for set in &sets {
            for shard in set.iter() {
                let (tx, rx) = oneshot::channel();
                let _ = shard.tx.send(LogShardMsg::Shutdown { done: tx }).await;
                dones.push(rx);
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }
}

/// Partitions a [`ColumnarLogBatch`] into per-shard sub-batches by
/// `shard_for_log` over each row's stream id (ADR-0109 decision 4), the
/// columnar analogue of [`LogIngestRouter::write`]'s `by_shard` grouping.
///
/// Each returned sub-batch is exactly what `ColumnarLogBatch::from_records`
/// would build from that shard's rows taken in row order: dynamic columns keep
/// the parent's `(name, type)`-sorted order and any the subset leaves all-absent
/// are dropped (`from_records` over the subset would never have created them),
/// and the stream directory is rebuilt id-ascending with dense refs. That
/// equivalence is what makes each per-shard object byte-identical to the row
/// path's, whose per-shard record vector this reproduces (the writer-level proof
/// is #602). Returns ascending-shard order; a shard with no rows is omitted,
/// exactly as the row path omits it.
fn partition_columnar(batch: &ColumnarLogBatch, shard_count: u32) -> Vec<(u32, ColumnarLogBatch)> {
    // Per-shard accumulator: the sub-batch under construction, the parent stream
    // refs of its rows (remapped to dense child refs once all rows are seen),
    // and one dense (cells, validity) pair per parent dynamic column.
    struct Acc {
        out: ColumnarLogBatch,
        parent_refs: Vec<u32>,
        dyn_cells: Vec<Vec<AttrValue>>,
        dyn_validity: Vec<Bitmap>,
    }
    let ncol = batch.dyn_columns.len();
    let mut accs: HashMap<u32, Acc> = HashMap::new();

    // Dense-slot cursors into the parent's packed buffers, advanced once per row
    // (regardless of shard) so a present cell reads the correct dense slot.
    let mut trace_slot = 0usize;
    let mut span_slot = 0usize;
    let mut col_slot = vec![0usize; ncol];

    for row in 0..batch.num_rows {
        let stream_id = batch.stream_ids[batch.stream_refs[row] as usize];
        let shard = shard_for_log(&stream_id, shard_count);
        let acc = accs.entry(shard).or_insert_with(|| Acc {
            out: ColumnarLogBatch::new(),
            parent_refs: Vec::new(),
            dyn_cells: vec![Vec::new(); ncol],
            dyn_validity: vec![Bitmap::new(); ncol],
        });

        acc.out.num_rows += 1;
        acc.out.ts_ns.push(batch.ts_ns[row]);
        acc.out.observed_ts_ns.push(batch.observed_ts_ns[row]);
        acc.out.severity_num.push(batch.severity_num[row]);
        acc.out.flags.push(batch.flags[row]);
        acc.out.severity_text.push(batch.severity_text.get(row));
        acc.out.body.push(batch.body.get(row));

        if batch.trace_id_validity.get(row) {
            acc.out
                .trace_id
                .extend_from_slice(batch.trace_id_at(trace_slot));
            acc.out.trace_id_validity.push(true);
            trace_slot += 1;
        } else {
            acc.out.trace_id_validity.push(false);
        }
        if batch.span_id_validity.get(row) {
            acc.out
                .span_id
                .extend_from_slice(batch.span_id_at(span_slot));
            acc.out.span_id_validity.push(true);
            span_slot += 1;
        } else {
            acc.out.span_id_validity.push(false);
        }

        acc.out
            .residual_attrs
            .push(batch.residual_attrs[row].clone());
        acc.parent_refs.push(batch.stream_refs[row]);

        for (c, slot) in col_slot.iter_mut().enumerate() {
            let col = &batch.dyn_columns[c];
            if col.validity.get(row) {
                acc.dyn_cells[c].push(col.cells[*slot].clone());
                acc.dyn_validity[c].push(true);
                *slot += 1;
            } else {
                acc.dyn_validity[c].push(false);
            }
        }
    }

    let mut result: Vec<(u32, ColumnarLogBatch)> = Vec::with_capacity(accs.len());
    for (shard, acc) in accs {
        let Acc {
            mut out,
            parent_refs,
            dyn_cells,
            dyn_validity,
        } = acc;

        // Stream directory: distinct parent refs ascending. The parent's
        // `stream_ids` are id-ascending, so ascending parent refs are
        // id-ascending too; rebuild them as dense child refs.
        let mut distinct: Vec<u32> = parent_refs.clone();
        distinct.sort_unstable();
        distinct.dedup();
        let mut child_ref_of: HashMap<u32, u32> = HashMap::with_capacity(distinct.len());
        for (child, &parent_ref) in distinct.iter().enumerate() {
            child_ref_of.insert(parent_ref, child as u32);
            out.stream_ids.push(batch.stream_ids[parent_ref as usize]);
            out.stream_attrs
                .push(batch.stream_attrs[parent_ref as usize].clone());
        }
        out.stream_refs = parent_refs
            .iter()
            .map(|parent_ref| child_ref_of[parent_ref])
            .collect();

        // Dynamic columns: keep the parent's `(name, type)` order, dropping any
        // column the subset left all-absent.
        for (c, (cells, validity)) in dyn_cells.into_iter().zip(dyn_validity).enumerate() {
            if validity.count_present() == 0 {
                continue;
            }
            out.dyn_columns.push(DynColumn {
                name: batch.dyn_columns[c].name.clone(),
                field_type: batch.dyn_columns[c].field_type,
                cells,
                validity,
            });
        }

        result.push((shard, out));
    }
    result.sort_by_key(|(shard, _)| *shard);
    result
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantHash;

    use super::*;

    /// Nanoseconds per unix hour, matching `generation.rs`'s private constant
    /// of the same value (`activation_hour`'s unit).
    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    /// A store wrapped in [`FaultStore`] whose every `get` (the provisioning
    /// re-read `active_set` issues on a stale cache) returns a transient error,
    /// modeling sustained store latency/unreachability rather than a one-off
    /// blip: the re-read never completes for as long as the fault is active.
    fn always_failing_get_store() -> Arc<dyn ObjectStoreBackend> {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("simulated sustained store latency".into()),
            )
            .with_occurrence(Occurrence::Always),
        );
        Arc::new(FaultStore::new(MemoryStore::new(), plan))
    }

    fn test_router(store: Arc<dyn ObjectStoreBackend>) -> LogIngestRouter {
        LogIngestRouter::new(
            IngestConfig {
                shard_count: 4,
                ..IngestConfig::default()
            },
            store,
            Arc::new(crate::clock::SystemClock),
        )
    }

    /// Under sustained store latency, a router whose cached
    /// view has gone stale by `C` keeps routing every flush inside the bounded
    /// grace window rather than failing all of them closed, and still fails
    /// closed once the horizon is crossed.
    ///
    /// The flipped line: before this fix, `active_set`'s `Err(_)` arm on a
    /// failed re-read went straight to `record_stale_provisioning_flush` +
    /// `Err(LogWriteError::StaleProvisioningView)`, with no `try_grace_extend`
    /// call in between. Under this test's `always_failing_get_store` (every
    /// re-read fails, modeling sustained latency), that pre-fix arm means every
    /// single one of the three `active_set` calls below -- at t0+C, well within
    /// the grace horizon, and past it -- would return `Err`: a total ingest
    /// outage for this tenant for as long as the store stays slow, exactly the
    /// sustained-latency finding. This test proves the fix: the first two calls succeed
    /// (degraded, metered), and only the third -- past the horizon, where an
    /// unseen generation change becomes possible -- fails closed.
    #[tokio::test]
    async fn nf2_grace_window_survives_sustained_store_latency_then_fails_closed() {
        let store = always_failing_get_store();
        let router = test_router(Arc::clone(&store));
        let t = tenant(9);
        let c = router.switch.refresh_interval_ns();

        // Seed a fresh cached view the ordinary way (no fault on this refresh;
        // the refresh call itself never touches the store).
        let t0 = 10 * NS_PER_HOUR;
        router.refresh_generations(t, vec![], t0);

        // Just past C: the cache is stale, the re-read fails (sustained
        // latency), but the grace horizon (t0 + min_lead_hours(C) = hour 12 for
        // the default C) has not been reached. Routes on the last-known-good
        // view instead of failing closed.
        let past_c_ns = t0 + c + 1;
        let set = router
            .active_set(t, past_c_ns)
            .await
            .expect("within the grace horizon, routes on the last-known-good view");
        assert_eq!(set.len(), 4);
        assert_eq!(
            router.metrics.snapshot().grace_extended_stale_flushes,
            1,
            "the degraded-routing counter fires exactly once so far"
        );

        // Still well within the horizon (hour 11 < hour 12): another flush,
        // still degraded rather than failed.
        let still_within_horizon_ns = 11 * NS_PER_HOUR;
        router
            .active_set(t, still_within_horizon_ns)
            .await
            .expect("still within the grace horizon");
        assert_eq!(router.metrics.snapshot().grace_extended_stale_flushes, 2);
        assert_eq!(
            router.metrics.snapshot().stale_provisioning_flushes,
            0,
            "no flush has failed closed yet"
        );

        // Past the horizon (hour 12): an unseen generation change becomes
        // possible, so this must fail closed exactly as the pre-fix behavior
        // did for every call in this test.
        let horizon_crossed_ns = 12 * NS_PER_HOUR;
        match router.active_set(t, horizon_crossed_ns).await {
            Err(LogWriteError::StaleProvisioningView) => {}
            Ok(_) => panic!("past the grace horizon, must fail closed, not route"),
            Err(other) => panic!("past the grace horizon, wrong error: {other:?}"),
        }
        assert_eq!(
            router.metrics.snapshot().stale_provisioning_flushes,
            1,
            "the fail-closed counter fires exactly once, only past the horizon"
        );
        assert_eq!(
            router.metrics.snapshot().grace_extended_stale_flushes,
            2,
            "the degraded counter does not move on the fail-closed call"
        );
    }

    /// Issue #389 regression, driven through the real charge path
    /// (`LogIngestRouter::write` -> `try_charge` -> `IngestByteBudget`): a single
    /// attribute whose value is a `Map` of 8192 `("", Bool)` entries. Under the
    /// pre-fix `attr_value_len` the record charged only the 8192 one-byte Bool
    /// payloads (~8.25 KB), so a 100 KB process budget admitted it; the fix also
    /// charges each entry's `(String, AttrValue)` header, so the same record now
    /// charges ~459 KB and the budget refuses it before any shard is touched.
    ///
    /// A helper-only assertion on `attr_value_len` would prove the estimate grew
    /// but not that the budget's admission decision changed, so this asserts the
    /// `write` call itself is shed.
    #[tokio::test]
    async fn wide_nested_map_attribute_is_shed_by_the_budget_after_the_nesting_fix() {
        use ravel_otlp::logs_normalize::NormalizedLogRecord;
        use ravel_types::TenantId;
        use ravel_types::logstream::{AttrValue, LogStreamId};

        let entries: Vec<(String, AttrValue)> = (0..8192)
            .map(|_| (String::new(), AttrValue::Bool(false)))
            .collect();
        let rec = NormalizedLogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: Vec::new(),
            ts_ns: 1_000,
            observed_ts_ns: 1_000,
            severity_num: 9,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("m".to_string(), AttrValue::Map(entries))],
        };

        // Magnitude: the fixed estimate is on the order of 459 KB, far above the
        // ~8.25 KB the old payload-only measure charged.
        let est = est_record_bytes(&rec);
        assert!(
            (400_000..=600_000).contains(&est),
            "nested-map estimate {est} is not on the ~459 KB order the fix charges"
        );

        // A budget bounded strictly between the old and new charge: admits the
        // record under the old measure, sheds it under the fix.
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(100_000));
        let router = test_router(Arc::new(MemoryStore::new())).with_budget(Arc::clone(&budget));

        let err = router
            .write(
                TenantId::new("acme"),
                vec![rec],
                WriteMode::Buffered,
                Duration::from_secs(1),
            )
            .await
            .expect_err("the fixed nesting estimate pushes the record past the 100 KB ceiling");
        assert!(
            matches!(err, LogWriteError::BufferBudgetExceeded),
            "the record must be shed at the byte budget, got {err:?}"
        );
        assert_eq!(
            budget.shed_total(),
            1,
            "the shed counter fires exactly once"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "a shed request charges nothing, so the gauge is untouched"
        );
    }

    /// A tenant whose cached view has genuinely changed `shard_count`
    /// (observed via a successful refresh, not grace-extension) routes at the
    /// new count immediately -- grace-extension is never on this router's path
    /// when the store is healthy, since `route_cached`/a successful re-read
    /// always wins first.
    #[tokio::test]
    async fn nf2_healthy_store_never_takes_the_grace_path() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = test_router(Arc::clone(&store));
        let t = tenant(10);
        let c = router.switch.refresh_interval_ns();

        let t0 = 10 * NS_PER_HOUR;
        router.refresh_generations(t, vec![], t0);

        // Past C, but the store is healthy: the re-read succeeds, so this
        // routes via the ordinary refresh path, not grace-extension.
        let set = router
            .active_set(t, t0 + c + 1)
            .await
            .expect("a healthy store's re-read succeeds");
        assert_eq!(set.len(), 4);
        assert_eq!(
            router.metrics.snapshot().grace_extended_stale_flushes,
            0,
            "no degraded routing when the store is healthy"
        );
    }

    // ---- ADR-0109 columnar write path ----

    use ravel_commit::rng::SeededRng;
    use ravel_logseg::{ColumnarLogBatch, LogRecord, stream_attrs_bytes};
    use ravel_object_store::{GetRange, list_all};
    use ravel_types::TenantId;
    use ravel_types::logstream::log_stream_id;

    /// A clock pinned to one instant: makes `epoch`, `flush_open_ns`, and every
    /// clock-derived flush-identity field deterministic and identical across two
    /// routers, which the byte-identity differential test requires. The tests
    /// that use it drive flushes through `flush_all`, never the age tick, so the
    /// real-timer default `sleep` is never depended on.
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn overlay() -> Arc<IndexedFieldsOverlay> {
        Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields)))
    }

    /// Buffers every write without a size or age flush, so a single `flush_all`
    /// drives exactly one flush per shard (seq 0 on both routers).
    fn buffer_all() -> IngestConfig {
        IngestConfig {
            shard_count: 4,
            target_bytes: 64 * 1024 * 1024,
            max_flush_delay: Duration::from_secs(3600),
            max_flush_delay_idle: Duration::from_secs(3600),
            flush_tick: Duration::from_secs(3600),
            ..IngestConfig::default()
        }
    }

    fn to_logrecord(r: &NormalizedLogRecord) -> LogRecord {
        LogRecord {
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

    /// Records spread across streams (so across shards) and carrying the
    /// features that stress the two paths' agreement: multiple attribute types,
    /// a within-record duplicate `(name, type)` that folds into `residual_attrs`,
    /// a nested `Map` value that resolves to canonical bytes, and present/absent
    /// trace and span ids.
    fn diverse_records() -> Vec<NormalizedLogRecord> {
        let mut out = Vec::new();
        for i in 0..48u32 {
            let host = format!("h{i}");
            let res: Vec<(String, AttrValue)> = vec![
                (
                    "service.name".to_string(),
                    AttrValue::Str("api".to_string()),
                ),
                ("host".to_string(), AttrValue::Str(host)),
            ];
            let stream_id = log_stream_id(&res, "scope", "", &[]);
            let stream_attrs = stream_attrs_bytes(&res, "scope", "", &[]);
            let mut attrs: Vec<(String, AttrValue)> = vec![
                ("k_str".to_string(), AttrValue::Str(format!("v{i}"))),
                ("k_int".to_string(), AttrValue::I64(i as i64)),
                ("k_bool".to_string(), AttrValue::Bool(i % 2 == 0)),
            ];
            if i % 3 == 0 {
                attrs.push(("k_str".to_string(), AttrValue::Str("dup".to_string())));
            }
            if i % 5 == 0 {
                attrs.push((
                    "nested".to_string(),
                    AttrValue::Map(vec![("a".to_string(), AttrValue::Bool(true))]),
                ));
            }
            let trace_id = if i % 2 == 0 {
                Some([i as u8; 16])
            } else {
                None
            };
            let span_id = if i % 4 == 0 {
                Some([(i as u8).wrapping_add(1); 8])
            } else {
                None
            };
            out.push(NormalizedLogRecord {
                stream_id,
                stream_attrs,
                ts_ns: 1_000 + i as i64,
                observed_ts_ns: 1_000 + i as i64,
                severity_num: (i % 24) as u8,
                severity_text: "INFO".to_string(),
                body: format!("body {i}"),
                trace_id,
                span_id,
                flags: i,
                attrs,
            });
        }
        out
    }

    async fn collect_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, Vec<u8>)> {
        let mut metas = list_all(store, "").await.expect("list all objects");
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            let bytes = store
                .get(&meta.key, GetRange::Full)
                .await
                .expect("get object")
                .data;
            out.push((meta.key, bytes.to_vec()));
        }
        out
    }

    /// The acceptance anchor (ADR-0109 decision 7, router level): the same
    /// records written through `write` and through `write_columnar` produce
    /// byte-identical stored objects. Two routers over two stores share one
    /// pinned clock and one seed, so `writer_id`, `epoch`, and `seq` match and
    /// only a real drift in admission, coercion, dynamic-column assignment, or
    /// stream-directory building could make a byte differ. Compared byte for
    /// byte over every stored object (data and commit records both), not row
    /// counts and not decoded content.
    #[tokio::test]
    async fn columnar_write_produces_the_same_objects_as_row_write() {
        let seed = 0x00C0_FFEE_u64;
        // A realistic wall-clock reading (2023): flush identity derives its hour
        // bucket from this, and `checked_ingest_hour_bucket` rejects a reading
        // below its plausibility floor.
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(1_700_000_000_000_000_000));

        let store_row: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_row = LogIngestRouter::with_rng(
            buffer_all(),
            Arc::clone(&store_row),
            Arc::clone(&clock),
            overlay(),
            Arc::new(SeededRng::new(seed)),
        );

        let store_col: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_col = LogIngestRouter::with_rng(
            buffer_all(),
            Arc::clone(&store_col),
            Arc::clone(&clock),
            overlay(),
            Arc::new(SeededRng::new(seed)),
        );

        let tenant = TenantId::new("acme");
        let records = diverse_records();
        let shards: std::collections::HashSet<u32> = records
            .iter()
            .map(|r| shard_for_log(&r.stream_id, 4))
            .collect();
        assert!(
            shards.len() > 1,
            "the fixture must span multiple shards to exercise partitioning, spans {}",
            shards.len()
        );

        router_row
            .write(
                tenant.clone(),
                records.clone(),
                WriteMode::Buffered,
                Duration::from_secs(5),
            )
            .await
            .expect("row buffered write enqueues");
        router_row.flush_all().await;

        let batch =
            ColumnarLogBatch::from_records(&records.iter().map(to_logrecord).collect::<Vec<_>>());
        router_col
            .write_columnar(
                tenant.clone(),
                batch,
                WriteMode::Buffered,
                Duration::from_secs(5),
            )
            .await
            .expect("columnar buffered write enqueues");
        router_col.flush_all().await;

        let objs_row = collect_objects(store_row.as_ref()).await;
        let objs_col = collect_objects(store_col.as_ref()).await;
        assert!(
            !objs_row.is_empty(),
            "the row path must have written at least one object"
        );
        assert_eq!(
            objs_row, objs_col,
            "row and columnar paths must produce byte-identical stored objects"
        );
    }

    /// Each per-shard sub-batch `partition_columnar` builds is exactly what
    /// `ColumnarLogBatch::from_records` would build from that shard's rows in row
    /// order (dynamic-column order and drop, stream-directory rebuild, dense
    /// slots). This is the structural invariant the byte-identity test relies
    /// on, checked directly so a partition bug is localized here rather than
    /// surfacing only as an opaque byte diff.
    #[test]
    fn partition_columnar_matches_from_records_per_shard() {
        let shard_count = 4u32;
        let records = diverse_records();
        let logrecords: Vec<LogRecord> = records.iter().map(to_logrecord).collect();
        let batch = ColumnarLogBatch::from_records(&logrecords);

        let mut expected: std::collections::HashMap<u32, Vec<LogRecord>> =
            std::collections::HashMap::new();
        for lr in &logrecords {
            let shard = shard_for_log(&lr.stream_id, shard_count);
            expected.entry(shard).or_default().push(lr.clone());
        }

        let parts = partition_columnar(&batch, shard_count);
        let seen: std::collections::HashSet<u32> = parts.iter().map(|(s, _)| *s).collect();
        assert_eq!(
            seen.len(),
            parts.len(),
            "each shard appears at most once in the partition"
        );
        assert_eq!(
            seen,
            expected.keys().copied().collect(),
            "the partition covers exactly the shards the row path routes to"
        );

        for (shard, part) in parts {
            let want = ColumnarLogBatch::from_records(&expected[&shard]);
            assert_eq!(
                part, want,
                "shard {shard}'s partition must equal from_records of its rows"
            );
        }
    }
}
