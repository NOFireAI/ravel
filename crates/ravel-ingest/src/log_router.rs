//! Owns the log shard actors and fans writes out to them, the log-pipeline
//! counterpart of [`crate::router`] (docs/ingest.md "Structure").
//!
//! Unlike [`crate::router::IngestRouter`], which takes a `Signal` because the
//! metrics/remote-write paths reuse it, this router bakes in [`Signal::Logs`]:
//! it has exactly one caller shape, so an unused parameter would only invite a
//! wrong value.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_logseg::{Bitmap, ColumnarLogBatch, DynCells, DynColumn, LogSegError, VarBytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::{CommitToken, TenantHash, shard_for_log};
use tokio::sync::{mpsc, oneshot};

use crate::budget::{BufferBudgetCeiling, IngestByteBudget, IngestByteBudgetLimit};
use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::deferral::DeferralCapFlag;
use crate::generation::{
    DEFAULT_REFRESH_INTERVAL_NS, FlushScope, GenerationSwitch, LiveSender, Routed, SwitchScope,
    load_generations,
};
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

/// The log pipeline's counterpart of [`crate::router`]'s private
/// `ShardHandle`. Its fields differ (a channel and a death flag instead of a
/// mutex and a flush floor), and at two fields a separate struct is cheaper
/// than sharing one across an unrelated boundary.
struct LogShardHandle {
    tx: mpsc::Sender<LogShardMsg>,
    /// Set once the router first observes this shard's channel closed. The
    /// actor is never restarted, so this only flips false to true; it dedups
    /// the `shard_deaths` and `shards_condemned` counters to one increment
    /// per shard per generation.
    dead: AtomicBool,
    /// The shard's at-cap flag (ADR-1642 deferral cap amendment), set and
    /// cleared by the actor and read here before enqueue, the only place a
    /// buffered-mode write can be refused.
    cap_flag: DeferralCapFlag,
}

impl LiveSender<LogShardMsg> for LogShardHandle {
    /// The actor's mailbox unless the shard is dead or the mailbox closed. A
    /// dead log shard is never respawned, so a hand-back to it waits for
    /// teardown.
    fn live_sender(&self) -> Option<mpsc::Sender<LogShardMsg>> {
        (!self.dead.load(Ordering::Relaxed) && !self.tx.is_closed()).then(|| self.tx.clone())
    }
}

/// Routes log writes to generation-versioned shard-actor sets (ADR-0052), the
/// log-pipeline counterpart of [`crate::router::IngestRouter`]. The generation-0
/// set is spawned at construction; a reshard's activation spawns the new set
/// lazily via the [`GenerationSwitch`] factory while the old set drains.
pub struct LogIngestRouter {
    /// Shared with every shard actor through a weak [`SwitchScope`], which the
    /// scan-set check at flush open reads (ADR-1642 scan-set amendment).
    switch: Arc<GenerationSwitch<LogShardHandle>>,
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
    /// The ceiling from `budget`, shared with every shard actor this router
    /// spawns so the per-buffer memory backstop is a fraction of the configured
    /// limit. Written by [`LogIngestRouter::with_budget`], which runs after the
    /// actors exist.
    backstop_ceiling: BufferBudgetCeiling,
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
        let backstop_ceiling = BufferBudgetCeiling::unlimited();
        #[cfg(feature = "stage-timing")]
        let stage_timings = Arc::new(LogStageTimings::new());
        let switch = Arc::new_cyclic(|weak: &Weak<GenerationSwitch<LogShardHandle>>| {
            let weak = weak.clone();
            let store = Arc::clone(&store);
            let refresh_store = Arc::clone(&store);
            let clock = Arc::clone(&clock);
            let refresh_clock = Arc::clone(&clock);
            let rng = Arc::clone(&rng);
            let metrics = Arc::clone(&metrics);
            let indexed_fields = Arc::clone(&indexed_fields);
            let backstop_ceiling = backstop_ceiling.clone();
            #[cfg(feature = "stage-timing")]
            let stage_timings = Arc::clone(&stage_timings);
            let factory = move |shard_count: u32| -> Vec<LogShardHandle> {
                let scope: Arc<dyn FlushScope<LogShardMsg>> =
                    Arc::new(SwitchScope::new(weak.clone(), shard_count));
                let writer_id = rng.new_uuid();
                let epoch =
                    u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
                (0..shard_count)
                    .map(|shard| {
                        let (tx, rx) = mpsc::channel(config.channel_depth);
                        let cap_flag = DeferralCapFlag::new(config.flush_deferral_cap_ns());
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
                            backstop_ceiling.clone(),
                            cap_flag.clone(),
                            Arc::clone(&scope),
                            #[cfg(feature = "stage-timing")]
                            Arc::clone(&stage_timings),
                        );
                        tokio::spawn(actor.run());
                        LogShardHandle {
                            tx,
                            dead: AtomicBool::new(false),
                            cap_flag,
                        }
                    })
                    .collect()
            };
            GenerationSwitch::new(config.shard_count, DEFAULT_REFRESH_INTERVAL_NS, factory)
                .with_refresh_source(refresh_store, ravel_types::Signal::Logs, refresh_clock)
        });

        LogIngestRouter {
            switch,
            store,
            clock,
            metrics,
            indexed_fields,
            config,
            budget: IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
            backstop_ceiling,
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

    /// Installs the shared process-wide ingest buffer byte budget (ADR-0069),
    /// and publishes its ceiling to this router's shard actors so the
    /// per-buffer memory backstop is a fraction of the configured limit rather
    /// than of the default one (issue #1305).
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<IngestByteBudget>) -> Self {
        self.backstop_ceiling.set(budget.limit());
        self.budget = budget;
        self
    }

    pub fn metrics(&self) -> &LogIngestMetrics {
        &self.metrics
    }

    /// The counter registry as a shared handle, for a caller that must read the
    /// same registry after [`Self::shutdown`] consumes the router (the graceful
    /// drain counts into it).
    pub fn metrics_handle(&self) -> Arc<LogIngestMetrics> {
        self.metrics.clone()
    }

    /// Whether every shard actor this router owns is live enough to serve:
    /// false once the router has observed a shard actor dead (a write or ack
    /// found its channel closed), since that first observed death condemns a
    /// log shard (issue #1691; docs/ingest.md, Log pipeline, has the full
    /// rule). It reads the condemned-shard counter rather than live handles,
    /// which the shard-actor sets never drop for the process lifetime.
    pub fn ready(&self) -> bool {
        self.metrics.condemned_shards() == 0
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
        self.refuse_dead_or_capped(&set, &shard_ids)?;

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
                self.metrics.record_partial_write();
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
    /// The bulk loader (`services/ravel-cli/src/load/logs.rs`) calls this in
    /// production; the `ravel-bench` columnar load harness and tests call it
    /// too.
    pub async fn write_columnar(
        &self,
        tenant: ravel_types::TenantId,
        batch: ColumnarLogBatch,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<LogWriteReceipt, LogWriteError> {
        self.write_columnar_partitioned(tenant, batch, mode, ack_deadline, partition_columnar)
            .await
    }

    /// [`Self::write_columnar`] with the partition step supplied, so a test can
    /// run the same write through a reference partition and compare objects.
    async fn write_columnar_partitioned<P>(
        &self,
        tenant: ravel_types::TenantId,
        batch: ColumnarLogBatch,
        mode: WriteMode,
        ack_deadline: Duration,
        partition: P,
    ) -> Result<LogWriteReceipt, LogWriteError>
    where
        P: FnOnce(ColumnarLogBatch, u32) -> Result<Vec<(u32, ColumnarLogBatch)>, LogSegError>,
    {
        // Caller-side input rejection, before anything else: a malformed batch
        // must not reach `est_columnar_bytes` (which indexes `stream_attrs` by
        // `stream_refs` with no bound check of its own) or `partition_columnar`,
        // and must not be counted as a `stream_id_collisions` hit the way a
        // batch that reached the writer's directory merge would be. Maps to
        // `SegmentBuild`, the same variant the shard actor's own flush-time
        // `LogSegError::MalformedColumnarBatch` maps to (log_shard.rs), so a
        // caller sees one failure shape whichever point rejects its input.
        batch
            .validate()
            .map_err(|e| LogWriteError::SegmentBuild(e.to_string()))?;

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
        // `write`'s sorted `by_shard` keys. It consumes the batch, so the parent
        // is freed before any shard is sent its part. The batch validated above,
        // so a refusal here is a partition bug; dropping `charge` refunds it.
        let by_shard = partition(batch, shard_count)
            .map_err(|e| LogWriteError::SegmentBuild(e.to_string()))?;
        if by_shard.is_empty() {
            return Ok(LogWriteReceipt::default());
        }
        let shard_ids: Vec<u32> = by_shard.iter().map(|(shard, _)| *shard).collect();
        self.refuse_dead_or_capped(&set, &shard_ids)?;

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

    /// Refuses the whole write, before any shard is sent anything, when one of
    /// `shards` is dead or has a flush deferred for the whole flush deferral
    /// cap (ADR-1642 deferral cap amendment). The cap check is the only refusal
    /// a buffered-mode write can get, since it is acknowledged at enqueue. A
    /// dead shard is checked first, so a deferral its actor published before
    /// dying cannot hide the death from `ready`.
    fn refuse_dead_or_capped(
        &self,
        set: &[LogShardHandle],
        shards: &[u32],
    ) -> Result<(), LogWriteError> {
        for &shard in shards {
            let handle = &set[shard as usize];
            if handle.dead.load(Ordering::Relaxed) || handle.tx.is_closed() {
                self.mark_shard_dead(handle);
                return Err(LogWriteError::ShardUnavailable);
            }
        }
        let now_ns = self.clock.now_ns();
        match shards
            .iter()
            .find(|&&shard| set[shard as usize].cap_flag.reached(now_ns))
        {
            Some(&capped) => {
                self.metrics.record_deferral_cap_refused();
                set[capped as usize]
                    .cap_flag
                    .note_refusal(ravel_types::Signal::Logs, capped);
                Err(LogWriteError::DeferralCapReached)
            }
            None => Ok(()),
        }
    }

    /// Records the first observation of a shard actor's death, deduped so a
    /// permanently dead shard is counted once no matter how many later writes
    /// route to it.
    fn mark_shard_dead(&self, handle: &LogShardHandle) {
        if !handle.dead.swap(true, Ordering::Relaxed) {
            self.metrics.record_shard_death();
            // The first death condemns a log shard (docs/ingest.md, Log pipeline).
            self.metrics.record_shard_condemned();
        }
    }

    /// Forces every shard to flush all buffered tenants now, for tests and
    /// graceful shutdown paths that need durability without waiting on
    /// `max_flush_delay`. Repeats while a pass handed records back, as
    /// [`crate::IngestRouter::flush_all`] does.
    pub async fn flush_all(&self) {
        for _ in 0..crate::router::HAND_BACK_DRAIN_PASSES {
            let handed_back = self.metrics.rerouted_flushes();
            self.flush_all_pass().await;
            if self.metrics.rerouted_flushes() == handed_back {
                break;
            }
        }
    }

    async fn flush_all_pass(&self) {
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
    /// after the flush, so durability holds without joining them. Sets drain
    /// largest first, each finished before the next is signalled, so records a
    /// retiring set hands back to a smaller set land in a set still running;
    /// a hand-back to a larger set is either written by it or refused by its
    /// closed mailbox, as [`crate::IngestRouter::shutdown`] sets out. The
    /// sets are listed again after each one, since a hand-back can construct
    /// the current generation's set during the drain.
    pub async fn shutdown(self) {
        let mut drained = Vec::new();
        while let Some((count, set)) = self.switch.largest_undrained_set(&drained) {
            drained.push(count);
            let mut dones = Vec::new();
            for shard in set.iter() {
                let (tx, rx) = oneshot::channel();
                let _ = shard.tx.send(LogShardMsg::Shutdown { done: tx }).await;
                dones.push(rx);
            }
            for rx in dones {
                let _ = rx.await;
            }
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
///
/// The parent is consumed column by column and freed column by column, each
/// column once its rows are dealt (#2624). Residual attribute lists and stream
/// blobs move into their shard's sub-batch. Dynamic cells are copied by row
/// ([`DynCells::push_from`]: string and byte cells are copied, a nested
/// `List`/`Map` original is cloned), as are the severity text, body, and trace
/// and span id bytes. Each column's per-shard buffers coexist with the
/// parent's while it is dealt.
///
/// The parent's column dictionaries (`dyn_col_dicts`) are dropped: every
/// sub-batch has an empty `dyn_col_dicts` and the writer takes its plain
/// per-cell path. Carrying them would copy each distinct value into every
/// shard that uses it, memory `est_columnar_bytes` does not count and the
/// ingest byte budget therefore cannot see (#2624).
///
/// The batch must be one [`ColumnarLogBatch::validate`] accepts. A per-row
/// column whose length disagrees with the row count returns
/// [`LogSegError::MalformedColumnarBatch`] rather than being truncated, as
/// does a column whose cell count disagrees with its present rows.
pub(crate) fn partition_columnar(
    batch: ColumnarLogBatch,
    shard_count: u32,
) -> Result<Vec<(u32, ColumnarLogBatch)>, LogSegError> {
    let rows = batch.stream_refs.len();
    let streams = batch.stream_ids.len();
    rows_agree("stream_attrs", batch.stream_attrs.len(), streams)?;
    if let Some(&r) = batch.stream_refs.iter().find(|&&r| r as usize >= streams) {
        return Err(LogSegError::MalformedColumnarBatch(format!(
            "partition: stream ref {r} out of range for {streams} streams"
        )));
    }
    rows_agree("severity_text", batch.severity_text.len(), rows)?;
    rows_agree("body", batch.body.len(), rows)?;
    for column in &batch.dyn_columns {
        rows_agree(&column.name, column.validity.len(), rows)?;
        rows_agree(
            &column.name,
            column.cells.len(),
            column.validity.count_present(),
        )?;
    }

    let ColumnarLogBatch {
        num_rows: _,
        ts_ns,
        observed_ts_ns,
        severity_num,
        flags,
        severity_text,
        body,
        trace_id,
        trace_id_validity,
        span_id,
        span_id_validity,
        stream_refs,
        stream_ids,
        mut stream_attrs,
        dyn_columns,
        dyn_col_dicts,
        residual_attrs,
    } = batch;
    drop(dyn_col_dicts);

    // The shards that receive rows, ascending, and each row's index into them.
    // A stream maps to one shard, so the shard is resolved once per stream.
    let mut referenced = vec![false; stream_ids.len()];
    for &r in &stream_refs {
        referenced[r as usize] = true;
    }
    let stream_shard: Vec<u32> = stream_ids
        .iter()
        .map(|id| shard_for_log(id, shard_count))
        .collect();
    let mut shards: Vec<u32> = stream_shard
        .iter()
        .zip(&referenced)
        .filter_map(|(&shard, &used)| used.then_some(shard))
        .collect();
    shards.sort_unstable();
    shards.dedup();
    let stream_part: Vec<usize> = stream_shard
        .iter()
        .map(|&shard| shards.partition_point(|&s| s < shard))
        .collect();
    let row_part: Vec<usize> = stream_refs
        .iter()
        .map(|&r| stream_part[r as usize])
        .collect();

    let mut rows_per_part = vec![0usize; shards.len()];
    for &p in &row_part {
        rows_per_part[p] += 1;
    }
    let mut out: Vec<ColumnarLogBatch> = rows_per_part
        .iter()
        .map(|&num_rows| ColumnarLogBatch {
            num_rows,
            stream_refs: Vec::with_capacity(num_rows),
            ..ColumnarLogBatch::new()
        })
        .collect();

    for (part, v) in out
        .iter_mut()
        .zip(deal_rows("ts_ns", ts_ns, &row_part, &rows_per_part)?)
    {
        part.ts_ns = v;
    }
    for (part, v) in out.iter_mut().zip(deal_rows(
        "observed_ts_ns",
        observed_ts_ns,
        &row_part,
        &rows_per_part,
    )?) {
        part.observed_ts_ns = v;
    }
    for (part, v) in out.iter_mut().zip(deal_rows(
        "severity_num",
        severity_num,
        &row_part,
        &rows_per_part,
    )?) {
        part.severity_num = v;
    }
    for (part, v) in out
        .iter_mut()
        .zip(deal_rows("flags", flags, &row_part, &rows_per_part)?)
    {
        part.flags = v;
    }
    for (part, v) in out.iter_mut().zip(deal_rows(
        "residual_attrs",
        residual_attrs,
        &row_part,
        &rows_per_part,
    )?) {
        part.residual_attrs = v;
    }
    for (part, v) in out
        .iter_mut()
        .zip(deal_var_bytes(&severity_text, &row_part, &rows_per_part)?)
    {
        part.severity_text = v;
    }
    drop(severity_text);
    for (part, v) in out
        .iter_mut()
        .zip(deal_var_bytes(&body, &row_part, &rows_per_part)?)
    {
        part.body = v;
    }
    drop(body);
    for (part, (ids, validity)) in out.iter_mut().zip(deal_fixed_width(
        "trace_id",
        trace_id,
        &trace_id_validity,
        16,
        &row_part,
        &rows_per_part,
    )?) {
        part.trace_id = ids;
        part.trace_id_validity = validity;
    }
    drop(trace_id_validity);
    for (part, (ids, validity)) in out.iter_mut().zip(deal_fixed_width(
        "span_id",
        span_id,
        &span_id_validity,
        8,
        &row_part,
        &rows_per_part,
    )?) {
        part.span_id = ids;
        part.span_id_validity = validity;
    }
    drop(span_id_validity);

    // Stream directory: walking the parent's refs ascending gives each shard its
    // streams id-ascending (the parent's `stream_ids` are id-ascending), as dense
    // child refs. Each stream belongs to exactly one shard, so its blob moves.
    let mut child_ref = vec![0u32; stream_ids.len()];
    for (r, &used) in referenced.iter().enumerate() {
        if !used {
            continue;
        }
        let part = &mut out[stream_part[r]];
        child_ref[r] = part.stream_ids.len() as u32;
        part.stream_ids.push(stream_ids[r]);
        part.stream_attrs.push(std::mem::take(&mut stream_attrs[r]));
    }
    drop(stream_attrs);
    for (&r, &p) in stream_refs.iter().zip(&row_part) {
        out[p].stream_refs.push(child_ref[r as usize]);
    }
    drop(stream_refs);

    // Dynamic columns: keep the parent's `(name, type)` order, dropping any
    // column a shard leaves all-absent.
    for column in dyn_columns {
        let DynColumn {
            name,
            field_type,
            cells,
            validity,
        } = column;
        let mut present = vec![0usize; shards.len()];
        let mut bytes = vec![0usize; shards.len()];
        let mut slot = 0usize;
        for (row, &p) in row_part.iter().enumerate() {
            if validity.get(row) {
                present[p] += 1;
                bytes[p] += cells.bytes_at(slot).map_or(0, <[u8]>::len);
                slot += 1;
            }
        }
        let mut part_cells: Vec<DynCells> = present
            .iter()
            .zip(&bytes)
            .map(|(&n, &b)| DynCells::with_capacity(field_type, n, b))
            .collect();
        let mut part_validity: Vec<Bitmap> = rows_per_part
            .iter()
            .map(|&n| Bitmap::with_capacity(n))
            .collect();
        let mut slot = 0usize;
        for (row, &p) in row_part.iter().enumerate() {
            match (validity.get(row), slot < cells.len()) {
                (true, true) => {
                    part_cells[p].push_from(&cells, slot)?;
                    part_validity[p].push(true);
                    slot += 1;
                }
                (true, false) => {
                    return Err(LogSegError::MalformedColumnarBatch(format!(
                        "partition: {name} row {row} is present past its {slot} cells"
                    )));
                }
                (false, _) => part_validity[p].push(false),
            }
        }
        drop(cells);
        for (p, (cells, validity)) in part_cells.into_iter().zip(part_validity).enumerate() {
            if cells.is_empty() {
                continue;
            }
            out[p].dyn_columns.push(DynColumn {
                name: name.clone(),
                field_type,
                cells,
                validity,
            });
        }
    }

    Ok(shards.into_iter().zip(out).collect())
}

/// Refuses a column whose length is not the `want` its batch implies.
fn rows_agree(column: &str, len: usize, want: usize) -> Result<(), LogSegError> {
    if len == want {
        return Ok(());
    }
    Err(LogSegError::MalformedColumnarBatch(format!(
        "partition: {column} has {len} entries, expected {want}"
    )))
}

/// Moves each row's value into its part's vector, in row order. `values` must
/// hold one entry per row.
fn deal_rows<T>(
    column: &str,
    values: Vec<T>,
    row_part: &[usize],
    rows_per_part: &[usize],
) -> Result<Vec<Vec<T>>, LogSegError> {
    rows_agree(column, values.len(), row_part.len())?;
    let mut parts: Vec<Vec<T>> = rows_per_part
        .iter()
        .map(|&n| Vec::with_capacity(n))
        .collect();
    for (value, &p) in values.into_iter().zip(row_part) {
        parts[p].push(value);
    }
    Ok(parts)
}

/// Copies each row's value into its part's buffer, in row order, each part
/// reserved for its rows' bytes. `values` must hold one value per row. A part
/// holds a subset of a validated parent's bytes, so its checked push cannot
/// refuse; a refusal is returned rather than wrapping the part's offsets.
fn deal_var_bytes(
    values: &VarBytes,
    row_part: &[usize],
    rows_per_part: &[usize],
) -> Result<Vec<VarBytes>, LogSegError> {
    let mut bytes = vec![0usize; rows_per_part.len()];
    for (row, &p) in row_part.iter().enumerate() {
        bytes[p] += values.get(row).len();
    }
    let mut parts: Vec<VarBytes> = rows_per_part
        .iter()
        .zip(&bytes)
        .map(|(&n, &b)| VarBytes::with_capacity(n, b))
        .collect();
    for (row, &p) in row_part.iter().enumerate() {
        parts[p].try_push(values.get(row))?;
    }
    Ok(parts)
}

/// Deals a packed fixed-width optional column (dense over present rows, with a
/// per-row presence bitmap) into one packed column and bitmap per part.
fn deal_fixed_width(
    column: &str,
    packed: Vec<u8>,
    validity: &Bitmap,
    width: usize,
    row_part: &[usize],
    rows_per_part: &[usize],
) -> Result<Vec<(Vec<u8>, Bitmap)>, LogSegError> {
    rows_agree(column, validity.len(), row_part.len())?;
    rows_agree(column, packed.len(), validity.count_present() * width)?;
    let mut present = vec![0usize; rows_per_part.len()];
    for (row, &p) in row_part.iter().enumerate() {
        if validity.get(row) {
            present[p] += 1;
        }
    }
    let mut out: Vec<(Vec<u8>, Bitmap)> = rows_per_part
        .iter()
        .zip(&present)
        .map(|(&rows, &n)| (Vec::with_capacity(n * width), Bitmap::with_capacity(rows)))
        .collect();
    let mut slot = 0usize;
    for (row, &p) in row_part.iter().enumerate() {
        let (ids, present) = &mut out[p];
        if validity.get(row) {
            ids.extend_from_slice(&packed[slot * width..slot * width + width]);
            present.push(true);
            slot += 1;
        } else {
            present.push(false);
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) use tests::{
    assert_no_child_dictionaries, assert_parent_dictionaries, assert_same_objects,
    cloning_partition_reference_pre_2624, collect_objects, dictionary_writes,
};

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_logseg::StrColumnDict;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantHash;
    use ravel_types::logstream::AttrValue;

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

    pub(crate) async fn collect_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, Vec<u8>)> {
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

    /// The row/columnar byte-identity anchor above, for a keyed tenant: a
    /// format-version-3 record with a two-column clustering key and the
    /// `undeclared` bloom scope. Records share streams, so the key reorders
    /// rows inside each stream, and every data object is checked to carry the
    /// descriptor, so identity is not met by both paths dropping the layout.
    #[tokio::test]
    async fn columnar_write_matches_row_write_for_a_keyed_tenant() {
        use prost::Message;
        use ravel_catalog::config_key;
        use ravel_logseg::footer::{
            SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType, open,
        };
        use ravel_logseg::{Predicate, RlogConfig, RlogReader};
        use ravel_object_store::PutOptions;
        use ravel_proto::sys::v1 as proto;

        let seed = 0x00C0_FFEE_u64;
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(1_700_000_000_000_000_000));
        let tenant = TenantId::new("acme");
        let typed = |key: &str, ty: proto::TypedAttrColumnType| proto::TypedAttrColumn {
            key: key.to_string(),
            r#type: ty as i32,
        };
        let record = proto::TenantConfigRecord {
            format_version: 3,
            tenant_hash: tenant.hash().0.to_vec(),
            lifecycle_state: proto::TenantLifecycleState::Active as i32,
            typed_attr_columns: Some(proto::TypedAttrColumnConfig {
                columns: vec![
                    typed("k_str", proto::TypedAttrColumnType::Str),
                    typed("k_int", proto::TypedAttrColumnType::I64),
                ],
            }),
            clustering_key: Some(proto::ClusteringKeyConfig {
                columns: vec!["k_str".to_string(), "k_int".to_string()],
                bucket_width: proto::ClusteringBucketWidth::OneDay as i32,
                generation: 3,
            }),
            bloom_scope: proto::BloomScope::Undeclared as i32,
            created_unix_ns: 1,
            updated_unix_ns: 1,
            ..Default::default()
        };
        let config = config_key(&tenant.hash());

        // `diverse_records` on six streams instead of 48, eight records each.
        let records: Vec<NormalizedLogRecord> = diverse_records()
            .into_iter()
            .enumerate()
            .map(|(i, mut r)| {
                let res: Vec<(String, AttrValue)> = vec![
                    (
                        "service.name".to_string(),
                        AttrValue::Str("api".to_string()),
                    ),
                    ("host".to_string(), AttrValue::Str(format!("h{}", i % 6))),
                ];
                r.stream_id = log_stream_id(&res, "scope", "", &[]);
                r.stream_attrs = stream_attrs_bytes(&res, "scope", "", &[]);
                r
            })
            .collect();

        let store_row: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store_col: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        for store in [&store_row, &store_col] {
            store
                .put(
                    &config,
                    record.encode_to_vec().into(),
                    PutOptions::default(),
                )
                .await
                .expect("put config record");
        }
        let router = |store: &Arc<dyn ObjectStoreBackend>| {
            LogIngestRouter::with_rng(
                buffer_all(),
                Arc::clone(store),
                Arc::clone(&clock),
                overlay(),
                Arc::new(SeededRng::new(seed)),
            )
        };

        let router_row = router(&store_row);
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

        let router_col = router(&store_col);
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
        assert_eq!(
            objs_row, objs_col,
            "row and columnar paths must produce byte-identical stored objects for a keyed tenant"
        );

        let want = SortDescriptor {
            bucket_width: SortBucketWidth::OneDay,
            key_columns: vec![
                SortKeyColumn {
                    name: "k_str".to_string(),
                    ty: SortKeyType::Str,
                },
                SortKeyColumn {
                    name: "k_int".to_string(),
                    ty: SortKeyType::I64,
                },
            ],
        };
        let shards: std::collections::HashSet<u32> = records
            .iter()
            .map(|r| shard_for_log(&r.stream_id, 4))
            .collect();
        let mut data_objects = 0;
        let mut reordered_streams = 0;
        for (_, bytes) in objs_row.iter().filter(|(key, _)| key != &config) {
            let Ok(ftr) = open(bytes) else { continue };
            data_objects += 1;
            assert_eq!(ftr.sort_descriptor.as_ref(), Some(&want));
            assert_eq!(ftr.clustering_generation, 3);
            let reader = RlogReader::new(bytes, &RlogConfig::default()).expect("reader");
            let (rows, _) = reader.scan(&Predicate::And(vec![])).expect("scan");
            let mut by_stream: HashMap<_, Vec<i64>> = HashMap::new();
            for row in &rows {
                by_stream.entry(row.stream_id).or_default().push(row.ts_ns);
            }
            reordered_streams += by_stream.values().filter(|ts| !ts.is_sorted()).count();
        }
        assert_eq!(data_objects, shards.len(), "one data object per shard");
        // Within each stream `k_str` = "v{i}" sorts "v42" before "v6" and "v43"
        // before "v7", so the key moves all six streams away from ts order.
        assert_eq!(reordered_streams, 6);
    }

    /// ADR-2135: a format-version-3 config record carrying neither the
    /// clustering key (field 13) nor the bloom scope (field 14) writes the
    /// same objects as a tenant with no config record at all. Same seed and
    /// clock as above, so every stored byte other than the config record
    /// itself is compared.
    #[tokio::test]
    async fn a_record_without_layout_fields_writes_the_same_objects_as_no_record() {
        use prost::Message;
        use ravel_catalog::config_key;
        use ravel_object_store::PutOptions;
        use ravel_proto::sys::v1::{TenantConfigRecord, TenantLifecycleState};

        let seed = 0x00C0_FFEE_u64;
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(1_700_000_000_000_000_000));
        let tenant = TenantId::new("acme");

        let store_none: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store_v3: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let record = TenantConfigRecord {
            format_version: 3,
            tenant_hash: tenant.hash().0.to_vec(),
            lifecycle_state: TenantLifecycleState::Active as i32,
            created_unix_ns: 1,
            updated_unix_ns: 1,
            ..Default::default()
        };
        let config = config_key(&tenant.hash());
        store_v3
            .put(
                &config,
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put config record");

        for store in [&store_none, &store_v3] {
            let router = LogIngestRouter::with_rng(
                buffer_all(),
                Arc::clone(store),
                Arc::clone(&clock),
                overlay(),
                Arc::new(SeededRng::new(seed)),
            );
            router
                .write(
                    tenant.clone(),
                    diverse_records(),
                    WriteMode::Buffered,
                    Duration::from_secs(5),
                )
                .await
                .expect("buffered write enqueues");
            router.flush_all().await;
        }

        let objs_none = collect_objects(store_none.as_ref()).await;
        let mut objs_v3 = collect_objects(store_v3.as_ref()).await;
        let before = objs_v3.len();
        objs_v3.retain(|(key, _)| key != &config);
        assert_eq!(objs_v3.len(), before - 1, "the config record is set aside");
        assert!(!objs_none.is_empty());
        assert_eq!(
            objs_none, objs_v3,
            "a record without fields 13 and 14 must not change a stored byte"
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

        let parts = partition_columnar(batch, shard_count).expect("a valid batch partitions");
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

    // ---- #2624: partition by move ----

    use proptest::prelude::*;

    /// The reference `partition_columnar` is checked against: the partition as
    /// it stood before #2624, which borrows the parent, clones every cell,
    /// residual list and stream blob, and builds no dictionaries. Kept as it
    /// was apart from its name and the cell type: it reads each dynamic cell
    /// back as an `AttrValue` through [`DynCells::value`] and rebuilds each
    /// shard's typed cells with [`DynCells::from_values`], where it once
    /// cloned the parent's `AttrValue` cells directly.
    pub(crate) fn cloning_partition_reference_pre_2624(
        batch: &ColumnarLogBatch,
        shard_count: u32,
    ) -> Vec<(u32, ColumnarLogBatch)> {
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
                    acc.dyn_cells[c].push(col.cells.value(*slot).expect("a valid cell"));
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
                    cells: DynCells::from_values(batch.dyn_columns[c].field_type, &cells)
                        .expect("cells of the column's type"),
                    validity,
                });
            }

            result.push((shard, out));
        }
        result.sort_by_key(|(shard, _)| *shard);
        result
    }

    /// Partitions `batch` both ways and asserts each sub-batch validates and
    /// equals the cloning reference's whole, `dyn_col_dicts` included: the
    /// reference builds none, so a sub-batch carrying a dictionary fails here.
    fn assert_partition_matches_reference(
        batch: &ColumnarLogBatch,
        shard_count: u32,
    ) -> Vec<(u32, ColumnarLogBatch)> {
        let want = cloning_partition_reference_pre_2624(batch, shard_count);
        let got = partition_columnar(batch.clone(), shard_count).expect("a valid batch partitions");
        let got_shards: Vec<u32> = got.iter().map(|(s, _)| *s).collect();
        let want_shards: Vec<u32> = want.iter().map(|(s, _)| *s).collect();
        assert_eq!(got_shards, want_shards, "the same shards, ascending");
        for ((shard, part), (_, reference)) in got.iter().zip(&want) {
            part.validate().expect("every sub-batch validates");
            assert_eq!(
                part, reference,
                "shard {shard}: the moved sub-batch must equal the cloning reference's"
            );
        }
        got
    }

    fn partition_stream(host: u32) -> (ravel_types::logstream::LogStreamId, Vec<u8>) {
        let res: Vec<(String, AttrValue)> =
            vec![("host".to_string(), AttrValue::Str(format!("p{host}")))];
        (
            log_stream_id(&res, "scope", "", &[]),
            stream_attrs_bytes(&res, "scope", "", &[]),
        )
    }

    /// Three streams on three different shards of eight (so five shards get no
    /// rows) with 1, 4 and 9 rows, interleaved. Every row of a stream shares
    /// one timestamp. `only_first` is present on the first stream's rows only,
    /// so it is all-absent in the other two shards; `blob` holds a `Map` cell
    /// beside `Bytes` cells; trace and span ids alternate present and absent.
    fn uneven_records() -> Vec<LogRecord> {
        let shard_count = 8;
        let mut streams = Vec::new();
        let mut seen_shards = Vec::new();
        for host in 0.. {
            let (id, attrs) = partition_stream(host);
            let shard = shard_for_log(&id, shard_count);
            if !seen_shards.contains(&shard) {
                seen_shards.push(shard);
                streams.push((id, attrs));
            }
            if streams.len() == 3 {
                break;
            }
        }
        let rows_per_stream = [1usize, 4, 9];
        let mut order = Vec::new();
        let mut left = rows_per_stream;
        while left.iter().any(|&n| n > 0) {
            for (s, n) in left.iter_mut().enumerate().rev() {
                if *n > 0 {
                    order.push(s);
                    *n -= 1;
                }
            }
        }
        order
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let (stream_id, stream_attrs) = streams[s].clone();
                let mut attrs = vec![
                    ("k_str".to_string(), AttrValue::Str(format!("v{}", i % 5))),
                    ("k_int".to_string(), AttrValue::I64(i as i64)),
                ];
                if s == 0 {
                    attrs.push(("only_first".to_string(), AttrValue::Str("x".to_string())));
                }
                if i % 2 == 0 {
                    attrs.push(("raw".to_string(), AttrValue::Bytes(vec![i as u8 % 3, 7])));
                }
                attrs.push((
                    "blob".to_string(),
                    if i % 4 == 0 {
                        AttrValue::Map(vec![("a".to_string(), AttrValue::I64(1))])
                    } else {
                        AttrValue::Bytes(vec![1, 2])
                    },
                ));
                if i % 3 == 0 {
                    attrs.push(("k_str".to_string(), AttrValue::Str(format!("dup{i}"))));
                }
                LogRecord {
                    stream_id,
                    stream_attrs,
                    ts_ns: 1_000 + s as i64,
                    observed_ts_ns: 2_000 + i as i64,
                    severity_num: (i % 24) as u8,
                    severity_text: format!("S{}", i % 2),
                    body: format!("body {i}"),
                    trace_id: (i % 2 == 0).then_some([i as u8; 16]),
                    span_id: (i % 3 != 0).then_some([i as u8; 8]),
                    flags: i as u32,
                    attrs,
                }
            })
            .collect()
    }

    /// The moving partition yields the cloning reference's sub-batches over
    /// several shards of uneven size, absent cells, a dynamic column present in
    /// only one shard, and shards that receive no rows. The parent carries
    /// dictionaries on Str, Bytes (one holding a `Map` cell) and I64 columns;
    /// no sub-batch carries any. The parent's residual lists and stream blobs
    /// reappear in the sub-batches at their original heap addresses: they were
    /// moved, not copied. Typed cells have no per-cell allocation to move; they
    /// are copied by row into each part's packed buffers.
    #[test]
    fn partition_columnar_moves_cells_and_drops_dictionaries() {
        let shard_count = 8u32;
        let records = uneven_records();
        let mut batch = ColumnarLogBatch::from_records(&records).with_dictionaries();

        // A producer dictionary in its own order, with an entry no id
        // references, for `k_str`; and one on the `I64` column, which the
        // writer never reads.
        let k_str = batch
            .dyn_columns
            .iter()
            .position(|c| c.name == "k_str")
            .expect("k_str column");
        let parent = batch.dyn_col_dicts[k_str]
            .clone()
            .expect("k_str dictionary");
        let mut distinct = parent.distinct.clone();
        distinct.reverse();
        distinct.insert(1, b"unreferenced".to_vec());
        let ids = parent
            .ids
            .iter()
            .map(|&id| {
                let bytes = &parent.distinct[id as usize];
                distinct.iter().position(|d| d == bytes).expect("entry") as u32
            })
            .collect();
        batch.dyn_col_dicts[k_str] = Some(StrColumnDict { distinct, ids });
        let k_int = batch
            .dyn_columns
            .iter()
            .position(|c| c.name == "k_int")
            .expect("k_int column");
        batch.dyn_col_dicts[k_int] = Some(StrColumnDict {
            distinct: vec![Vec::new()],
            ids: vec![0; batch.dyn_columns[k_int].cells.len()],
        });
        batch.validate().expect("the parent validates");
        assert_parent_dictionaries(&batch, &["k_str", "k_int", "raw", "blob"], "blob");

        let parts = assert_partition_matches_reference(&batch, shard_count);
        assert_eq!(parts.len(), 3, "three shards receive rows, five do not");
        let mut rows: Vec<usize> = parts.iter().map(|(_, p)| p.num_rows).collect();
        rows.sort_unstable();
        assert_eq!(rows, [1, 4, 9]);
        let with_only_first = parts
            .iter()
            .filter(|(_, p)| p.dyn_columns.iter().any(|c| c.name == "only_first"))
            .count();
        assert_eq!(with_only_first, 1, "only_first is dropped where all-absent");
        assert_no_child_dictionaries(parts.iter().map(|(s, p)| (*s, p)));

        let str_ptrs = |b: &ColumnarLogBatch| {
            let mut ptrs: Vec<usize> = b
                .stream_attrs
                .iter()
                .map(|a| a.as_ptr() as usize)
                .chain(
                    b.residual_attrs
                        .iter()
                        .filter(|r| !r.is_empty())
                        .map(|r| r.as_ptr() as usize),
                )
                .collect();
            ptrs.sort_unstable();
            ptrs
        };
        let parent_ptrs = str_ptrs(&batch);
        let parts = partition_columnar(batch, shard_count).expect("a valid batch partitions");
        let mut child_ptrs: Vec<usize> = parts.iter().flat_map(|(_, p)| str_ptrs(p)).collect();
        child_ptrs.sort_unstable();
        assert_eq!(
            child_ptrs, parent_ptrs,
            "every stream blob and residual list is moved, not reallocated"
        );
    }

    /// A column shorter than the rows it describes is refused with a typed
    /// error naming it, not dealt short. `write_columnar` validates first, so
    /// only a direct call reaches this.
    #[test]
    fn partition_columnar_refuses_a_short_column_instead_of_truncating() {
        type Shorten = fn(&mut ColumnarLogBatch);
        let short: [(&str, Shorten); 5] = [
            ("flags", |b| {
                b.flags.pop();
            }),
            ("residual_attrs", |b| {
                b.residual_attrs.pop();
            }),
            ("ts_ns", |b| {
                b.ts_ns.pop();
            }),
            ("trace_id", |b| {
                b.trace_id.truncate(b.trace_id.len() - 16);
            }),
            ("k_str", |b| {
                if let Some(c) = b.dyn_columns.iter_mut().find(|c| c.name == "k_str") {
                    let mut shorter = DynCells::new(c.field_type);
                    for slot in 0..c.cells.len() - 1 {
                        shorter.push_from(&c.cells, slot).expect("same type");
                    }
                    c.cells = shorter;
                }
            }),
        ];
        for (column, shorten) in short {
            let mut batch = ColumnarLogBatch::from_records(&uneven_records());
            shorten(&mut batch);
            match partition_columnar(batch, 8) {
                Err(LogSegError::MalformedColumnarBatch(message)) => assert!(
                    message.contains(column),
                    "the error names {column}: {message}"
                ),
                other => panic!("a short {column} must be refused, got {other:?}"),
            }
        }
    }

    /// Records for the property test: few streams and attribute names, so
    /// streams repeat, timestamps tie, and columns are partly present; values
    /// mix Str, Bytes, I64 and a Map that shares the Bytes column.
    fn partition_record_strategy() -> impl Strategy<Value = LogRecord> {
        let value = prop_oneof![
            "[a-c]{1,2}".prop_map(AttrValue::Str),
            proptest::collection::vec(0u8..3, 1..3).prop_map(AttrValue::Bytes),
            (0i64..3).prop_map(AttrValue::I64),
            ("[a-b]", 0i64..2).prop_map(|(k, v)| AttrValue::Map(vec![(k, AttrValue::I64(v))])),
        ];
        (
            0u32..6,
            0i64..3,
            proptest::collection::vec(("[a-d]", value), 0..5),
            proptest::option::of(any::<[u8; 16]>()),
            proptest::option::of(any::<[u8; 8]>()),
            "[a-z]{0,4}",
            any::<u32>(),
        )
            .prop_map(|(host, ts_ns, attrs, trace_id, span_id, body, flags)| {
                let (stream_id, stream_attrs) = partition_stream(host);
                LogRecord {
                    stream_id,
                    stream_attrs,
                    ts_ns,
                    observed_ts_ns: ts_ns,
                    severity_num: (flags % 24) as u8,
                    severity_text: String::new(),
                    body,
                    trace_id,
                    span_id,
                    flags,
                    attrs,
                }
            })
    }

    proptest! {
        /// The moving partition equals the cloning reference over generated
        /// batches and shard counts, with and without dictionaries.
        #[test]
        fn partition_columnar_matches_the_cloning_reference(
            records in proptest::collection::vec(partition_record_strategy(), 0..40),
            shard_count in 1u32..9,
            dictionaries in any::<bool>(),
        ) {
            let mut batch = ColumnarLogBatch::from_records(&records);
            if dictionaries {
                batch = batch.with_dictionaries();
            }
            assert_partition_matches_reference(&batch, shard_count);
        }
    }

    /// The stored objects do not change when the moving partition replaces the
    /// cloning one: the same batch, carrying dictionaries on `k_str` (Str),
    /// `raw` (Bytes) and `nested` (`Map` cells in a Bytes column), written
    /// through each partition by two routers with one seed and one pinned
    /// clock gives byte-identical objects, and no shard's part carries a
    /// dictionary. Rows tie on `(stream, ts)`, so a partition that reordered
    /// rows within a shard would change bytes.
    #[tokio::test]
    async fn moved_partition_writes_the_same_objects_as_the_cloning_reference() {
        let seed = 0x00C0_FFEE_u64;
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(1_700_000_000_000_000_000));
        let tenant = TenantId::new("acme");
        let records: Vec<LogRecord> = diverse_records()
            .into_iter()
            .enumerate()
            .map(|(i, mut r)| {
                let res: Vec<(String, AttrValue)> = vec![
                    (
                        "service.name".to_string(),
                        AttrValue::Str("api".to_string()),
                    ),
                    ("host".to_string(), AttrValue::Str(format!("h{}", i % 6))),
                ];
                r.stream_id = log_stream_id(&res, "scope", "", &[]);
                r.stream_attrs = stream_attrs_bytes(&res, "scope", "", &[]);
                r.ts_ns = 1_000 + (i / 12) as i64;
                let mut record = to_logrecord(&r);
                record
                    .attrs
                    .push(("raw".to_string(), AttrValue::Bytes(vec![(i % 3) as u8, 4])));
                record
            })
            .collect();
        let batch = ColumnarLogBatch::from_records(&records).with_dictionaries();
        assert_parent_dictionaries(&batch, &["k_str", "raw", "nested"], "nested");

        let parts = partition_columnar(batch.clone(), 4).expect("a valid batch partitions");
        assert!(parts.len() > 1, "the fixture spans several shards");
        assert_no_child_dictionaries(parts.iter().map(|(s, p)| (*s, p)));

        let router = |store: &Arc<dyn ObjectStoreBackend>| {
            LogIngestRouter::with_rng(
                buffer_all(),
                Arc::clone(store),
                Arc::clone(&clock),
                overlay(),
                Arc::new(SeededRng::new(seed)),
            )
        };
        let store_ref: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_ref = router(&store_ref);
        router_ref
            .write_columnar_partitioned(
                tenant.clone(),
                batch.clone(),
                WriteMode::Buffered,
                Duration::from_secs(5),
                |b, n| Ok(cloning_partition_reference_pre_2624(&b, n)),
            )
            .await
            .expect("reference write enqueues");
        router_ref.flush_all().await;

        let store_new: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_new = router(&store_new);
        router_new
            .write_columnar(tenant, batch, WriteMode::Buffered, Duration::from_secs(5))
            .await
            .expect("moving write enqueues");
        router_new.flush_all().await;

        let objs_ref = collect_objects(store_ref.as_ref()).await;
        let objs_new = collect_objects(store_new.as_ref()).await;
        assert!(
            objs_ref.len() > parts.len(),
            "one data object per shard plus commit records, got {}",
            objs_ref.len()
        );
        assert_eq!(
            objs_ref, objs_new,
            "the moving partition must not change a stored byte"
        );
    }

    /// Asserts two stores' objects have the same keys and the same bytes,
    /// naming the first object that differs.
    pub(crate) fn assert_same_objects(want: &[(String, Vec<u8>)], got: &[(String, Vec<u8>)]) {
        let keys = |objs: &[(String, Vec<u8>)]| -> Vec<String> {
            objs.iter().map(|(key, _)| key.clone()).collect()
        };
        assert_eq!(keys(got), keys(want), "the same object keys");
        for ((key, want), (_, got)) in want.iter().zip(got) {
            assert!(
                got == want,
                "object {key} differs from the cloning reference's"
            );
        }
    }

    /// Asserts the parent `batch` carries a dictionary on each of `columns`,
    /// and that `map_column`, one of them, holds a `Map` cell.
    pub(crate) fn assert_parent_dictionaries(
        batch: &ColumnarLogBatch,
        columns: &[&str],
        map_column: &str,
    ) {
        assert!(columns.contains(&map_column), "{map_column} is listed");
        for name in columns {
            let kept = batch
                .dyn_columns
                .iter()
                .position(|c| c.name == *name)
                .map(|i| batch.dyn_col_dicts.get(i).is_some_and(Option::is_some));
            assert_eq!(kept, Some(true), "the parent carries {name}'s dictionary");
        }
        let has_map = batch
            .dyn_columns
            .iter()
            .filter(|c| c.name == map_column)
            .any(|c| match &c.cells {
                DynCells::Bytes(b) => b.nested.iter().any(|(_, v)| matches!(v, AttrValue::Map(_))),
                _ => false,
            });
        assert!(has_map, "the parent's {map_column} holds a Map cell");
    }

    /// Asserts no part carries a dictionary: the split drops them all.
    pub(crate) fn assert_no_child_dictionaries<'a>(
        parts: impl IntoIterator<Item = (u32, &'a ColumnarLogBatch)>,
    ) {
        for (shard, part) in parts {
            assert!(
                part.dyn_col_dicts.is_empty(),
                "shard {shard}: a part carries no dictionaries, got {:?}",
                part.dyn_col_dicts
            );
        }
    }

    /// Two writes over the same 24 streams, every row at one timestamp, so
    /// rows tie on `(stream, ts)` within and across writes and a flush that
    /// merged them in another order would change bytes. Both carry the
    /// dictionaries `with_dictionaries` derives on `k_str` and `svc` (Str) and
    /// on `raw` and `blob` (Bytes), except that the second has none on
    /// `k_str`. `blob` holds a `Map` cell every fifth row.
    pub(crate) fn dictionary_writes() -> Vec<ColumnarLogBatch> {
        (0..2u32)
            .map(|w| {
                let records: Vec<LogRecord> = (0..48u32)
                    .map(|i| {
                        let res: Vec<(String, AttrValue)> = vec![
                            (
                                "service.name".to_string(),
                                AttrValue::Str("api".to_string()),
                            ),
                            ("host".to_string(), AttrValue::Str(format!("m{}", i % 24))),
                        ];
                        let blob = if (i + w) % 5 == 0 {
                            AttrValue::Map(vec![("a".to_string(), AttrValue::I64(i64::from(w)))])
                        } else {
                            AttrValue::Bytes(vec![(i % 2) as u8, 5])
                        };
                        LogRecord {
                            stream_id: log_stream_id(&res, "scope", "", &[]),
                            stream_attrs: stream_attrs_bytes(&res, "scope", "", &[]),
                            ts_ns: 1_000,
                            observed_ts_ns: 2_000 + i64::from(i),
                            severity_num: (i % 24) as u8,
                            severity_text: "INFO".to_string(),
                            body: format!("w{w} row {i}"),
                            trace_id: (i % 2 == 0).then_some([i as u8; 16]),
                            span_id: (i % 3 == 0).then_some([(i + w) as u8; 8]),
                            flags: i,
                            attrs: vec![
                                (
                                    "k_str".to_string(),
                                    AttrValue::Str(format!("v{}", (i + w) % 7)),
                                ),
                                ("svc".to_string(), AttrValue::Str(format!("s{}", i % 3))),
                                (
                                    "raw".to_string(),
                                    AttrValue::Bytes(vec![((i + w) % 4) as u8, 9]),
                                ),
                                ("blob".to_string(), blob),
                                ("k_int".to_string(), AttrValue::I64(i64::from(i))),
                            ],
                        }
                    })
                    .collect();
                let mut batch = ColumnarLogBatch::from_records(&records).with_dictionaries();
                if w == 1 {
                    let k_str = batch
                        .dyn_columns
                        .iter()
                        .position(|c| c.name == "k_str")
                        .expect("k_str column");
                    batch.dyn_col_dicts[k_str] = None;
                }
                batch.validate().expect("the write validates");
                batch
            })
            .collect()
    }

    /// Two columnar writes merged into each shard's buffer and written by one
    /// flush per shard store the same objects whether the router partitions
    /// them by move or with the cloning reference. Both writes carry
    /// dictionaries on Str and Bytes columns, one of them holding a `Map`
    /// cell; no part of either carries one.
    #[tokio::test]
    async fn merged_columnar_writes_store_the_same_objects_as_the_cloning_reference() {
        let seed = 0x00C0_FFEE_u64;
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(1_700_000_000_000_000_000));
        let shard_count = buffer_all().shard_count;
        let writes = dictionary_writes();
        for write in &writes {
            assert_parent_dictionaries(write, &["svc", "raw", "blob"], "blob");
        }

        let parts: Vec<Vec<(u32, ColumnarLogBatch)>> = writes
            .iter()
            .map(|w| partition_columnar(w.clone(), shard_count).expect("a valid batch partitions"))
            .collect();
        let shards_of = |parts: &[(u32, ColumnarLogBatch)]| -> Vec<u32> {
            parts.iter().map(|(s, _)| *s).collect()
        };
        let shards = shards_of(&parts[0]);
        assert!(shards.len() > 1, "the writes span several shards");
        assert_eq!(
            shards_of(&parts[1]),
            shards,
            "both writes reach the same shards"
        );
        for write_parts in &parts {
            assert_no_child_dictionaries(write_parts.iter().map(|(s, p)| (*s, p)));
        }

        let tenant = TenantId::new("acme");
        let router = |store: &Arc<dyn ObjectStoreBackend>| {
            LogIngestRouter::with_rng(
                buffer_all(),
                Arc::clone(store),
                Arc::clone(&clock),
                overlay(),
                Arc::new(SeededRng::new(seed)),
            )
        };
        let store_ref: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_ref = router(&store_ref);
        for batch in &writes {
            router_ref
                .write_columnar_partitioned(
                    tenant.clone(),
                    batch.clone(),
                    WriteMode::Buffered,
                    Duration::from_secs(5),
                    |b, n| Ok(cloning_partition_reference_pre_2624(&b, n)),
                )
                .await
                .expect("reference write enqueues");
        }
        router_ref.flush_all().await;

        let store_new: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router_new = router(&store_new);
        for batch in writes {
            router_new
                .write_columnar(
                    tenant.clone(),
                    batch,
                    WriteMode::Buffered,
                    Duration::from_secs(5),
                )
                .await
                .expect("moving write enqueues");
        }
        router_new.flush_all().await;

        let objs_ref = collect_objects(store_ref.as_ref()).await;
        let objs_new = collect_objects(store_new.as_ref()).await;
        let commits = objs_ref
            .iter()
            .filter(|(key, _)| key.contains("/c/"))
            .count();
        assert_eq!(
            commits,
            shards.len(),
            "one flush per shard, so each shard's two writes share one object"
        );
        assert_same_objects(&objs_ref, &objs_new);
    }
}
