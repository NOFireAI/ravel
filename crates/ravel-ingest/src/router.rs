//! Owns the shard actors and fans writes out to them
//! (docs/ingest.md "Structure").

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedPoint;
use ravel_types::{CommitToken, Signal, TenantId, shard_for};
use tokio::sync::{mpsc, oneshot};

use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};
use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::error::WriteError;
use crate::generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed, load_generations};
use crate::metrics::IngestMetrics;
use crate::shard::{ShardActor, ShardMsg};
#[cfg(feature = "stage-timing")]
use crate::stage_timing::{MetricStage, MetricStageTimings};
use crate::value::{IngestExemplar, IngestPoint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Ack only after every involved shard's flush has both its data object
    /// and commit record durably stored.
    Strict,
    /// Ack at enqueue; never durable on its own (docs/consistency-model.md).
    Buffered,
}

/// One token per shard the request's points flushed through. Empty in
/// buffered mode, or if the request carried no points.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteReceipt {
    pub tokens: Vec<CommitToken>,
}

/// The router's live handle to one shard actor. Interior-mutable because a
/// handle lives inside a [`GenerationSwitch`] set behind `Arc<Vec<ShardHandle>>`
/// (shared, never mutated in place), yet the supervisor must swap in a fresh
/// sender when it respawns the actor after a death (issue #1299).
struct ShardHandle {
    inner: Mutex<ShardInner>,
    /// The ADR-1307 monotonic flush-open floor for this shard index, owned here
    /// and handed to every incarnation of the actor. The guarantee is
    /// per-shard, not per-actor: `writer_id` is not part of the query-time
    /// duplicate-resolution comparator (ADR-1307 "Known limitation",
    /// docs/catalog-and-mvcc.md), so a fresh writer identity does not rescue a
    /// `created_unix_ns` that ran backwards across a respawn. A floor reset per
    /// incarnation would narrow the guarantee to one actor's lifetime, which is
    /// exactly the window a respawn ends.
    flush_floor_ns: Arc<AtomicI64>,
}

/// Supervisor state guarded together so a death observation, the respawn that
/// replaces `tx`, and the incarnation a concurrent observer compares against
/// are one atomic transition.
struct ShardInner {
    /// Sender for the current live actor incarnation.
    tx: mpsc::Sender<ShardMsg>,
    /// Bumped on every respawn. A write captures it beside the sender it sends
    /// on; a later death report carrying a stale incarnation is a duplicate of a
    /// death already counted and already respawned, and is ignored.
    incarnation: u64,
    /// Respawns spent on this shard since the last decay. Once it reaches
    /// [`IngestRouter::MAX_SHARD_RESPAWNS`] the next death condemns the shard
    /// instead of respawning it. Decays to zero when the shard has run a whole
    /// [`IngestRouter::respawn_decay_window_ns`] without dying, so the budget
    /// bounds a crash loop rather than a process lifetime.
    respawns: u32,
    /// Clock reading at the most recent respawn, for the decay above. Only read
    /// when `respawns > 0`, so the initial zero is never compared.
    last_respawn_ns: i64,
    /// Set once the shard has exhausted its respawn budget: the router stops
    /// respawning it and reports not-ready, which sheds traffic from this
    /// replica but does not replace it. One-way.
    condemned: bool,
}

impl ShardHandle {
    fn new(tx: mpsc::Sender<ShardMsg>, flush_floor_ns: Arc<AtomicI64>) -> Self {
        ShardHandle {
            inner: Mutex::new(ShardInner {
                tx,
                incarnation: 0,
                respawns: 0,
                last_respawn_ns: 0,
                condemned: false,
            }),
            flush_floor_ns,
        }
    }

    /// Poison-recovering lock: a shard's supervisor state is best-effort
    /// self-healing, not a durability path, so a prior panicked holder must not
    /// take this one down (matches `GenerationSwitch::lock`).
    fn lock(&self) -> std::sync::MutexGuard<'_, ShardInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The current sender and the incarnation it belongs to: a write sends on
    /// the sender and, if it fails, attributes the death to that incarnation.
    fn send_target(&self) -> (mpsc::Sender<ShardMsg>, u64) {
        let inner = self.lock();
        (inner.tx.clone(), inner.incarnation)
    }
}

/// Routes writes to generation-versioned shard-actor sets (ADR-0052).
///
/// The generation-0 set of `config.shard_count` shard actors is spawned once at
/// construction; a reshard's activation spawns the new generation's set lazily
/// through the [`GenerationSwitch`] factory and routes subsequent writes to it,
/// while the old set keeps draining. Task count is bounded by the sum of live
/// generations' shard counts, not by write volume: no path spawns a task per
/// message or per point.
pub struct IngestRouter {
    switch: GenerationSwitch<ShardHandle>,
    store: Arc<dyn ObjectStoreBackend>,
    signal: Signal,
    clock: Arc<dyn Clock>,
    /// Randomness source for minting a fresh writer identity when respawning a
    /// dead shard actor (issue #1299). The same source the initial factory and
    /// the shard actors' PUT-retry jitter draw from (ADR-0068 decision 2).
    rng: Arc<dyn RngSource>,
    metrics: Arc<IngestMetrics>,
    config: IngestConfig,
    /// Process-wide ingest buffer byte budget (ADR-0069 decision 1). Shared by `Arc` with the log and span routers so one ceiling
    /// bounds the sum across all signals. Defaults to `Unlimited` for callers
    /// (chiefly tests) that build a router without one via [`IngestRouter::new`];
    /// `services/ravel-server` installs the configured budget with
    /// [`IngestRouter::with_budget`].
    budget: Arc<IngestByteBudget>,
    /// Per-stage timing accumulator (ADR-0104 decision 1), shared by `Arc` with
    /// every shard actor and flush task so the seam records into one table the
    /// bench reporter reads via [`IngestRouter::stage_timings`]. Present only
    /// under the `stage-timing` feature; with it off this field, and every
    /// timing site, is compiled out.
    #[cfg(feature = "stage-timing")]
    stage_timings: Arc<MetricStageTimings>,
}

impl IngestRouter {
    /// How many times the router respawns a shard actor before condemning it
    /// (issue #1299). A shard actor dies only by panicking mid-flush, and the
    /// two causes want opposite responses. A transient cause (a split-brain
    /// from a racing writer that has since lost, a spurious store failure
    /// surfaced as a panic) clears under a fresh actor with a new writer
    /// identity, so some respawns are worth trying. A deterministic cause (a
    /// poison-pill buffered point, a corrupt object every flush re-reads)
    /// reproduces on every incarnation, so unbounded respawning is a hot crash
    /// loop that never makes progress. Three bounds the loop: enough to ride
    /// out a couple of independent transients, few enough that a deterministic
    /// killer condemns the shard quickly. The
    /// buffered points the dead actor held are lost on every respawn (a respawn
    /// restores write capacity for the shard, not its buffer); the strict-mode
    /// writer already saw `ShardUnavailable` for them.
    ///
    /// The budget is spent within a window, not over the process lifetime: see
    /// [`IngestRouter::respawn_decay_window_ns`].
    pub const MAX_SHARD_RESPAWNS: u32 = 3;

    /// How long a shard actor must run without dying before its spent respawns
    /// decay to zero (issue #1299): `config.max_flush_lifetime`, one hour by
    /// default.
    ///
    /// Without a decay the budget is a process-lifetime allowance, so three
    /// unrelated transients spread over days condemn a shard that recovered
    /// cleanly from each one, and a long-lived replica is condemned by its own
    /// age rather than by a crash loop. The budget is supposed to separate a
    /// deterministic killer from independent transients, and time is what
    /// distinguishes them.
    ///
    /// The window has to be long enough that a deterministic killer cannot
    /// outlive it between deaths. A poison-pill point kills the actor on the
    /// next flush it is part of, and every flush path is bounded by a flush
    /// cadence far shorter than this: `max_flush_delay` (2s by default) when a
    /// tenant is writing, `max_flush_delay_idle` (40s) when it is quiet, so an
    /// hour is at least 90 times the slowest cadence that can carry the killer
    /// to the next death. `max_flush_lifetime` is the reference rather than a
    /// fresh constant because it is already the configured outer bound on how
    /// long one flush attempt may take: no single flush, and therefore no
    /// death caused by one, can straddle a whole window.
    ///
    /// Decay does not un-condemn a shard. Condemnation stays one-way and
    /// `ready()` stays monotonic; what decays is the budget that has not been
    /// exhausted yet.
    fn respawn_decay_window_ns(&self) -> i64 {
        i64::try_from(self.config.max_flush_lifetime.as_nanos()).unwrap_or(i64::MAX)
    }

    /// Construct with the production OS-entropy randomness source. Writer ids
    /// and PUT-retry backoff jitter draw from OS entropy, unchanged from
    /// before the [`RngSource`] seam (ADR-0068 decision 2).
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        signal: Signal,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::with_rng(config, store, signal, clock, Arc::new(SystemRng))
    }

    /// Construct with an injected [`RngSource`]. The simulation harness passes
    /// a seeded source derived from its master seed so writer ids and retry
    /// jitter are reproducible (ADR-0068 decision 2); production wiring uses
    /// [`IngestRouter::new`], whose source is OS entropy.
    pub fn with_rng(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        signal: Signal,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn RngSource>,
    ) -> Self {
        // Preallocate the lock-free per-shard skew accumulators for this
        // router's fixed shard set (issue #865), so the per-message enqueue and
        // process paths never contend on a shared lock.
        let metrics = Arc::new(IngestMetrics::new(config.shard_count));
        #[cfg(feature = "stage-timing")]
        let stage_timings = Arc::new(MetricStageTimings::new());
        // Each generation's shard-actor set gets a fresh writer identity, so
        // two sets never collide on a commit key for the same shard index.
        let factory = {
            let store = Arc::clone(&store);
            let clock = Arc::clone(&clock);
            let rng = Arc::clone(&rng);
            let metrics = Arc::clone(&metrics);
            #[cfg(feature = "stage-timing")]
            let stage_timings = Arc::clone(&stage_timings);
            move |shard_count: u32| -> Vec<ShardHandle> {
                let writer_id = rng.new_uuid();
                let epoch =
                    u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
                (0..shard_count)
                    .map(|shard| {
                        let (tx, rx) = mpsc::channel(config.channel_depth);
                        // One flush-open floor per shard index, shared with
                        // every later incarnation of this actor (ADR-1307).
                        let flush_floor_ns = Arc::new(AtomicI64::new(0));
                        let actor = ShardActor::new(
                            shard,
                            signal,
                            writer_id,
                            epoch,
                            Arc::clone(&store),
                            Arc::clone(&clock),
                            Arc::clone(&rng),
                            config,
                            Arc::clone(&metrics),
                            rx,
                            Arc::clone(&flush_floor_ns),
                            #[cfg(feature = "stage-timing")]
                            Arc::clone(&stage_timings),
                        );
                        tokio::spawn(actor.run());
                        ShardHandle::new(tx, flush_floor_ns)
                    })
                    .collect()
            }
        };
        let switch =
            GenerationSwitch::new(config.shard_count, DEFAULT_REFRESH_INTERVAL_NS, factory);

        IngestRouter {
            switch,
            store,
            signal,
            clock,
            rng,
            metrics,
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
    pub fn stage_timings(&self) -> Arc<MetricStageTimings> {
        Arc::clone(&self.stage_timings)
    }

    /// Installs the process-wide ingest buffer byte budget (ADR-0069 decision
    /// 1). `services/ravel-server` builds one [`IngestByteBudget`] at startup
    /// and calls this on each of the metrics, log, and span routers with the
    /// same `Arc`, so a single `--max-ingest-buffer-bytes` ceiling bounds the
    /// buffered-byte sum across every signal.
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<IngestByteBudget>) -> Self {
        self.budget = budget;
        self
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.metrics
    }

    /// The counter registry as a shared handle, for a process-level component
    /// that outlives a borrow and must count into the same registry the
    /// `/metrics` snapshot reads: the metric metadata sink
    /// ([`crate::MetadataSink`], ADR-0085 decision 1) is spawned as its own task
    /// and holds this rather than a second, invisible `IngestMetrics`.
    pub fn metrics_handle(&self) -> Arc<IngestMetrics> {
        self.metrics.clone()
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
    ) -> Result<Arc<Vec<ShardHandle>>, WriteError> {
        match self.switch.route_cached(tenant, now_ns) {
            Routed::Fresh(set) => Ok(set),
            Routed::Stale => {
                match load_generations(
                    self.store.as_ref(),
                    self.signal,
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
                            Err(WriteError::StaleProvisioningView)
                        }
                    },
                }
            }
        }
    }

    /// Update a tenant's cached shard-generation view (ADR-0052 section 2). The
    /// server's background refresher calls this on interval `C` with the
    /// tenant's decoded provisioning history so an activation is observed before
    /// it takes effect and the staleness guard stays satisfied.
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
    /// views dropped. The server's idle-tenant sweep loop drives this; an
    /// evicted view is re-derived from the provisioning record on the tenant's
    /// next write ([`GenerationSwitch::evict_idle`]).
    pub fn evict_idle_generation_views(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.switch.evict_idle(now_ns, ttl_ns)
    }

    /// Groups `points` by `shard_for`, sends one `ShardMsg::Write` per
    /// involved shard, and (in strict mode) awaits every involved shard's
    /// ack within `ack_deadline`. Sending blocks on a full channel: that
    /// backpressure is intentional (docs/ingest.md "Channel").
    pub async fn write(
        &self,
        tenant: TenantId,
        points: Vec<NormalizedPoint>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<WriteReceipt, WriteError> {
        self.write_points(
            tenant,
            points.into_iter().map(IngestPoint::from).collect(),
            Vec::new(),
            mode,
            ack_deadline,
        )
        .await
    }

    /// Like [`Self::write`], but for points that already carry their value
    /// shape (scalar or histogram) rather than OTLP's `NormalizedPoint`.
    /// Both entry points reach the same
    /// shard buffer and the same RSEG v5 writer; this one is for callers
    /// that construct [`IngestPoint`]s directly, chiefly the wire surfaces
    /// mixing scalar and native-histogram points from one request.
    pub async fn write_values(
        &self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<WriteReceipt, WriteError> {
        self.write_points(tenant, points, Vec::new(), mode, ack_deadline)
            .await
    }

    /// Like [`Self::write_values`], additionally carrying the exemplars a
    /// normalize path admitted for these points (ADR-0047 decision 1). Each
    /// exemplar routes to `shard_for(series_id)`, the same shard its series'
    /// samples route to, so it lands in the buffer that will flush the object
    /// holding its parent sample. An exemplar whose parent sample is not in
    /// that flush is dropped and counted there, never written.
    pub async fn write_values_with_exemplars(
        &self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<WriteReceipt, WriteError> {
        self.write_points(tenant, points, exemplars, mode, ack_deadline)
            .await
    }

    async fn write_points(
        &self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<WriteReceipt, WriteError> {
        // An empty write never routes and never fails: nothing to flush, so a
        // stale provisioning view is irrelevant to it.
        if points.is_empty() && exemplars.is_empty() {
            return Ok(WriteReceipt::default());
        }

        // Global ingest byte budget (ADR-0069 decision 1). Charge the estimated
        // buffered bytes here -- after decode/normalize/admission (the caller
        // hands us the admitted points), before any shard buffer is touched --
        // and shed at the ceiling before routing so a shed request touches no
        // shard and mints no commit token. The charge is refunded when the
        // flush(es) holding these bytes complete or fail: the guard is cloned
        // into every shard message below, each shard buffer holds its clone
        // until it flushes, and `IngestByteCharge::drop` refunds the exact
        // amount once the last clone is dropped. On any early return from here
        // on (a stale provisioning view, a dead shard) the clones not yet
        // handed to a live shard drop with this frame, so nothing leaks.
        #[cfg(feature = "stage-timing")]
        let admit_start = std::time::Instant::now();
        let estimate: u64 = points
            .iter()
            .map(IngestPoint::est_charge_bytes)
            .fold(0u64, u64::saturating_add)
            .saturating_add(
                exemplars
                    .iter()
                    .map(|e| e.est_bytes() as u64)
                    .fold(0u64, u64::saturating_add),
            );
        let charge = Arc::new(
            self.budget
                .try_charge(estimate)
                .map_err(|_| WriteError::BufferBudgetExceeded)?,
        );
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(MetricStage::Admit, admit_start.elapsed());

        // Route this write against the tenant's current generation view,
        // re-reading the provisioning record when the cache is older than `C`
        // and failing closed if that read cannot complete (ADR-0052 section 3).
        #[cfg(feature = "stage-timing")]
        let route_start = std::time::Instant::now();
        let set = self.active_set(tenant.hash(), self.clock.now_ns()).await?;
        let shard_count = set.len() as u32;
        let mut by_shard: HashMap<u32, Vec<IngestPoint>> = HashMap::new();
        for point in points {
            let shard = shard_for(&point.series_id, shard_count);
            by_shard.entry(shard).or_default().push(point);
        }
        // Exemplars route by their own series id, which is the same shard
        // their samples took. A shard that got exemplars but no points is
        // still involved: its buffer may already hold the parent samples from
        // an earlier request in this flush window.
        let mut exemplars_by_shard: HashMap<u32, Vec<IngestExemplar>> = HashMap::new();
        for exemplar in exemplars {
            let shard = shard_for(&exemplar.series_id, shard_count);
            exemplars_by_shard.entry(shard).or_default().push(exemplar);
        }
        let mut shard_ids: Vec<u32> = by_shard
            .keys()
            .chain(exemplars_by_shard.keys())
            .copied()
            .collect();
        shard_ids.sort_unstable();
        shard_ids.dedup();

        // Parallel to `ack_rxs`: the (shard, incarnation) each receiver belongs
        // to, so a closed ack channel is attributed to the right shard and to
        // the exact actor incarnation this request sent on (issue #1299), and
        // counted as that incarnation's death rather than a duplicate of one
        // already respawned.
        let mut ack_shards: Vec<(u32, u64)> = Vec::with_capacity(shard_ids.len());
        let mut ack_rxs = Vec::with_capacity(shard_ids.len());
        for shard in shard_ids {
            let points = by_shard.remove(&shard).unwrap_or_default();
            // The current live sender for this shard and the incarnation it
            // belongs to, captured together so a send failure or a dropped ack
            // is attributed to exactly the incarnation this request used.
            let (shard_tx, incarnation) = set[shard as usize].send_target();
            // Strict mode acknowledges the points this request sent. A shard
            // that received only exemplars gets no ack: an exemplar is a
            // decoration on a measurement (ADR-0047 decision 1), its loss is
            // counted and visible rather than acknowledged, and its flush may
            // write nothing at all when the parent samples are not buffered.
            // Minting an ack for that shard would leave a waiter its flush
            // path has no token to answer with, and a dropped oneshot reads as
            // a dead shard.
            let ack = match mode {
                WriteMode::Strict if !points.is_empty() => {
                    let (tx, rx) = oneshot::channel();
                    ack_shards.push((shard, incarnation));
                    ack_rxs.push(rx);
                    Some(tx)
                }
                WriteMode::Strict | WriteMode::Buffered => None,
            };
            let msg = ShardMsg::Write {
                tenant: tenant.clone(),
                points,
                exemplars: exemplars_by_shard.remove(&shard).unwrap_or_default(),
                ack,
                charge: Some(Arc::clone(&charge)),
            };
            if shard_tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this incarnation is dead. Observe it (which
                // counts the death once and respawns the shard within its
                // budget) and surface the typed error rather than acking as if
                // the points landed.
                self.observe_shard_death(shard, &set[shard as usize], incarnation);
                return Err(WriteError::ShardUnavailable);
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
            .record(MetricStage::Route, route_start.elapsed());

        if mode == WriteMode::Buffered {
            return Ok(WriteReceipt::default());
        }

        // `join_all` preserves input order, so `joined[i]` is `ack_shards[i]`.
        // On a deadline elapse the whole `join_all` future is dropped, so no
        // per-shard ack is observed: `AckTimeout` carries no recovered tokens
        // (a sibling that committed inside the elapsed window is unknowable
        // here, and reporting an unresolved ack as durable would be wrong).
        let joined = tokio::time::timeout(ack_deadline, futures::future::join_all(ack_rxs))
            .await
            .map_err(|_| WriteError::AckTimeout)?;

        // Every ack resolved. Scan them all: collect every shard that acked a
        // durable commit (issue #1130), and record the first failure in shard
        // order so the returned classification and the death side effect match
        // the pre-fix early-return. A shard whose ack failed to resolve
        // (`RecvError`: the actor panicked mid-flush) is NOT a durable write and
        // contributes no token; its (shard, incarnation) is remembered so the
        // death is observed against the exact incarnation this write sent on.
        let mut durable = Vec::with_capacity(joined.len());
        let mut first_error: Option<WriteError> = None;
        let mut dead_shard: Option<(u32, u64)> = None;
        for ((shard, incarnation), result) in ack_shards.into_iter().zip(joined) {
            match result {
                Ok(Ok(token)) => durable.push(token),
                Ok(Err(shard_error)) => {
                    if first_error.is_none() {
                        first_error = Some(shard_error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(WriteError::ShardUnavailable);
                        dead_shard = Some((shard, incarnation));
                    }
                }
            }
        }

        if let Some(inner) = first_error {
            // Observe the death only when the first failure in shard order is a
            // dropped ack (a resolved shard-level error is not a death). This
            // counts the death once for this incarnation and respawns the shard
            // within its budget, or condemns it past the budget.
            if let Some((shard, incarnation)) = dead_shard {
                self.observe_shard_death(shard, &set[shard as usize], incarnation);
            }
            // Carry the durably-acked sibling tokens only when there are any; a
            // failure with no partial success surfaces as the bare variant,
            // unchanged from before this fix.
            let error = if durable.is_empty() {
                inner
            } else {
                self.metrics.record_partial_write();
                WriteError::PartialWrite {
                    inner: Box::new(inner),
                    durable,
                }
            };
            return Err(error);
        }

        Ok(WriteReceipt { tokens: durable })
    }

    /// Observe one death report for `shard`'s actor at `observed_incarnation`
    /// (issue #1299). Deduped by incarnation, so concurrent observers of the
    /// same death, and later writes that route to an already-replaced actor,
    /// count the death once. Within the respawn budget it spawns a fresh actor
    /// and swaps in its sender under the same lock, so the next write to this
    /// shard reaches a live actor; past the budget the shard is condemned (no
    /// further respawn) and [`Self::ready`] reports not-ready, which sheds
    /// traffic from this replica without replacing it. The dead actor's
    /// buffered points are lost either way (docs/ingest.md "Metrics
    /// (self-observability)").
    ///
    /// The budget decays first: a shard that ran a whole
    /// [`Self::respawn_decay_window_ns`] without a death starts this one from
    /// zero, so the budget bounds a crash loop instead of a process lifetime.
    fn observe_shard_death(&self, shard: u32, handle: &ShardHandle, observed_incarnation: u64) {
        let mut inner = handle.lock();
        // A duplicate report of a death already handled: another observer got
        // here first and respawned (so the live incarnation moved on), or the
        // shard is already condemned. Count nothing.
        if inner.incarnation != observed_incarnation || inner.condemned {
            return;
        }
        self.metrics.record_shard_death();
        let now_ns = self.clock.now_ns();
        if inner.respawns > 0
            && now_ns.saturating_sub(inner.last_respawn_ns) >= self.respawn_decay_window_ns()
        {
            inner.respawns = 0;
        }
        if inner.respawns >= Self::MAX_SHARD_RESPAWNS {
            inner.condemned = true;
            self.metrics.record_shard_condemned();
            return;
        }
        inner.respawns += 1;
        inner.last_respawn_ns = now_ns;
        inner.incarnation += 1;
        inner.tx = self.spawn_shard_actor(shard, Arc::clone(&handle.flush_floor_ns));
    }

    /// Spawn a replacement actor for `shard` with a fresh writer identity and
    /// return its sender (issue #1299). A new `writer_id` keeps the
    /// replacement's object and commit keys disjoint from the dead
    /// incarnation's, so a late PUT from the old actor cannot collide with the
    /// new one. This restores write capacity for the shard, not the buffered
    /// points the dead actor lost.
    ///
    /// `flush_floor_ns` is the shard's existing ADR-1307 floor, not a fresh
    /// one: the replacement must not be free to stamp a `created_unix_ns`
    /// behind what the dead incarnation already published for the same shard.
    fn spawn_shard_actor(
        &self,
        shard: u32,
        flush_floor_ns: Arc<AtomicI64>,
    ) -> mpsc::Sender<ShardMsg> {
        let writer_id = self.rng.new_uuid();
        let epoch =
            u64::try_from(self.clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
        let (tx, rx) = mpsc::channel(self.config.channel_depth);
        let actor = ShardActor::new(
            shard,
            self.signal,
            writer_id,
            epoch,
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            Arc::clone(&self.rng),
            self.config,
            Arc::clone(&self.metrics),
            rx,
            flush_floor_ns,
            #[cfg(feature = "stage-timing")]
            Arc::clone(&self.stage_timings),
        );
        tokio::spawn(actor.run());
        tx
    }

    /// Whether every shard actor this router owns is live enough to serve:
    /// false once any shard has exhausted its respawn budget and been condemned
    /// (issue #1299). `services/ravel-server` ANDs it into `/readyz`, so a
    /// false here sheds traffic from this replica: Kubernetes removes the pod
    /// from its Service endpoints. It does not restart or reschedule the pod
    /// (`/healthz` is deliberately independent of ingest health), so recovering
    /// the shard needs an operator to roll it.
    ///
    /// Monotonic, deliberately. It reads the condemned-shard counter rather
    /// than counting live condemned handles, because a `ShardHandle` is never
    /// dropped for the process lifetime: `GenerationSwitch::evict_idle` sweeps
    /// only the per-tenant views and never the shard-actor sets (see
    /// `generation.rs`), so a live count would equal this counter and cost a
    /// generation lock plus a per-handle lock on a path that must stay
    /// lock-free. Condemnation is terminal for the process either way, since
    /// nothing un-condemns a shard; what recovers a transient is the respawn
    /// budget's decay ([`Self::respawn_decay_window_ns`]), which keeps a shard
    /// out of condemnation in the first place.
    pub fn ready(&self) -> bool {
        self.metrics.condemned_shards() == 0
    }

    /// Forces every shard to flush all buffered tenants now, for tests and
    /// graceful shutdown paths that need durability without waiting on
    /// `max_flush_delay`.
    pub async fn flush_all(&self) {
        let sets = self.switch.all_sets();
        let mut dones = Vec::new();
        for set in &sets {
            for shard in set.iter() {
                let (done_tx, done_rx) = oneshot::channel();
                let (shard_tx, _) = shard.send_target();
                if shard_tx
                    .send(ShardMsg::FlushNow { done: done_tx })
                    .await
                    .is_ok()
                {
                    dones.push(done_rx);
                }
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }

    /// Flushes every live generation's shard actors, across all generations, so
    /// a retiring generation's buffers drain too (ADR-0052 section 2). The actor
    /// tasks end on their own once their channels close after the drain; the
    /// `done` acknowledgement fires after the flush, so durability holds without
    /// joining the detached tasks.
    pub async fn shutdown(self) {
        let sets = self.switch.all_sets();
        let mut dones = Vec::new();
        for set in &sets {
            for shard in set.iter() {
                let (done_tx, done_rx) = oneshot::channel();
                let (shard_tx, _) = shard.send_target();
                let _ = shard_tx.send(ShardMsg::Shutdown { done: done_tx }).await;
                dones.push(done_rx);
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_catalog::ShardGeneration;
    use ravel_object_store::list_all;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

    use super::*;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    struct FrozenClock(AtomicI64);

    impl Clock for FrozenClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }

        fn sleep(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            // Never resolves: the test drives flushing explicitly via
            // `flush_all`, not via the actor's age-based tick. A sleep that
            // completes immediately turns the actor's `tokio::select!` loop
            // into a synchronous spin that never yields to the executor.
            Box::pin(std::future::pending())
        }
    }

    fn test_point(tenant: &TenantId, metric: &str, ts_ns: i64, value: f64) -> NormalizedPoint {
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
        NormalizedPoint {
            series_id,
            labels: Arc::new(labels),
            sample: Sample { ts_ns, value },
            is_monotonic_sum: false,
        }
    }

    /// Test debt (formal/tla/TRACEABILITY.md, ingest row 3, `DoAdmit` write
    /// path): an admitted write lands on the shard `shard_for` selects for
    /// the generation's active count at the write's hour, not on the
    /// process's default (generation 0) count -- so a write flushed after a
    /// reshard has activated is durable on the *new* topology's shard, and
    /// that shard is independently confirmed both by the returned commit
    /// token and by the commit record actually found in the store.
    #[tokio::test]
    async fn admitted_write_lands_on_the_shard_the_routed_count_selects() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // Flush open checks the reading against the 2020 plausibility floor
        // (crate::config::MIN_PLAUSIBLE_INGEST_CLOCK_NS), so the test clock
        // must sit in real epoch-hour territory, not small offsets from 0.
        let base_hour = crate::config::MIN_PLAUSIBLE_INGEST_CLOCK_NS / NS_PER_HOUR + 10;
        let t0 = base_hour * NS_PER_HOUR;
        let clock = Arc::new(FrozenClock(AtomicI64::new(t0)));
        let router = IngestRouter::new(
            IngestConfig {
                shard_count: 4,
                ..IngestConfig::default()
            },
            Arc::clone(&store),
            Signal::Metrics,
            clock.clone(),
        );

        let tenant = TenantId::new("acme");
        // A reshard to 8 shards activates at the seed hour itself. Seeding
        // this via `refresh_generations` (never touches the store) and
        // writing at that same instant keeps the cached view fresh (well
        // within the refresh interval `C`), so the write routes on this
        // history directly instead of falling back to the process default.
        let activation_hour = u32::try_from(base_hour).expect("fits u32");
        router.refresh_generations(
            tenant.hash(),
            vec![
                ShardGeneration {
                    generation: 0,
                    shard_count: 4,
                    activation_hour: 0,
                    appended_unix_ns: 0,
                },
                ShardGeneration {
                    generation: 1,
                    shard_count: 8,
                    activation_hour,
                    appended_unix_ns: 0,
                },
            ],
            t0,
        );

        let point = test_point(&tenant, "cpu_usage", t0, 1.0);
        let expected_shard = shard_for(&point.series_id, 8);
        // The process default (4 shards) must route this point to a
        // different shard than the routed (post-reshard, 8-shard) count, or a
        // write that used the wrong count would land on `expected_shard` by
        // coincidence and both assertions below would still pass.
        assert_ne!(
            shard_for(&point.series_id, 4),
            expected_shard,
            "this series id must not land on the same shard under the process \
             default count (4) and the routed count (8), or routing with the \
             wrong count would go undetected"
        );

        // The frozen clock never ages past `max_flush_delay` on its own, so
        // `flush_all` must run concurrently with (not after) the write, and
        // after (not before) the write's own enqueue; joining the two
        // futures interleaves them correctly (crates/ravel-ingest/tests/
        // acks_and_modes.rs uses the same pattern).
        let (write_result, ()) = tokio::join!(
            router.write(
                tenant.clone(),
                vec![point],
                WriteMode::Strict,
                Duration::from_secs(5),
            ),
            router.flush_all(),
        );
        let receipt = write_result.expect("strict write on the routed 8-shard generation succeeds");

        assert_eq!(receipt.tokens.len(), 1, "one shard was involved");
        assert_eq!(
            receipt.tokens[0].shard, expected_shard,
            "the receipt's token names the shard `shard_for` selects for the \
             routed (post-reshard) count, not the process default"
        );

        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        let commit_key = objects
            .iter()
            .find(|o| o.key.contains("/c/"))
            .expect("exactly one commit record")
            .key
            .clone();
        let raw = store
            .get(&commit_key, ravel_object_store::GetRange::Full)
            .await
            .expect("get commit record");
        let decoded = ravel_commit::record::decode(&raw.data).expect("decode commit record");
        assert_eq!(
            decoded.shard, expected_shard,
            "the commit record durably placed in the store names the same \
             routed shard, not a stale generation-0 count"
        );
        assert_eq!(decoded.series_count, 1);
        assert_eq!(decoded.sample_count, 1);
    }
}
