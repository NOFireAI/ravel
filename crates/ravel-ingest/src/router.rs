//! Owns the shard actors and fans writes out to them
//! (docs/ingest.md "Structure").

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedPoint;
use ravel_types::{CommitToken, Signal, TenantId, shard_for};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::error::WriteError;
use crate::generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed, load_generations};
use crate::metrics::IngestMetrics;
use crate::shard::{ShardActor, ShardMsg};
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

struct ShardHandle {
    tx: mpsc::Sender<ShardMsg>,
    /// Set once the router first observes this shard's channel closed (send
    /// half or a strict-mode ack failing because the actor task is gone).
    /// The actor is never restarted, so this only ever flips false to true;
    /// it dedups the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
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
    metrics: Arc<IngestMetrics>,
    config: IngestConfig,
}

impl IngestRouter {
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        signal: Signal,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let metrics = Arc::new(IngestMetrics::default());
        // Each generation's shard-actor set gets a fresh writer identity, so
        // two sets never collide on a commit key for the same shard index.
        let factory = {
            let store = Arc::clone(&store);
            let clock = Arc::clone(&clock);
            let metrics = Arc::clone(&metrics);
            move |shard_count: u32| -> Vec<ShardHandle> {
                let writer_id = Uuid::new_v4();
                let epoch =
                    u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
                (0..shard_count)
                    .map(|shard| {
                        let (tx, rx) = mpsc::channel(config.channel_depth);
                        let actor = ShardActor::new(
                            shard,
                            signal,
                            writer_id,
                            epoch,
                            Arc::clone(&store),
                            Arc::clone(&clock),
                            config,
                            Arc::clone(&metrics),
                            rx,
                        );
                        tokio::spawn(actor.run());
                        ShardHandle {
                            tx,
                            dead: AtomicBool::new(false),
                        }
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
            metrics,
            config,
        }
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.metrics
    }

    /// Resolve the tenant's active shard-actor set for a write at `now_ns`,
    /// re-reading the provisioning record when the cached view is older than the
    /// refresh interval `C` (ADR-0052 section 3). Fails closed on an
    /// unreadable/untrusted record rather than routing on a stale view.
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
                    Err(_) => {
                        self.metrics.record_stale_provisioning_flush();
                        Err(WriteError::StaleProvisioningView)
                    }
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
    /// shape (scalar or histogram) rather than OTLP's `NormalizedPoint`
    /// (docs/rseg-v3-plan.md section 7). Both entry points reach the same
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
        // Route this write against the tenant's current generation view,
        // re-reading the provisioning record when the cache is older than `C`
        // and failing closed if that read cannot complete (ADR-0052 section 3).
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

        // Parallel to `ack_rxs`: the shard each receiver belongs to, so a
        // closed ack channel can be attributed to the right shard and counted
        // as that shard's death.
        let mut ack_shards = Vec::with_capacity(shard_ids.len());
        let mut ack_rxs = Vec::with_capacity(shard_ids.len());
        for shard in shard_ids {
            let points = by_shard.remove(&shard).unwrap_or_default();
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
                    ack_shards.push(shard);
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
            };
            if set[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the points
                // landed (a8-F03).
                self.mark_shard_dead(&set[shard as usize]);
                return Err(WriteError::ShardUnavailable);
            }
        }

        if mode == WriteMode::Buffered {
            return Ok(WriteReceipt::default());
        }

        // `join_all` preserves input order, so `joined[i]` is `ack_shards[i]`.
        let joined = tokio::time::timeout(ack_deadline, futures::future::join_all(ack_rxs))
            .await
            .map_err(|_| WriteError::AckTimeout)?;
        let mut tokens = Vec::with_capacity(joined.len());
        for (shard, result) in ack_shards.into_iter().zip(joined) {
            // A `RecvError` here means the actor dropped the ack sender without
            // sending: it panicked mid-flush (a healthy actor always acks, even
            // on abandonment). Count the death and report it as unavailable.
            let inner = match result {
                Ok(inner) => inner,
                Err(_) => {
                    self.mark_shard_dead(&set[shard as usize]);
                    return Err(WriteError::ShardUnavailable);
                }
            };
            tokens.push(inner?);
        }
        Ok(WriteReceipt { tokens })
    }

    /// Records the first observation of a shard actor's death, deduped so a
    /// permanently dead shard is counted once no matter how many later writes
    /// route to it (docs/ingest.md "Metrics (self-observability)", a8-F03).
    fn mark_shard_dead(&self, handle: &ShardHandle) {
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
                if shard.tx.send(ShardMsg::FlushNow { done: tx }).await.is_ok() {
                    dones.push(rx);
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
                let (tx, rx) = oneshot::channel();
                let _ = shard.tx.send(ShardMsg::Shutdown { done: tx }).await;
                dones.push(rx);
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }
}
