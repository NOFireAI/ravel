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
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::error::WriteError;
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
    task: JoinHandle<()>,
    /// Set once the router first observes this shard's channel closed (send
    /// half or a strict-mode ack failing because the actor task is gone).
    /// The actor is never restarted, so this only ever flips false to true;
    /// it dedups the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
}

/// Owns `shard_count` shard actor tasks and routes writes to them by
/// `shard_for(series_id, shard_count)`.
///
/// Exactly one `tokio::spawn` per shard happens here, in `new`; no code path
/// spawns a task per message or per point, so task count is fixed at
/// construction and independent of write volume.
pub struct IngestRouter {
    shards: Vec<ShardHandle>,
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
        let writer_id = Uuid::new_v4();
        let epoch = u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);

        let shards = (0..config.shard_count)
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
                let task = tokio::spawn(actor.run());
                ShardHandle {
                    tx,
                    task,
                    dead: AtomicBool::new(false),
                }
            })
            .collect();

        IngestRouter {
            shards,
            metrics,
            config,
        }
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.metrics
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
        let shard_count = self.config.shard_count;
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
        if by_shard.is_empty() && exemplars_by_shard.is_empty() {
            return Ok(WriteReceipt::default());
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
            let ack = match mode {
                WriteMode::Strict => {
                    let (tx, rx) = oneshot::channel();
                    ack_shards.push(shard);
                    ack_rxs.push(rx);
                    Some(tx)
                }
                WriteMode::Buffered => None,
            };
            let msg = ShardMsg::Write {
                tenant: tenant.clone(),
                points,
                exemplars: exemplars_by_shard.remove(&shard).unwrap_or_default(),
                ack,
            };
            if self.shards[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the points
                // landed (a8-F03).
                self.mark_shard_dead(shard);
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
                    self.mark_shard_dead(shard);
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
    fn mark_shard_dead(&self, shard: u32) {
        if !self.shards[shard as usize]
            .dead
            .swap(true, Ordering::Relaxed)
        {
            self.metrics.record_shard_death();
        }
    }

    /// Forces every shard to flush all buffered tenants now, for tests and
    /// graceful shutdown paths that need durability without waiting on
    /// `max_flush_delay`.
    pub async fn flush_all(&self) {
        let mut dones = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let (tx, rx) = oneshot::channel();
            if shard.tx.send(ShardMsg::FlushNow { done: tx }).await.is_ok() {
                dones.push(rx);
            }
        }
        for rx in dones {
            let _ = rx.await;
        }
    }

    /// Flushes every shard's buffered tenants, then stops and joins every
    /// shard actor task.
    pub async fn shutdown(self) {
        let mut dones = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let (tx, rx) = oneshot::channel();
            let _ = shard.tx.send(ShardMsg::Shutdown { done: tx }).await;
            dones.push(rx);
        }
        for rx in dones {
            let _ = rx.await;
        }
        for shard in self.shards {
            let _ = shard.task.await;
        }
    }
}
