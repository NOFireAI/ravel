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

use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::{CommitToken, TenantHash, shard_for_log};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::log_error::LogWriteError;
use crate::log_metrics::LogIngestMetrics;
use crate::log_shard::{LogShardActor, LogShardMsg};
use crate::router::WriteMode;

/// Resolves the POSTINGS indexed-field list for a tenant at flush time
/// (ADR-0049 decision 3, issue #511). The shard actor calls this once per
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
/// issue #511.
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
    task: JoinHandle<()>,
    /// Set once the router first observes this shard's channel closed. The
    /// actor is never restarted, so this only flips false to true; it dedups
    /// the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
}

/// Owns `shard_count` log shard actor tasks and routes writes to them by
/// `shard_for_log(stream_id, shard_count)`.
///
/// Exactly one `tokio::spawn` per shard happens here, in [`Self::new`]; no
/// code path spawns a task per message or per record, so task count is fixed
/// at construction and independent of write volume.
pub struct LogIngestRouter {
    shards: Vec<LogShardHandle>,
    metrics: Arc<LogIngestMetrics>,
    config: IngestConfig,
}

impl LogIngestRouter {
    /// Builds a router whose shards index no POSTINGS field
    /// ([`NoIndexedFields`]). Use [`Self::new_with_indexed_fields`] to wire
    /// per-tenant configuration (issue #511).
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::new_with_indexed_fields(config, store, clock, Arc::new(NoIndexedFields))
    }

    /// Like [`Self::new`], but every shard resolves each tenant's POSTINGS
    /// indexed-field list through `indexed_fields` at flush time (ADR-0049
    /// decision 3, issue #511). This is the production constructor; the server
    /// passes its per-tenant `IndexedFieldConfig` here.
    pub fn new_with_indexed_fields(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        indexed_fields: Arc<dyn LogIndexedFields>,
    ) -> Self {
        let metrics = Arc::new(LogIngestMetrics::default());
        let writer_id = Uuid::new_v4();
        let epoch = u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);

        let shards = (0..config.shard_count)
            .map(|shard| {
                let (tx, rx) = mpsc::channel(config.channel_depth);
                let actor = LogShardActor::new(
                    shard,
                    writer_id,
                    epoch,
                    Arc::clone(&store),
                    Arc::clone(&clock),
                    config,
                    Arc::clone(&metrics),
                    rx,
                    Arc::clone(&indexed_fields),
                );
                let task = tokio::spawn(actor.run());
                LogShardHandle {
                    tx,
                    task,
                    dead: AtomicBool::new(false),
                }
            })
            .collect();

        LogIngestRouter {
            shards,
            metrics,
            config,
        }
    }

    pub fn metrics(&self) -> &LogIngestMetrics {
        &self.metrics
    }

    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
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
        let shard_count = self.config.shard_count;
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
            };
            if self.shards[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the records
                // landed.
                self.mark_shard_dead(shard);
                return Err(LogWriteError::ShardUnavailable);
            }
        }

        if mode == WriteMode::Buffered {
            return Ok(LogWriteReceipt::default());
        }

        // `join_all` preserves input order, so `joined[i]` is `ack_shards[i]`.
        let joined = tokio::time::timeout(ack_deadline, futures::future::join_all(ack_rxs))
            .await
            .map_err(|_| LogWriteError::AckTimeout)?;
        let mut tokens = Vec::with_capacity(joined.len());
        for (shard, result) in ack_shards.into_iter().zip(joined) {
            // A `RecvError` here means the actor dropped the ack sender without
            // sending: it panicked mid-flush (a healthy actor always acks, even
            // on abandonment). Count the death and report it as unavailable.
            let inner = match result {
                Ok(inner) => inner,
                Err(_) => {
                    self.mark_shard_dead(shard);
                    return Err(LogWriteError::ShardUnavailable);
                }
            };
            tokens.push(inner?);
        }
        Ok(LogWriteReceipt { tokens })
    }

    /// Records the first observation of a shard actor's death, deduped so a
    /// permanently dead shard is counted once no matter how many later writes
    /// route to it.
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
            if shard
                .tx
                .send(LogShardMsg::FlushNow { done: tx })
                .await
                .is_ok()
            {
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
            let _ = shard.tx.send(LogShardMsg::Shutdown { done: tx }).await;
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
