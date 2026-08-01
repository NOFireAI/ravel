//! Owns the span shard actors and fans writes out to them, the span-pipeline
//! counterpart of [`crate::log_router`] (docs/ingest.md "Structure",
//! ADR-0041).
//!
//! Like [`crate::log_router::LogIngestRouter`] and unlike
//! [`crate::router::IngestRouter`], this router bakes in its signal
//! ([`ravel_types::Signal::Spans`]): it has exactly one caller shape, so an
//! unused parameter would only invite a wrong value.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_types::CommitToken;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::router::WriteMode;
use crate::span_error::SpanWriteError;
use crate::span_metrics::SpanIngestMetrics;
use crate::span_shard::{SpanShardActor, SpanShardMsg};

/// Routing v1 for spans (persistent contract, ADR-0041 decision 2): shard from
/// the trace id's leading bytes, in exactly the style
/// [`ravel_types::shard_for`] and [`ravel_types::shard_for_log`] use, just
/// keyed on `trace_id` instead of a derived identity. Keeping one trace's spans
/// confined to one shard is what makes trace-by-id assembly a bounded scan of
/// one shard rather than a fan-out across every shard.
///
/// Routing is tenant-scoped because each shard buffers per `TenantId` upstream
/// of this function, not because of anything in the id: a `trace_id` is chosen
/// by the sender and carries no tenant, exactly as a `LogStreamId` does not.
///
/// This function belongs beside its two siblings in `ravel-types`, and should
/// move there when a task's scope includes that crate. It lives here because
/// `crates/ravel-types` was outside this change's allowed scope; the placement
/// is the only thing provisional about it. The routing *rule* is frozen: it
/// determines which shard's object keys a trace's spans land under, so changing
/// it later is a format-change ADR event (ADR-0041 "Consequences").
pub fn shard_for_span(trace_id: &[u8; 16], shard_count: u32) -> u32 {
    debug_assert!(shard_count > 0);
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&trace_id[..8]);
    (u64::from_le_bytes(prefix) % u64::from(shard_count.max(1))) as u32
}

/// One token per shard the request's spans flushed through. Empty in buffered
/// mode, or if the request carried no spans.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpanWriteReceipt {
    pub tokens: Vec<CommitToken>,
}

/// A duplicate of [`crate::log_router`]'s private `LogShardHandle`: three
/// fields, so duplicating is cheaper than making one module's struct
/// `pub(crate)` across an unrelated boundary for one shared shape.
struct SpanShardHandle {
    tx: mpsc::Sender<SpanShardMsg>,
    task: JoinHandle<()>,
    /// Set once the router first observes this shard's channel closed. The
    /// actor is never restarted, so this only flips false to true; it dedups
    /// the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
}

/// Owns `shard_count` span shard actor tasks and routes writes to them by
/// [`shard_for_span`].
///
/// Exactly one `tokio::spawn` per shard happens here, in [`Self::new`]; no code
/// path spawns a task per message or per span, so task count is fixed at
/// construction and independent of write volume.
pub struct SpanIngestRouter {
    shards: Vec<SpanShardHandle>,
    metrics: Arc<SpanIngestMetrics>,
    config: IngestConfig,
}

impl SpanIngestRouter {
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let metrics = Arc::new(SpanIngestMetrics::default());
        let writer_id = Uuid::new_v4();
        let epoch = u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);

        let shards = (0..config.shard_count)
            .map(|shard| {
                let (tx, rx) = mpsc::channel(config.channel_depth);
                let actor = SpanShardActor::new(
                    shard,
                    writer_id,
                    epoch,
                    Arc::clone(&store),
                    Arc::clone(&clock),
                    config,
                    Arc::clone(&metrics),
                    rx,
                );
                let task = tokio::spawn(actor.run());
                SpanShardHandle {
                    tx,
                    task,
                    dead: AtomicBool::new(false),
                }
            })
            .collect();

        SpanIngestRouter {
            shards,
            metrics,
            config,
        }
    }

    pub fn metrics(&self) -> &SpanIngestMetrics {
        &self.metrics
    }

    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
    }

    /// Groups `spans` by [`shard_for_span`], sends one `SpanShardMsg::Write`
    /// per involved shard, and (in strict mode) awaits every involved shard's
    /// ack within `ack_deadline`. Sending blocks on a full channel: that
    /// backpressure is intentional (docs/ingest.md "Channel").
    pub async fn write(
        &self,
        tenant: ravel_types::TenantId,
        spans: Vec<NormalizedSpan>,
        mode: WriteMode,
        ack_deadline: Duration,
    ) -> Result<SpanWriteReceipt, SpanWriteError> {
        let shard_count = self.config.shard_count;
        let mut by_shard: HashMap<u32, Vec<NormalizedSpan>> = HashMap::new();
        for span in spans {
            let shard = shard_for_span(&span.trace_id, shard_count);
            by_shard.entry(shard).or_default().push(span);
        }
        if by_shard.is_empty() {
            return Ok(SpanWriteReceipt::default());
        }

        let mut shard_ids: Vec<u32> = by_shard.keys().copied().collect();
        shard_ids.sort_unstable();

        // Parallel to `ack_rxs`: the shard each receiver belongs to, so a
        // closed ack channel is attributed to the right shard and counted as
        // that shard's death.
        let mut ack_shards = Vec::with_capacity(shard_ids.len());
        let mut ack_rxs = Vec::with_capacity(shard_ids.len());
        for shard in shard_ids {
            let spans = by_shard.remove(&shard).unwrap_or_default();
            let ack = match mode {
                WriteMode::Strict => {
                    let (tx, rx) = oneshot::channel();
                    ack_shards.push(shard);
                    ack_rxs.push(rx);
                    Some(tx)
                }
                WriteMode::Buffered => None,
            };
            let msg = SpanShardMsg::Write {
                tenant: tenant.clone(),
                spans,
                ack,
            };
            if self.shards[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the spans
                // landed.
                self.mark_shard_dead(shard);
                return Err(SpanWriteError::ShardUnavailable);
            }
        }

        if mode == WriteMode::Buffered {
            return Ok(SpanWriteReceipt::default());
        }

        // `join_all` preserves input order, so `joined[i]` is `ack_shards[i]`.
        let joined = tokio::time::timeout(ack_deadline, futures::future::join_all(ack_rxs))
            .await
            .map_err(|_| SpanWriteError::AckTimeout)?;
        let mut tokens = Vec::with_capacity(joined.len());
        for (shard, result) in ack_shards.into_iter().zip(joined) {
            // A `RecvError` here means the actor dropped the ack sender without
            // sending: it panicked mid-flush (a healthy actor always acks, even
            // on abandonment). Count the death and report it as unavailable.
            let inner = match result {
                Ok(inner) => inner,
                Err(_) => {
                    self.mark_shard_dead(shard);
                    return Err(SpanWriteError::ShardUnavailable);
                }
            };
            tokens.push(inner?);
        }
        Ok(SpanWriteReceipt { tokens })
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
                .send(SpanShardMsg::FlushNow { done: tx })
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
            let _ = shard.tx.send(SpanShardMsg::Shutdown { done: tx }).await;
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, StatusCode};
    use ravel_types::TenantId;

    use super::*;
    use crate::clock::SystemClock;

    fn norm_span(trace_id: [u8; 16], span: u8, start_ns: i64) -> NormalizedSpan {
        NormalizedSpan {
            trace_id,
            span_id: [span; 8],
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: start_ns,
            end_ts_ns: start_ns + 10,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        }
    }

    fn trace_id(lead: u64) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&lead.to_le_bytes());
        id
    }

    #[test]
    fn shard_for_span_matches_leading_bytes_mod_shard_count() {
        let id = trace_id(0x0102_0304_0506_0708);
        let expected = (0x0102_0304_0506_0708u64 % 4) as u32;
        assert_eq!(shard_for_span(&id, 4), expected);
    }

    #[test]
    fn shard_for_span_is_deterministic_and_ignores_the_trailing_bytes() {
        let mut a = trace_id(42);
        let mut b = a;
        a[8..].copy_from_slice(&[1u8; 8]);
        b[8..].copy_from_slice(&[2u8; 8]);
        assert_eq!(shard_for_span(&a, 8), shard_for_span(&b, 8));
        assert_eq!(shard_for_span(&a, 8), shard_for_span(&a, 8));
    }

    #[test]
    fn shard_for_span_never_divides_by_zero() {
        // The debug_assert documents the contract; a release build with a zero
        // shard count must still not panic, exactly like shard_for_log.
        assert_eq!(shard_for_span(&trace_id(7), 1), 0);
    }

    /// Every span of one trace routes to one shard: the property ADR-0041's
    /// routing decision exists to guarantee. Proven on the durable objects,
    /// not just on the routing function: with 4 shards and one trace, exactly
    /// one shard directory holds an object.
    #[tokio::test]
    async fn one_trace_lands_in_exactly_one_shard() {
        let store = Arc::new(MemoryStore::new());
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count: 4,
                target_bytes: 1,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        );
        let tenant = TenantId::new("acme");
        let id = trace_id(0xdead_beef);
        let spans = (0..8).map(|i| norm_span(id, i, 1_000 + i as i64)).collect();

        let receipt = router
            .write(
                tenant.clone(),
                spans,
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("strict write commits");
        assert_eq!(receipt.tokens.len(), 1, "one trace acks from one shard");
        assert_eq!(receipt.tokens[0].shard, shard_for_span(&id, 4));

        let prefix = format!("t/{}/s/l0/", tenant.hash().to_hex());
        let objects = list_all(store.as_ref(), &prefix).await.expect("list");
        assert_eq!(objects.len(), 1, "all 8 spans are in one object");

        let bytes = store
            .get(&objects[0].key, GetRange::Full)
            .await
            .expect("get")
            .data;
        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("open");
        let (spans, _stats) = reader
            .scan(&SpanQuery::trace(id, i64::MIN, i64::MAX))
            .expect("scan");
        assert_eq!(spans.len(), 8);

        router.shutdown().await;
    }

    #[tokio::test]
    async fn spans_of_different_traces_fan_out_to_their_own_shards() {
        let store = Arc::new(MemoryStore::new());
        let shard_count = 4;
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count,
                target_bytes: 1,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        );
        // Leading values 0..4 land on shards 0..4 under the mod rule.
        let spans: Vec<NormalizedSpan> = (0..shard_count)
            .map(|i| norm_span(trace_id(u64::from(i)), 1, 1_000))
            .collect();
        let receipt = router
            .write(
                TenantId::new("acme"),
                spans,
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("strict write commits");

        let mut shards: Vec<u32> = receipt.tokens.iter().map(|t| t.shard).collect();
        shards.sort_unstable();
        assert_eq!(shards, (0..shard_count).collect::<Vec<_>>());
        router.shutdown().await;
    }

    #[tokio::test]
    async fn an_empty_write_acks_with_no_tokens() {
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count: 2,
                ..IngestConfig::default()
            },
            Arc::new(MemoryStore::new()),
            Arc::new(SystemClock),
        );
        let receipt = router
            .write(
                TenantId::new("acme"),
                Vec::new(),
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("an empty write never fails");
        assert!(receipt.tokens.is_empty());
        router.shutdown().await;
    }

    #[tokio::test]
    async fn buffered_mode_returns_no_tokens_and_shutdown_still_makes_it_durable() {
        let store = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count: 2,
                target_bytes: 8 * 1024 * 1024,
                max_flush_delay: Duration::from_secs(3600),
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        );
        let receipt = router
            .write(
                tenant.clone(),
                vec![norm_span(trace_id(1), 1, 1_000)],
                WriteMode::Buffered,
                Duration::from_secs(10),
            )
            .await
            .expect("buffered write never blocks past enqueue");
        assert!(receipt.tokens.is_empty());

        router.shutdown().await;

        let prefix = format!("t/{}/s/l0/", tenant.hash().to_hex());
        let objects = list_all(store.as_ref(), &prefix).await.expect("list");
        assert_eq!(objects.len(), 1, "the shutdown drain flushed the buffer");
    }
}
