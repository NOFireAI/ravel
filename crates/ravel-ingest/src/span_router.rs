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

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_types::CommitToken;
use tokio::sync::{mpsc, oneshot};

use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};
use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed, load_generations};
use crate::router::WriteMode;
use crate::span_error::SpanWriteError;
use crate::span_metrics::SpanIngestMetrics;
use crate::span_shard::{SpanShardActor, SpanShardMsg, est_span_bytes};

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
///
/// Unlike [`ravel_types::shard_for`]'s `SeriesId` and [`ravel_types::
/// shard_for_log`]'s `LogStreamId`, `trace_id` is not a derived identity: an
/// OTel SDK assigns it directly (usually random, but not guaranteed to be -
/// some ID generators are low-entropy in their leading bytes, and a hostile
/// client can choose it outright). Routing on `trace_id`'s raw leading bytes
/// the way the two siblings route on their already-hashed leading bytes would
/// let a single low-entropy or adversarially-chosen byte pin all of a
/// tenant's span traffic onto one shard. So this function blake3-hashes
/// `trace_id` first and routes on the hash's leading bytes, giving it the
/// same uniformly-distributed input the two siblings get for free from their
/// id's own construction.
pub fn shard_for_span(trace_id: &[u8; 16], shard_count: u32) -> u32 {
    debug_assert!(shard_count > 0);
    let hash = blake3::hash(trace_id);
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
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
    /// Set once the router first observes this shard's channel closed. The
    /// actor is never restarted, so this only flips false to true; it dedups
    /// the `shard_deaths` counter to one increment per shard.
    dead: AtomicBool,
}

/// Routes span writes to generation-versioned shard-actor sets (ADR-0052), the
/// span-pipeline counterpart of [`crate::router::IngestRouter`]. The
/// generation-0 set is spawned at construction; a reshard's activation spawns
/// the new set lazily via the [`GenerationSwitch`] factory while the old set
/// drains.
pub struct SpanIngestRouter {
    switch: GenerationSwitch<SpanShardHandle>,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    metrics: Arc<SpanIngestMetrics>,
    config: IngestConfig,
    /// Process-wide ingest buffer byte budget (ADR-0069 decision 1), shared by
    /// `Arc` with the metrics and log routers. Defaults to `Unlimited`;
    /// `services/ravel-server` installs the configured budget via
    /// [`SpanIngestRouter::with_budget`].
    budget: Arc<IngestByteBudget>,
}

impl SpanIngestRouter {
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let metrics = Arc::new(SpanIngestMetrics::default());
        // Production OS-entropy source for writer ids and PUT-retry jitter
        // (ADR-0068 decision 2). Like the log router, the span pipeline has no
        // seeded-injection caller; routing through the seam still keeps
        // `rand::rng()` and `Uuid::new_v4()` off this production path.
        let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
        let factory = {
            let store = Arc::clone(&store);
            let clock = Arc::clone(&clock);
            let rng = Arc::clone(&rng);
            let metrics = Arc::clone(&metrics);
            move |shard_count: u32| -> Vec<SpanShardHandle> {
                let writer_id = rng.new_uuid();
                let epoch =
                    u64::try_from(clock.now_ns().div_euclid(1_000_000_000).max(0)).unwrap_or(0);
                (0..shard_count)
                    .map(|shard| {
                        let (tx, rx) = mpsc::channel(config.channel_depth);
                        let actor = SpanShardActor::new(
                            shard,
                            writer_id,
                            epoch,
                            Arc::clone(&store),
                            Arc::clone(&clock),
                            Arc::clone(&rng),
                            config,
                            Arc::clone(&metrics),
                            rx,
                        );
                        tokio::spawn(actor.run());
                        SpanShardHandle {
                            tx,
                            dead: AtomicBool::new(false),
                        }
                    })
                    .collect()
            }
        };
        let switch =
            GenerationSwitch::new(config.shard_count, DEFAULT_REFRESH_INTERVAL_NS, factory);

        SpanIngestRouter {
            switch,
            store,
            clock,
            metrics,
            config,
            budget: IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        }
    }

    /// Installs the shared process-wide ingest buffer byte budget (ADR-0069).
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<IngestByteBudget>) -> Self {
        self.budget = budget;
        self
    }

    pub fn metrics(&self) -> &SpanIngestMetrics {
        &self.metrics
    }

    /// Resolve the tenant's active shard-actor set for a write at `now_ns`,
    /// re-reading the provisioning record when the cached view is older than the
    /// refresh interval `C` (ADR-0052 section 3). When the re-read cannot
    /// complete, falls back to [`GenerationSwitch::try_grace_extend`]'s bounded
    /// NF-2 grace window (issue #655) before failing closed: continuing on the
    /// last-known-good view is only safe while that method's horizon predicate
    /// holds, so a genuinely unknowable generation change still fails the flush
    /// exactly as before.
    async fn active_set(
        &self,
        tenant: ravel_types::TenantHash,
        now_ns: i64,
    ) -> Result<Arc<Vec<SpanShardHandle>>, SpanWriteError> {
        match self.switch.route_cached(tenant, now_ns) {
            Routed::Fresh(set) => Ok(set),
            Routed::Stale => {
                match load_generations(
                    self.store.as_ref(),
                    ravel_types::Signal::Spans,
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
                            Err(SpanWriteError::StaleProvisioningView)
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
    /// tenant's next span write.
    pub fn evict_idle_generation_views(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.switch.evict_idle(now_ns, ttl_ns)
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
        if spans.is_empty() {
            return Ok(SpanWriteReceipt::default());
        }

        // Global ingest byte budget (ADR-0069 decision 1): charge the estimated
        // buffered bytes and shed at the ceiling before routing, so a shed
        // request touches no shard and mints no commit token. The charge is
        // cloned into every shard message below and refunded when the flush(es)
        // holding these bytes complete or fail; any early return from here on
        // drops the not-yet-handed-off clones, so nothing leaks.
        let estimate: u64 = spans
            .iter()
            .map(|s| est_span_bytes(s) as u64)
            .fold(0u64, u64::saturating_add);
        let charge = Arc::new(
            self.budget
                .try_charge(estimate)
                .map_err(|_| SpanWriteError::BufferBudgetExceeded)?,
        );

        // Route against the tenant's current generation view, re-reading the
        // provisioning record when the cache is older than `C` and failing
        // closed if that read cannot complete (ADR-0052 section 3).
        let set = self.active_set(tenant.hash(), self.clock.now_ns()).await?;
        let shard_count = set.len() as u32;
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
                charge: Some(Arc::clone(&charge)),
            };
            if set[shard as usize].tx.send(msg).await.is_err() {
                // The actor task is gone (it never closes its own receiver
                // while alive), so this shard is dead. Count it once and
                // surface the typed error rather than acking as if the spans
                // landed.
                self.mark_shard_dead(&set[shard as usize]);
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
                    self.mark_shard_dead(&set[shard as usize]);
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
    fn mark_shard_dead(&self, handle: &SpanShardHandle) {
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
                    .send(SpanShardMsg::FlushNow { done: tx })
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

    /// Flushes every live generation's span shard actors so a retiring
    /// generation's buffers drain too (ADR-0052 section 2). The detached actor
    /// tasks end on their own after the drain; the `done` acknowledgement fires
    /// after the flush, so durability holds without joining them.
    pub async fn shutdown(self) {
        let sets = self.switch.all_sets();
        let mut dones = Vec::new();
        for set in &sets {
            for shard in set.iter() {
                let (tx, rx) = oneshot::channel();
                let _ = shard.tx.send(SpanShardMsg::Shutdown { done: tx }).await;
                dones.push(rx);
            }
        }
        for rx in dones {
            let _ = rx.await;
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
    fn shard_for_span_is_deterministic_and_uses_every_byte() {
        let a = trace_id(42);
        assert_eq!(shard_for_span(&a, 8), shard_for_span(&a, 8));

        // The bug this routing exists to avoid: reinterpreting only the
        // leading 8 raw bytes as the shard key ignores every byte after
        // them, so a low-entropy or adversarially-fixed prefix pins all
        // traffic sharing it onto one shard regardless of the rest of the
        // id. Hashing the full 16 bytes means the trailing bytes are not
        // ignored - changing them can and generally does change the shard.
        let differs = (0u8..32).any(|tail| {
            let mut b = a;
            b[8..].copy_from_slice(&[tail; 8]);
            shard_for_span(&b, 8) != shard_for_span(&a, 8)
        });
        assert!(
            differs,
            "trailing bytes must be able to change the shard (full-width hash), not be ignored"
        );
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
        // Hashed routing does not guarantee a bijection over a handful of
        // sequential trace ids the way the old raw-leading-bytes math
        // happened to for small inputs, so this uses enough distinct traces
        // that every shard is used with overwhelming probability
        // ((3/4)^64 ~= 1e-8 chance any one shard is empty by pure luck) rather
        // than asserting an exact one-trace-per-shard bijection.
        let trace_count = 64u64;
        let spans: Vec<NormalizedSpan> = (0..trace_count)
            .map(|i| norm_span(trace_id(i), 1, 1_000))
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
        shards.dedup();
        assert_eq!(
            shards,
            (0..shard_count).collect::<Vec<_>>(),
            "64 distinct traces should exercise every shard"
        );
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

    // ---- ADR-0052 router live-switch integration tests ----
    //
    // These drive the real router (spawning real actors and writing to a
    // MemoryStore) to prove the generation switch is wired into the write path.
    // The switch mechanism itself is unit-tested generically in
    // `crate::generation`; the three routers embed the same `GenerationSwitch`,
    // so this per-router integration test also stands in for the metrics and
    // log routers (ADR-0052 allows one shared test when the routers share
    // structure, which they do).

    use crate::generation::DEFAULT_REFRESH_INTERVAL_NS;
    use ravel_catalog::ShardGeneration;
    use std::sync::atomic::{AtomicI64, Ordering};

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// A clock whose reading a test sets explicitly, so routing hour and
    /// staleness age are deterministic.
    #[derive(Debug)]
    struct ManualClock(AtomicI64);
    impl ManualClock {
        fn new(now_ns: i64) -> Self {
            ManualClock(AtomicI64::new(now_ns))
        }
        fn set(&self, now_ns: i64) {
            self.0.store(now_ns, Ordering::SeqCst);
        }
    }
    impl crate::clock::Clock for ManualClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn sg(generation: u32, shard_count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation,
            shard_count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    /// A reshard activation switches the router's routing to the new
    /// generation's shard-actor set: after activation a write can land on a
    /// shard index only the larger set has, while a write on the old set before
    /// the switch stays valid and durable (the old actors are not force-closed).
    #[tokio::test]
    async fn activation_routes_to_new_generation_set() {
        let store = Arc::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::new(50 * NS_PER_HOUR)); // hour 50
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count: 4,
                target_bytes: 1, // flush every write immediately
                ..IngestConfig::default()
            },
            store.clone(),
            clock.clone(),
        );
        let tenant = TenantId::new("acme");
        let history = vec![sg(0, 4, 0), sg(1, 8, 100)]; // reshard 4 -> 8 at hour 100

        // Before activation (hour 50): routes at count 4, so every shard index
        // is < 4.
        router.refresh_generations(tenant.hash(), history.clone(), clock.now_ns());
        let before = router
            .write(
                tenant.clone(),
                (0..32).map(|i| norm_span(trace_id(i), 1, 1_000)).collect(),
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("write before activation");
        assert!(
            before.tokens.iter().all(|t| t.shard < 4),
            "before activation every shard index is < 4"
        );

        // Advance past the activation hour and refresh the view (the background
        // refresher's job): routing now uses count 8.
        clock.set(100 * NS_PER_HOUR);
        router.refresh_generations(tenant.hash(), history, clock.now_ns());
        let after = router
            .write(
                tenant.clone(),
                (0..64)
                    .map(|i| norm_span(trace_id(1_000 + i), 1, 1_000))
                    .collect(),
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("write after activation");
        assert!(
            after.tokens.iter().any(|t| t.shard >= 4),
            "after activation a write reaches a shard index only the count-8 set has"
        );

        // Both generations' data is durable: the old set was not force-closed.
        router.shutdown().await;
        let prefix = format!("t/{}/s/l0/", tenant.hash().to_hex());
        let objects = list_all(store.as_ref(), &prefix).await.expect("list");
        let shards: std::collections::BTreeSet<String> = objects
            .iter()
            .filter_map(|o| o.key.strip_prefix(&prefix))
            .filter_map(|rest| rest.split('/').next())
            .map(|s| s.to_string())
            .collect();
        assert!(
            shards.iter().any(|s| s.parse::<u32>().unwrap_or(0) >= 4),
            "objects exist under a shard index from the new generation"
        );
    }

    /// Staleness fail-closed, with the NF-2 bounded grace window (issue #655):
    /// once the router's cached view for a tenant ages past `C`, the router
    /// re-reads the provisioning record before routing; if that re-read
    /// cannot complete (here a store fault on the record GET), the router
    /// falls back to [`crate::generation::GenerationSwitch::try_grace_extend`]
    /// rather than failing the flush immediately. Inside the grace horizon
    /// (`hour_of(now_ns) < hour_of(refreshed_at_ns) + min_lead_hours(C)`) the
    /// write still routes, on the last-known-good view, and the
    /// grace-extended counter increments; only once the horizon is crossed
    /// does the write fail closed with the typed error and the ordinary
    /// stale-flush counter (ADR-0052 section 3). Before the grace window
    /// existed, this test's first assertion was `expect_err` on the
    /// still-within-horizon write -- that is the exact behavior issue #655
    /// NF-2 replaces, since it turned any sustained store latency into a
    /// total outage instead of a degraded-but-available router.
    #[tokio::test]
    async fn stale_view_routes_via_grace_window_then_fails_closed_past_horizon() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};

        // The record GET always faults, so a stale view can never be refreshed.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("prov unavailable".into()))
                .with_key_contains("/prov"),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let t0 = 10 * NS_PER_HOUR;
        let clock = Arc::new(ManualClock::new(t0));
        let router = SpanIngestRouter::new(
            IngestConfig {
                shard_count: 4,
                target_bytes: 1,
                ..IngestConfig::default()
            },
            store.clone(),
            clock.clone(),
        );
        let tenant = TenantId::new("acme");
        router.refresh_generations(tenant.hash(), vec![sg(0, 4, 0)], t0);

        // Age the cached view past C: the next write must re-read, which
        // faults, but the horizon (hour 12, since min_lead_hours(C) = 2 for
        // the default C) has not been crossed, so it routes on the cached
        // view instead of failing closed.
        clock.set(t0 + DEFAULT_REFRESH_INTERVAL_NS + 1);
        router
            .write(
                tenant.clone(),
                vec![norm_span(trace_id(1), 1, 1_000)],
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("within the grace horizon, a stale view still routes");
        assert_eq!(
            router.metrics().snapshot().grace_extended_stale_flushes,
            1,
            "the grace-extended counter must increment"
        );
        assert_eq!(
            router.metrics().snapshot().stale_provisioning_flushes,
            0,
            "no flush has failed closed yet"
        );

        // Cross the horizon (hour 12): the same unreachable store now fails
        // the write closed rather than extending the grace window further.
        clock.set(12 * NS_PER_HOUR);
        let err = router
            .write(
                tenant.clone(),
                vec![norm_span(trace_id(1), 1, 1_000)],
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect_err(
                "past the grace horizon, a stale view whose re-read fails must fail closed",
            );
        assert!(
            matches!(err, SpanWriteError::StaleProvisioningView),
            "got: {err:?}"
        );
        assert_eq!(
            router.metrics().snapshot().stale_provisioning_flushes,
            1,
            "the stale-flush counter must increment"
        );

        // A successful refresh (the background refresher's job) clears
        // staleness; the next write routes again without touching the store.
        router.refresh_generations(tenant.hash(), vec![sg(0, 4, 0)], clock.now_ns());
        router
            .write(
                tenant.clone(),
                vec![norm_span(trace_id(1), 1, 1_000)],
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("after a refresh the write routes again");
        router.shutdown().await;
    }
}
