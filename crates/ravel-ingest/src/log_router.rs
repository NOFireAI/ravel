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
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::{CommitToken, TenantHash, shard_for_log};
use tokio::sync::{mpsc, oneshot};

use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};
use crate::clock::Clock;
use crate::config::IngestConfig;
use crate::generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed, load_generations};
use crate::indexed_fields::IndexedFieldsOverlay;
use crate::log_error::LogWriteError;
use crate::log_metrics::LogIngestMetrics;
use crate::log_shard::{LogShardActor, LogShardMsg, est_record_bytes};
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
        let metrics = Arc::new(LogIngestMetrics::default());
        // Production OS-entropy source for writer ids and PUT-retry jitter
        // (ADR-0068 decision 2). The log pipeline is not exercised by the
        // seeded simulation driver, so this router has no injected variant;
        // routing every draw through the seam still keeps `rand::rng()` and
        // `Uuid::new_v4()` off this production path.
        let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
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
        }
        // Routing ends at dispatch: the strict-mode ack wait below is downstream
        // durability (merge/encode/PUT happen in the shard), not a router stage.
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(LogStage::Route, route_start.elapsed());

        if mode == WriteMode::Buffered {
            return Ok(LogWriteReceipt::default());
        }

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
}
