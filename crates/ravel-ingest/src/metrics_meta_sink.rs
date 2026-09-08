//! The one-per-process metric metadata sink (ADR-0085 decision 1, write side).
//!
//! Every ingest surface (OTLP, OTAP, RW1, RW2) decodes a metric's
//! `(family_name, type, help, unit)` tuple and hands it to [`MetadataSink`]
//! through [`MetadataSink::observe`]. This crate owns the sink because it
//! already owns the object-store client and the per-process ingest pipeline all
//! four surfaces funnel through; `ravel-server` constructs exactly one and
//! spawns its flush loop next to the lifecycle refresh loop. One sink per
//! process, not one per protocol, so a single process never races itself on
//! `t/<tenant_hash>/m/meta`.
//!
//! # The two halves, and why they are split
//!
//! [`observe`](MetadataSink::observe) runs on the ingest request path. It is
//! synchronous, does no I/O, awaits nothing, and cannot fail: it fingerprints
//! each tuple, compares the fingerprint against the last-flushed one, and
//! pushes only new-or-changed tuples into a per-tenant pending set under a
//! short mutex hold. Nothing an ingest client sends can make a request fail,
//! slow down, or block on the metadata record.
//!
//! [`flush_once`](MetadataSink::flush_once) runs on the background flush loop,
//! once per debounce window ([`MetadataSinkConfig::window`], default 30 s). Per
//! tenant with a non-empty pending set it does exactly **one GET** of the
//! record and **at most one CAS PUT**, whatever number of names arrived. The
//! merge is [`ravel_catalog::merge_entries`] (field-wise, so RW1
//! supplying help without unit and OTLP later supplying unit without help
//! compose instead of clobbering); a merge that changes nothing skips the PUT
//! and only updates the local fingerprints, which is the common case right
//! after a restart when every name looks new locally but the durable record
//! already reflects it.
//!
//! # The local fingerprint map is a skip, never a source of truth
//!
//! The map holds a hash of the last-flushed `(kind, help, unit)` per (tenant,
//! family), keyed on content rather than on name alone, so a help or unit
//! change mid-process is detected locally instead of being masked by "we have
//! seen this name before". It is never trusted on its own: the durable record
//! is always read and merged before any write, so a fresh process with an empty
//! map converges to zero PUTs rather than rewriting what is already stored.
//!
//! Both maps are bounded, not just the durable record: a family name that
//! would grow one tenant's tracked set past `MetadataSinkConfig::entry_cap`
//! (the same cap the durable record enforces) is never fingerprinted or
//! queued, and a tenant beyond `MetadataSinkConfig::max_tracked_tenants` has
//! its least-recently-touched, no-pending-work sibling evicted to make room.
//! A hostile client minting unbounded distinct family names or tenant
//! identities therefore cannot grow this process's memory without bound; the
//! cost is at most one extra GET per window for the tenant whose local state
//! was capped or evicted, never lost data (the durable record is unaffected,
//! and it enforces its own cap independently via `merge_entries`).
//!
//! # Failure handling
//!
//! Nothing here propagates an error to a caller. A CAS conflict re-reads,
//! re-merges onto the winner's body, and retries with jittered backoff up to
//! [`MetadataSinkConfig::max_cas_retries`]; on exhaustion the window's update is
//! dropped, `ingest_metadata_flush_dropped_total` is incremented and a warning
//! logged. Nothing is re-queued: the next `observe` of a still-changed tuple
//! re-arms it naturally, because its fingerprint was never committed. Any other
//! store or record error is treated the same way. A contended or degraded
//! metadata key can therefore never become an ingest availability regression.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_catalog::{
    DEFAULT_METRICS_META_ENTRY_CAP, MetricKind as CatalogMetricKind, MetricMetadataEntry,
    MetricsMetaError, merge_entries, read_metrics_meta, write_metrics_meta,
};
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::metadata::{MetricKind as WireMetricKind, MetricMetadata};
use ravel_types::{TenantHash, TenantId};
use tokio::sync::oneshot;

use crate::clock::{Clock, SystemClock};
use crate::metrics::IngestMetrics;

/// Default debounce window: 30 s (ADR-0085 decision 1). One GET and at most one
/// PUT per tenant per window, however many names arrived in it.
pub const DEFAULT_METADATA_FLUSH_WINDOW: Duration = Duration::from_secs(30);

/// Default bounded CAS retry count (ADR-0085 decision 1). Exceeding it drops the
/// window's update and counts it rather than looping against a hot key.
pub const DEFAULT_MAX_CAS_RETRIES: u32 = 5;

/// Base CAS backoff step. Attempt `n` sleeps `BASE * 2^(n-1)` plus up to 10%
/// jitter, so two replicas that collided do not re-collide in lockstep. Small
/// on purpose: this is a background flush whose whole retry budget must stay far
/// inside one debounce window.
const CAS_BACKOFF_BASE: Duration = Duration::from_millis(50);

/// Default cap on the number of distinct tenants [`MetadataSink`]'s local
/// fingerprint map tracks at once. Chosen generously above any deployment's
/// real tenant count (this is a memory-safety backstop, not an operational
/// limit); a tenant beyond the cap is not refused, its least-recently-touched
/// sibling with no pending flush is evicted from the local map instead, which
/// only costs that sibling one extra GET on its next observed metric (the
/// durable record already has what the local map forgot).
pub const DEFAULT_MAX_TRACKED_TENANTS: usize = 4096;

/// Tuning for [`MetadataSink`]. Every default is the ADR-0085 decision 1 value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataSinkConfig {
    /// Debounce window between flushes. One GET and at most one CAS PUT per
    /// tenant with pending metadata per window.
    pub window: Duration,
    /// Per-tenant durable entry cap. A *new* family name arriving when the
    /// record is already at the cap is dropped and counted
    /// (`ingest_metadata_entries_dropped_total`); its points are still ingested
    /// and queryable. Also the bound on the local fingerprint/pending maps'
    /// per-tenant size (see [`SinkState::fingerprints`]): a client cannot make
    /// this process track more distinct families for one tenant, in memory,
    /// than could ever be durably stored for it.
    pub entry_cap: usize,
    /// Cap on the number of distinct tenants tracked in the local fingerprint
    /// map at once (see [`DEFAULT_MAX_TRACKED_TENANTS`]). Bounds this
    /// process's total metadata-sink memory to roughly
    /// `max_tracked_tenants * entry_cap` entries regardless of how many
    /// distinct tenants or metric names a client mints.
    pub max_tracked_tenants: usize,
    /// How many times a CAS PUT is retried against a freshly read body after a
    /// conflict. The initial attempt is not a retry, so a window issues at most
    /// `max_cas_retries + 1` PUTs.
    pub max_cas_retries: u32,
    /// Turns the sink into a no-op: `observe` returns immediately without
    /// touching any map and `flush_once` does no I/O. For an operator who needs
    /// to switch metadata capture off without redeploying a different binary.
    pub disabled: bool,
}

impl Default for MetadataSinkConfig {
    fn default() -> Self {
        MetadataSinkConfig {
            window: DEFAULT_METADATA_FLUSH_WINDOW,
            entry_cap: DEFAULT_METRICS_META_ENTRY_CAP,
            max_tracked_tenants: DEFAULT_MAX_TRACKED_TENANTS,
            max_cas_retries: DEFAULT_MAX_CAS_RETRIES,
            disabled: false,
        }
    }
}

/// What one [`MetadataSink::flush_once`] call did, for tests and for the flush
/// loop's log line. Every field is a per-call count, never cumulative; the
/// cumulative view is [`IngestMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushSummary {
    /// Tenants that had a non-empty pending set and were processed.
    pub tenants_flushed: usize,
    /// Metadata-record GETs issued. One per tenant per attempt (a CAS retry
    /// re-reads, so it adds one).
    pub gets: usize,
    /// CAS PUTs *attempted*. A conflicted attempt counts here too, so
    /// `puts - cas_conflicts` is what landed.
    pub puts: usize,
    /// PUTs refused because another writer moved the record first.
    pub cas_conflicts: usize,
    /// Tenants whose window update was dropped (retries exhausted, or a
    /// read/write failure).
    pub dropped_flushes: usize,
    /// New family names not stored because the record was at `entry_cap`.
    pub dropped_over_cap: usize,
}

/// Map a decoder-side metric kind onto the durable record's kind.
///
/// The two enums have identical variants and live in two crates that must not
/// depend on each other (`ravel-otlp` is a store-agnostic decoder,
/// `ravel-catalog` owns the durable record), so the mapping lives here, in the
/// one crate that depends on both. It is a function rather than the
/// `impl From<..> for ..` a reader would expect: both types are foreign to this
/// crate, so that impl is forbidden by the orphan rule (E0117) and would have
/// to be written inside `ravel-otlp` or `ravel-catalog`, coupling one decoder
/// crate to the catalog or vice versa.
pub fn catalog_metric_kind(kind: WireMetricKind) -> CatalogMetricKind {
    match kind {
        WireMetricKind::Counter => CatalogMetricKind::Counter,
        WireMetricKind::Gauge => CatalogMetricKind::Gauge,
        WireMetricKind::Histogram => CatalogMetricKind::Histogram,
        WireMetricKind::Summary => CatalogMetricKind::Summary,
        WireMetricKind::Unknown => CatalogMetricKind::Unknown,
    }
}

/// One pending metadata tuple: the wire tuple plus the fingerprint that will be
/// committed to the local map once the flush that carried it succeeds (or
/// proved to be a no-op against the durable record).
#[derive(Debug, Clone)]
struct PendingEntry {
    kind: WireMetricKind,
    help: String,
    unit: String,
    fingerprint: u64,
}

/// One tenant's pending set. `tenant` is kept for log context only: the record's
/// key is derived from the hash, which the caller already resolved.
#[derive(Debug)]
struct PendingTenant {
    tenant: TenantId,
    entries: BTreeMap<String, PendingEntry>,
}

#[derive(Debug, Default)]
struct SinkState {
    /// Last-flushed fingerprint per (tenant, family name). Nested rather than
    /// keyed by a `(TenantHash, String)` tuple so the request-path lookup in
    /// `observe` borrows the name instead of cloning it into a key.
    ///
    /// Bounded per tenant at `MetadataSinkConfig::entry_cap`: a family name
    /// that would grow a tenant's tracked set past the cap is never
    /// fingerprinted or queued (see `observe`), so this map cannot grow past
    /// `entry_cap` distinct families per known tenant regardless of how many
    /// distinct names a client sends. Bounded in tenant count at
    /// `MetadataSinkConfig::max_tracked_tenants` (see `evict_tenant_if_full`):
    /// a hostile or misconfigured client cannot make this process's memory
    /// grow without bound just by minting metric names or tenants.
    fingerprints: HashMap<TenantHash, HashMap<String, u64>>,
    /// Names whose fingerprint differs from `fingerprints`, awaiting the next
    /// flush window. Ordered so a flush is deterministic. Same per-tenant cap
    /// as `fingerprints` (the two together, not each separately, bound one
    /// tenant's tracked family count).
    pending: BTreeMap<TenantHash, PendingTenant>,
    /// Last `touch_seq` a tenant was observed at, for `evict_tenant_if_full`'s
    /// least-recently-touched eviction when `max_tracked_tenants` is reached.
    /// A logical clock (not wall time) so eviction order is deterministic
    /// under test without a clock dependency.
    tenant_touch: HashMap<TenantHash, u64>,
    touch_seq: u64,
}

/// The one-per-process metric metadata sink. Share it by `Arc`: every ingest
/// surface calls [`observe`](Self::observe) on the same instance, and
/// [`run`](Self::run) drives the single flush loop.
pub struct MetadataSink {
    store: Arc<dyn ObjectStoreBackend>,
    config: MetadataSinkConfig,
    metrics: Arc<IngestMetrics>,
    /// Injected clock: the flush cadence and the CAS backoff sleep both go
    /// through it, so tests drive both without a wall-clock sleep (CLAUDE.md
    /// time injection).
    clock: Arc<dyn Clock>,
    /// Injected jitter source for the CAS backoff and the flush cadence.
    rng: Arc<dyn RngSource>,
    state: Mutex<SinkState>,
}

impl std::fmt::Debug for MetadataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSink")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl MetadataSink {
    /// Build the process's sink. `store` is the same foreground handle the
    /// ingest router writes data objects through, and `metrics` is the router's
    /// own counter registry (`IngestRouter::metrics_handle`) so the flush
    /// counters land in the snapshot an operator already scrapes.
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        config: MetadataSinkConfig,
        metrics: Arc<IngestMetrics>,
    ) -> Self {
        MetadataSink {
            store,
            config,
            metrics,
            clock: Arc::new(SystemClock),
            rng: Arc::new(SystemRng),
            state: Mutex::new(SinkState::default()),
        }
    }

    /// Replace the injected clock (the flush cadence and the CAS backoff).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Replace the injected jitter source.
    #[must_use]
    pub fn with_rng(mut self, rng: Arc<dyn RngSource>) -> Self {
        self.rng = rng;
        self
    }

    pub fn config(&self) -> &MetadataSinkConfig {
        &self.config
    }

    /// Record what one request's normalization decoded, on the ingest request
    /// path.
    ///
    /// Synchronous, allocation-light, and infallible by construction: it takes
    /// one short mutex, compares each tuple's fingerprint against the
    /// last-flushed one, and stores only the changed ones. It never touches the
    /// object store, never awaits, and returns nothing a caller could be
    /// tempted to fail a request on. Ingest acknowledgement therefore never
    /// depends on the metadata record (ADR-0085 decision 1).
    ///
    /// Within one window the last tuple for a family wins. A tuple with an
    /// empty `family_name` is dropped defensively: it names no family, and the
    /// record writer refuses it anyway.
    pub fn observe(
        &self,
        tenant: &TenantId,
        tenant_hash: TenantHash,
        metadata: impl IntoIterator<Item = MetricMetadata>,
    ) {
        if self.config.disabled {
            return;
        }
        let mut metadata = metadata.into_iter().peekable();
        if metadata.peek().is_none() {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            // A poisoned mutex means a previous holder panicked. Metadata
            // capture is best-effort and off the ack path, so this must not
            // propagate into an ingest request; the flush loop logs the
            // degradation on its next tick.
            return;
        };
        // Touch this tenant (for LRU eviction below) and, if it is one this
        // process has never tracked, make room under `max_tracked_tenants`
        // before adding anything for it. A tenant with pending work is never
        // evicted, so this can never evict the tenant it is about to serve.
        state.touch_seq = state.touch_seq.wrapping_add(1);
        let seq = state.touch_seq;
        state.tenant_touch.insert(tenant_hash, seq);
        if !state.fingerprints.contains_key(&tenant_hash)
            && !state.pending.contains_key(&tenant_hash)
        {
            evict_tenant_if_full(&mut state, tenant_hash, self.config.max_tracked_tenants);
        }
        for meta in metadata {
            if meta.family_name.is_empty() {
                continue;
            }
            let fingerprint = fingerprint_of(&meta);
            let existing_fingerprint = state
                .fingerprints
                .get(&tenant_hash)
                .and_then(|names| names.get(meta.family_name.as_str()))
                .copied();
            if existing_fingerprint == Some(fingerprint) {
                continue;
            }
            let already_pending = state
                .pending
                .get(&tenant_hash)
                .is_some_and(|p| p.entries.contains_key(meta.family_name.as_str()));
            // A name absent from both maps would grow this tenant's tracked
            // set by one slot. Refuse it past `entry_cap` so the local maps
            // cannot outgrow what the durable record could ever hold for this
            // tenant, however many distinct names a client sends -- the same
            // counter the flush-time durable-record cap uses, since both are
            // "this family name did not get a metadata entry because the
            // tenant is already at its cap."
            if existing_fingerprint.is_none() && !already_pending {
                let known = state.fingerprints.get(&tenant_hash).map_or(0, HashMap::len)
                    + state
                        .pending
                        .get(&tenant_hash)
                        .map_or(0, |p| p.entries.len());
                if known >= self.config.entry_cap {
                    self.metrics.record_metadata_entries_dropped(1);
                    continue;
                }
            }
            let pending = state
                .pending
                .entry(tenant_hash)
                .or_insert_with(|| PendingTenant {
                    tenant: tenant.clone(),
                    entries: BTreeMap::new(),
                });
            pending.entries.insert(
                meta.family_name,
                PendingEntry {
                    kind: meta.kind,
                    help: meta.help,
                    unit: meta.unit,
                    fingerprint,
                },
            );
        }
    }

    /// Flush every tenant with a non-empty pending set: one GET and at most one
    /// CAS PUT each (ADR-0085 decision 1). `now_ns` is the merge's
    /// `updated_unix_ns` for entries that actually changed, injected rather than
    /// read so a flush is deterministic.
    ///
    /// Never returns an error: every failure is logged, counted, and dropped.
    pub async fn flush_once(&self, now_ns: i64) -> FlushSummary {
        let mut summary = FlushSummary::default();
        if self.config.disabled {
            return summary;
        }
        let pending = self.take_pending();
        for (tenant_hash, pending) in pending {
            summary.tenants_flushed += 1;
            self.flush_tenant(tenant_hash, pending, now_ns, &mut summary)
                .await;
        }
        summary
    }

    /// The flush loop: one [`flush_once`](Self::flush_once) per debounce window
    /// until `shutdown` fires, then one final flush so a graceful stop does not
    /// discard the window in progress.
    ///
    /// The window sleep is jittered so replicas that started together do not
    /// contend on the same key on the same tick.
    pub async fn run(self: Arc<Self>, mut shutdown: oneshot::Receiver<()>) {
        if self.config.disabled {
            // Still wait for shutdown rather than returning: the task handle's
            // join then behaves the same in both configurations.
            let _ = shutdown.await;
            return;
        }
        loop {
            let window = jittered(self.config.window, self.rng.as_ref());
            tokio::select! {
                _ = self.clock.sleep(window) => {
                    let summary = self.flush_once(self.clock.now_ns()).await;
                    log_summary("metric metadata flush window complete", &summary);
                }
                _ = &mut shutdown => break,
            }
        }
        let summary = self.flush_once(self.clock.now_ns()).await;
        log_summary("metric metadata final flush on shutdown complete", &summary);
    }

    /// Take (not clone) every tenant's pending set. Anything `observe` adds
    /// while the flush runs lands in a fresh pending set and is flushed next
    /// window; because fingerprints are committed only after this flush
    /// concludes, such a tuple is at worst re-sent and merged as a no-op, never
    /// lost.
    fn take_pending(&self) -> BTreeMap<TenantHash, PendingTenant> {
        match self.state.lock() {
            Ok(mut state) => std::mem::take(&mut state.pending),
            Err(_) => {
                tracing::warn!(
                    "metric metadata sink state mutex poisoned; skipping this flush window \
                     (metadata capture degraded, ingest unaffected)"
                );
                BTreeMap::new()
            }
        }
    }

    async fn flush_tenant(
        &self,
        tenant_hash: TenantHash,
        pending: PendingTenant,
        now_ns: i64,
        summary: &mut FlushSummary,
    ) {
        let incoming: Vec<MetricMetadataEntry> = pending
            .entries
            .iter()
            .map(|(family_name, entry)| MetricMetadataEntry {
                family_name: family_name.clone(),
                kind: catalog_metric_kind(entry.kind),
                help: entry.help.clone(),
                unit: entry.unit.clone(),
                updated_unix_ns: now_ns,
            })
            .collect();

        let mut retries: u32 = 0;
        loop {
            summary.gets += 1;
            self.metrics.record_metadata_flush_get();
            let record = match read_metrics_meta(self.store.as_ref(), &tenant_hash).await {
                Ok(record) => record,
                Err(err) => {
                    self.record_dropped_flush(&pending.tenant, &tenant_hash, summary, &err);
                    return;
                }
            };
            let (existing, expected) = match record {
                Some((entries, version)) => (entries, Some(version)),
                // No record yet: the first write bootstraps with
                // `CreateIfAbsent` rather than CAS against a version that does
                // not exist (ADR-0085 decision 1).
                None => (Vec::new(), None),
            };

            let outcome = merge_entries(&existing, &incoming, now_ns, self.config.entry_cap);
            if !outcome.changed {
                // The durable record already says everything this window
                // carried: skip the PUT and just remember it locally. The
                // common case for a freshly started process.
                self.commit_fingerprints(tenant_hash, &pending);
                self.record_over_cap(&pending.tenant, outcome.dropped_over_cap, summary);
                return;
            }

            summary.puts += 1;
            self.metrics.record_metadata_flush_put();
            match write_metrics_meta(self.store.as_ref(), &tenant_hash, &outcome.merged, expected)
                .await
            {
                Ok(_) => {
                    self.commit_fingerprints(tenant_hash, &pending);
                    self.record_over_cap(&pending.tenant, outcome.dropped_over_cap, summary);
                    return;
                }
                Err(MetricsMetaError::CasConflict { .. }) => {
                    summary.cas_conflicts += 1;
                    if retries >= self.config.max_cas_retries {
                        // Drop this window's update and count it. Nothing is
                        // re-queued and no fingerprint is committed, so the next
                        // `observe` of a still-changed tuple re-arms it.
                        summary.dropped_flushes += 1;
                        self.metrics.record_metadata_flush_dropped();
                        tracing::warn!(
                            tenant = %pending.tenant.as_str(),
                            tenant_hash = %tenant_hash.to_hex(),
                            names = pending.entries.len(),
                            retries = self.config.max_cas_retries,
                            "metric metadata CAS lost every retry; dropping this window's \
                             metadata update (ingest unaffected, re-armed on the next changed \
                             tuple)"
                        );
                        return;
                    }
                    retries += 1;
                    self.clock.sleep(self.cas_backoff(retries)).await;
                }
                Err(err) => {
                    self.record_dropped_flush(&pending.tenant, &tenant_hash, summary, &err);
                    return;
                }
            }
        }
    }

    /// `CAS_BACKOFF_BASE * 2^(retry - 1)`, jittered. Saturates rather than
    /// overflowing on an absurd configured retry count.
    fn cas_backoff(&self, retry: u32) -> Duration {
        let factor = 1u32
            .checked_shl(retry.saturating_sub(1))
            .unwrap_or(u32::MAX);
        jittered(CAS_BACKOFF_BASE.saturating_mul(factor), self.rng.as_ref())
    }

    /// Remember every tuple this flush carried, so a later identical `observe`
    /// is skipped locally. Called after a successful PUT and after a no-op
    /// merge, never after a drop.
    fn commit_fingerprints(&self, tenant_hash: TenantHash, pending: &PendingTenant) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let names = state.fingerprints.entry(tenant_hash).or_default();
        for (family_name, entry) in &pending.entries {
            names.insert(family_name.clone(), entry.fingerprint);
        }
    }

    fn record_over_cap(&self, tenant: &TenantId, dropped: usize, summary: &mut FlushSummary) {
        if dropped == 0 {
            return;
        }
        summary.dropped_over_cap += dropped;
        self.metrics.record_metadata_entries_dropped(dropped as u64);
        tracing::warn!(
            tenant = %tenant.as_str(),
            dropped,
            cap = self.config.entry_cap,
            "metric metadata record at the per-tenant entry cap; new family names not stored \
             (their points are still ingested and queryable)"
        );
    }

    fn record_dropped_flush(
        &self,
        tenant: &TenantId,
        tenant_hash: &TenantHash,
        summary: &mut FlushSummary,
        err: &MetricsMetaError,
    ) {
        summary.dropped_flushes += 1;
        self.metrics.record_metadata_flush_dropped();
        tracing::warn!(
            tenant = %tenant.as_str(),
            tenant_hash = %tenant_hash.to_hex(),
            error = %err,
            "metric metadata flush failed; dropping this window's metadata update (ingest \
             unaffected, re-armed on the next changed tuple)"
        );
    }
}

/// If `state.fingerprints` already tracks `max_tracked_tenants` distinct
/// tenants, evict the least-recently-touched one that has no pending flush,
/// to make room for `incoming` (never itself a candidate). A tenant with
/// pending data is never evicted, so an evicted tenant's local state is
/// simply forgotten -- exactly what a process restart already does to every
/// tenant, and self-healing the same way: its next observed metric costs one
/// extra GET, not lost data (the durable record is unaffected).
///
/// A linear scan, same as `MetadataCache::evict_if_full` in `ravel-query`:
/// this only runs when a genuinely new tenant is seen with the map already
/// at capacity, not on every `observe` call, and `max_tracked_tenants` is a
/// memory-safety backstop sized in the low thousands, not a per-request cost.
fn evict_tenant_if_full(state: &mut SinkState, incoming: TenantHash, max_tracked_tenants: usize) {
    let max = max_tracked_tenants.max(1);
    while state.fingerprints.len() >= max {
        let victim = state
            .fingerprints
            .keys()
            .filter(|hash| **hash != incoming && !state.pending.contains_key(*hash))
            .min_by_key(|hash| state.tenant_touch.get(*hash).copied().unwrap_or(0))
            .copied();
        match victim {
            Some(hash) => {
                state.fingerprints.remove(&hash);
                state.tenant_touch.remove(&hash);
            }
            // Every tracked tenant has pending work: nothing is safe to
            // evict this call. The map temporarily exceeds
            // `max_tracked_tenants`; the next flush drains pending sets and
            // the next `observe` after that will find room.
            None => break,
        }
    }
}

fn log_summary(message: &'static str, summary: &FlushSummary) {
    if summary.tenants_flushed == 0 {
        return;
    }
    tracing::debug!(
        tenants = summary.tenants_flushed,
        gets = summary.gets,
        puts = summary.puts,
        cas_conflicts = summary.cas_conflicts,
        dropped_flushes = summary.dropped_flushes,
        dropped_over_cap = summary.dropped_over_cap,
        "{message}"
    );
}

/// `base` plus up to 10% jitter, the same shape every other jittered background
/// cadence in the codebase uses.
fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if bound_ms == 0 {
        return base;
    }
    base + Duration::from_millis(rng.jitter_ms(bound_ms))
}

/// Hash of the fields that make a metadata tuple current: kind, help, unit.
/// Deliberately *not* the family name (it is the map key) and not any timestamp
/// (a timestamp-only difference is not a change; see
/// [`ravel_catalog::MergeOutcome::changed`]). blake3 rather than a
/// `DefaultHasher` so the value is stable across processes and builds, which is
/// what makes a fingerprint comparable to one a restart would compute.
fn fingerprint_of(meta: &MetricMetadata) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[kind_tag(meta.kind)]);
    // Length-prefixed so ("ab", "c") and ("a", "bc") cannot collide.
    hasher.update(&(meta.help.len() as u64).to_le_bytes());
    hasher.update(meta.help.as_bytes());
    hasher.update(&(meta.unit.len() as u64).to_le_bytes());
    hasher.update(meta.unit.as_bytes());
    let digest = hasher.finalize();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(head)
}

fn kind_tag(kind: WireMetricKind) -> u8 {
    match kind {
        WireMetricKind::Counter => 1,
        WireMetricKind::Gauge => 2,
        WireMetricKind::Histogram => 3,
        WireMetricKind::Summary => 4,
        WireMetricKind::Unknown => 5,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, PageToken,
        PutOptions, PutOutcome, StoreError,
    };

    const NOW: i64 = 1_700_000_000_000_000_000;

    /// A clock whose `sleep` completes immediately, so a CAS backoff costs no
    /// wall-clock time, and whose `now_ns` is fixed.
    #[derive(Debug)]
    struct InstantClock;

    impl Clock for InstantClock {
        fn now_ns(&self) -> i64 {
            NOW
        }

        fn sleep(
            &self,
            _dur: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::ready(()))
        }
    }

    /// A clock whose `sleep` never completes, so the flush loop's window arm can
    /// never fire and only the shutdown arm can.
    #[derive(Debug)]
    struct NeverClock;

    impl Clock for NeverClock {
        fn now_ns(&self) -> i64 {
            NOW
        }

        fn sleep(
            &self,
            _dur: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::pending())
        }
    }

    /// Deterministic jitter: none.
    #[derive(Debug)]
    struct NoJitter;

    impl RngSource for NoJitter {
        fn jitter_ms(&self, _max_ms: u64) -> u64 {
            0
        }

        fn new_uuid(&self) -> uuid::Uuid {
            uuid::Uuid::nil()
        }
    }

    /// A backend that panics on every access. `observe` must never reach it.
    struct PanicStore;

    #[async_trait::async_trait]
    impl ObjectStoreBackend for PanicStore {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            panic!("observe must not touch the object store");
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            panic!("observe must not touch the object store");
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            panic!("observe must not touch the object store");
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            panic!("observe must not touch the object store");
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            panic!("observe must not touch the object store");
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            panic!("observe must not touch the object store");
        }

        fn capabilities(&self) -> Capabilities {
            panic!("observe must not touch the object store");
        }
    }

    type Counting = Arc<InstrumentedStore<MemoryStore>>;
    type CountingFaulty = Arc<InstrumentedStore<FaultStore<MemoryStore>>>;

    fn counting_store() -> Counting {
        Arc::new(InstrumentedStore::new(MemoryStore::new()))
    }

    fn faulty_store(plan: FaultPlan) -> CountingFaulty {
        Arc::new(InstrumentedStore::new(FaultStore::new(
            MemoryStore::new(),
            plan,
        )))
    }

    fn gets(store: &InstrumentedStore<impl ObjectStoreBackend>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Get).calls
    }

    fn puts(store: &InstrumentedStore<impl ObjectStoreBackend>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Put).calls
    }

    fn sink(store: Arc<dyn ObjectStoreBackend>, config: MetadataSinkConfig) -> Arc<MetadataSink> {
        Arc::new(
            MetadataSink::new(store, config, Arc::new(IngestMetrics::default()))
                .with_clock(Arc::new(InstantClock))
                .with_rng(Arc::new(NoJitter)),
        )
    }

    fn meta(name: &str, help: &str, unit: &str) -> MetricMetadata {
        MetricMetadata {
            family_name: name.to_string(),
            kind: WireMetricKind::Counter,
            help: help.to_string(),
            unit: unit.to_string(),
        }
    }

    fn families(prefix: &str, count: usize) -> Vec<MetricMetadata> {
        (0..count)
            .map(|i| meta(&format!("{prefix}_{i}_total"), "help text", "bytes"))
            .collect()
    }

    async fn stored(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<MetricMetadataEntry> {
        read_metrics_meta(store, &tenant.hash())
            .await
            .expect("read record")
            .map(|(entries, _)| entries)
            .unwrap_or_default()
    }

    /// Acceptance test for ADR-0085 decision 1's request budget: however many
    /// names arrive in a window, a tenant costs exactly one GET and at most one
    /// PUT. Asserted on the store's own call counters, not on the summary, so a
    /// summary that lied would not pass.
    #[tokio::test]
    async fn window_flush_does_one_get_and_at_most_one_put_per_tenant() {
        let store = counting_store();
        let sink = sink(store.clone(), MetadataSinkConfig::default());
        let a = TenantId::new("tenant-a");
        let b = TenantId::new("tenant-b");

        for chunk in families("alpha", 250).chunks(25) {
            sink.observe(&a, a.hash(), chunk.to_vec());
        }
        for chunk in families("beta", 250).chunks(25) {
            sink.observe(&b, b.hash(), chunk.to_vec());
        }

        let summary = sink.flush_once(NOW).await;

        assert_eq!(gets(store.as_ref()), 2, "one GET per tenant, per window");
        assert_eq!(puts(store.as_ref()), 2, "one PUT per tenant, per window");
        assert_eq!(summary.tenants_flushed, 2);
        assert_eq!(summary.cas_conflicts, 0);
        assert_eq!(summary.dropped_flushes, 0);
        assert_eq!(summary.dropped_over_cap, 0);

        assert_eq!(stored(store.as_ref(), &a).await.len(), 250);
        assert_eq!(stored(store.as_ref(), &b).await.len(), 250);
    }

    /// A restarted replica's local map is empty, so every name looks new. The
    /// durable record already reflects them, so the merge is a no-op and the
    /// window must cost its GET and nothing else.
    #[tokio::test]
    async fn restart_against_populated_record_does_zero_puts() {
        let store = counting_store();
        let a = TenantId::new("tenant-a");
        let b = TenantId::new("tenant-b");
        let first = sink(store.clone(), MetadataSinkConfig::default());
        first.observe(&a, a.hash(), families("alpha", 250));
        first.observe(&b, b.hash(), families("beta", 250));
        first.flush_once(NOW).await;
        let gets_before = gets(store.as_ref());
        let puts_before = puts(store.as_ref());

        // A fresh process: same store, same tuples, empty fingerprint map.
        let restarted = sink(store.clone(), MetadataSinkConfig::default());
        restarted.observe(&a, a.hash(), families("alpha", 250));
        restarted.observe(&b, b.hash(), families("beta", 250));
        let summary = restarted.flush_once(NOW + 1_000_000_000).await;

        assert_eq!(gets(store.as_ref()) - gets_before, 2);
        assert_eq!(puts(store.as_ref()) - puts_before, 0);
        assert_eq!(summary.puts, 0);
        assert_eq!(summary.tenants_flushed, 2);
    }

    /// A help change during the process's lifetime is detected locally (the
    /// fingerprint is keyed on content, not just on the name) and flushed; an
    /// identical re-observation is not.
    #[tokio::test]
    async fn changed_help_mid_lifetime_is_flushed() {
        let store = counting_store();
        let sink = sink(store.clone(), MetadataSinkConfig::default());
        let tenant = TenantId::new("tenant-a");
        sink.observe(
            &tenant,
            tenant.hash(),
            [meta("http_requests_total", "old", "")],
        );
        sink.flush_once(NOW).await;
        let gets_before = gets(store.as_ref());
        let puts_before = puts(store.as_ref());

        sink.observe(
            &tenant,
            tenant.hash(),
            [meta("http_requests_total", "new", "")],
        );
        let summary = sink.flush_once(NOW + 1).await;
        assert_eq!(gets(store.as_ref()) - gets_before, 1);
        assert_eq!(puts(store.as_ref()) - puts_before, 1);
        assert_eq!(summary.puts, 1);
        let entries = stored(store.as_ref(), &tenant).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].help, "new");

        // The identical tuple again: nothing pending, so the next window does
        // not even read the record. Re-baselined here because the `stored`
        // assertion above is itself a GET through the same counting store.
        let gets_settled = gets(store.as_ref());
        sink.observe(
            &tenant,
            tenant.hash(),
            [meta("http_requests_total", "new", "")],
        );
        let summary = sink.flush_once(NOW + 2).await;
        assert_eq!(
            gets(store.as_ref()) - gets_settled,
            0,
            "no GET without pending work"
        );
        assert_eq!(summary.tenants_flushed, 0);
        assert_eq!(summary.gets, 0);
    }

    /// RW1 supplies help without unit and OTLP supplies unit without help; the
    /// two must compose, never clobber (the field-wise merge rule).
    #[tokio::test]
    async fn empty_incoming_unit_does_not_clobber_populated() {
        let store = counting_store();
        let sink = sink(store.clone(), MetadataSinkConfig::default());
        let tenant = TenantId::new("tenant-a");
        sink.observe(
            &tenant,
            tenant.hash(),
            [meta("bytes_sent_total", "", "bytes")],
        );
        sink.flush_once(NOW).await;

        sink.observe(
            &tenant,
            tenant.hash(),
            [meta("bytes_sent_total", "bytes sent", "")],
        );
        sink.flush_once(NOW + 1).await;

        let entries = stored(store.as_ref(), &tenant).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].help, "bytes sent");
        assert_eq!(
            entries[0].unit, "bytes",
            "an empty incoming unit must not clobber"
        );
    }

    /// A lost CAS re-reads the winner's body, re-merges onto it, and converges.
    #[tokio::test]
    async fn cas_conflict_rereads_and_converges() {
        let store = faulty_store(
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
                    .with_key_contains("/m/meta")
                    .with_occurrence(ravel_object_store::fault::Occurrence::Nth(1)),
            ),
        );
        let sink = sink(store.clone(), MetadataSinkConfig::default());
        let tenant = TenantId::new("tenant-a");
        sink.observe(&tenant, tenant.hash(), families("alpha", 3));

        let summary = sink.flush_once(NOW).await;

        assert_eq!(
            store
                .inner()
                .fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            1,
            "the scripted conflict must actually have fired"
        );
        assert_eq!(summary.cas_conflicts, 1);
        assert_eq!(summary.dropped_flushes, 0);
        assert_eq!(summary.gets, 2, "the retry re-reads before re-merging");
        assert_eq!(summary.puts, 2);
        assert_eq!(stored(store.as_ref(), &tenant).await.len(), 3);
    }

    /// More conflicts than the retry budget: the window's update is dropped and
    /// counted, and the ingest path never learns about it.
    #[tokio::test]
    async fn cas_retries_exhausted_drops_and_counts() {
        let store = faulty_store(FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/m/meta"),
        ));
        let metrics = Arc::new(IngestMetrics::default());
        let config = MetadataSinkConfig {
            max_cas_retries: 2,
            ..MetadataSinkConfig::default()
        };
        let sink = Arc::new(
            MetadataSink::new(store.clone(), config, metrics.clone())
                .with_clock(Arc::new(InstantClock))
                .with_rng(Arc::new(NoJitter)),
        );
        let tenant = TenantId::new("tenant-a");

        // `observe` is infallible and returns nothing: the ingest path cannot
        // see the failure that follows.
        sink.observe(&tenant, tenant.hash(), families("alpha", 2));
        let summary = sink.flush_once(NOW).await;

        assert_eq!(summary.dropped_flushes, 1);
        assert_eq!(summary.cas_conflicts, 3, "initial attempt plus two retries");
        assert_eq!(summary.puts, 3);
        assert_eq!(metrics.snapshot().metadata_flush_dropped_total, 1);
        assert!(stored(store.as_ref(), &tenant).await.is_empty());

        // Nothing was re-queued, and no fingerprint was committed, so the next
        // observation of the same tuple re-arms the write.
        assert_eq!(sink.flush_once(NOW + 1).await.tenants_flushed, 0);
        sink.observe(&tenant, tenant.hash(), families("alpha", 2));
        assert_eq!(sink.flush_once(NOW + 2).await.tenants_flushed, 1);
    }

    /// The per-tenant entry cap drops *new* names, visibly, and never blocks an
    /// update to a name already stored.
    #[tokio::test]
    async fn over_cap_new_names_are_dropped_and_counted() {
        let store = counting_store();
        let metrics = Arc::new(IngestMetrics::default());
        let config = MetadataSinkConfig {
            entry_cap: 3,
            ..MetadataSinkConfig::default()
        };
        let sink = Arc::new(
            MetadataSink::new(store.clone(), config, metrics.clone())
                .with_clock(Arc::new(InstantClock))
                .with_rng(Arc::new(NoJitter)),
        );
        let tenant = TenantId::new("tenant-a");

        sink.observe(&tenant, tenant.hash(), families("alpha", 5));
        let summary = sink.flush_once(NOW).await;

        let entries = stored(store.as_ref(), &tenant).await;
        assert_eq!(entries.len(), 3);
        // `observe` itself refuses a name that would grow this tenant's
        // tracked set past `entry_cap` (the local-map memory bound), so the
        // two over-cap names never reach `pending` and `merge_entries` never
        // sees them at flush time: `dropped_over_cap` is the *merge's* own
        // count, and it is 0 here. The counter still fires from `observe`'s
        // own drop path, which is what the durable outcome (3 stored, 2
        // dropped) actually depends on.
        assert_eq!(summary.dropped_over_cap, 0);
        assert_eq!(metrics.snapshot().metadata_entries_dropped_total, 2);

        // An existing name's help still updates at the cap: shrinking or
        // refusing a stored entry is a pruning decision, not a side effect of
        // one more name arriving.
        let existing = entries[0].family_name.clone();
        sink.observe(
            &tenant,
            tenant.hash(),
            [meta(&existing, "revised help", "bytes")],
        );
        sink.flush_once(NOW + 1).await;
        let entries = stored(store.as_ref(), &tenant).await;
        assert_eq!(entries.len(), 3);
        let updated = entries
            .iter()
            .find(|e| e.family_name == existing)
            .expect("existing family still stored");
        assert_eq!(updated.help, "revised help");
    }

    /// The memory-safety regression this exists to prevent: a single burst of
    /// far more distinct family names than `entry_cap` must not make the
    /// local maps grow past it before a flush ever runs. Proven indirectly
    /// (the fields are private): if `observe` queued every name instead of
    /// capping locally, `flush_once` would still do one GET/PUT (the flush
    /// loop is always one-per-tenant), but `merge_entries` would report a
    /// dropped_over_cap of `10_000 - cap`, not 0, because the flush-time
    /// cap would be doing all the work `observe` should have already done.
    #[tokio::test]
    async fn observe_bounds_a_huge_burst_before_any_flush_runs() {
        let store = counting_store();
        let metrics = Arc::new(IngestMetrics::default());
        let config = MetadataSinkConfig {
            entry_cap: 5,
            ..MetadataSinkConfig::default()
        };
        let sink = Arc::new(
            MetadataSink::new(store.clone(), config, metrics.clone())
                .with_clock(Arc::new(InstantClock))
                .with_rng(Arc::new(NoJitter)),
        );
        let tenant = TenantId::new("tenant-a");

        sink.observe(&tenant, tenant.hash(), families("burst", 10_000));
        let summary = sink.flush_once(NOW).await;

        assert_eq!(summary.gets, 1);
        assert_eq!(summary.puts, 1);
        assert_eq!(
            summary.dropped_over_cap, 0,
            "observe already refused the over-cap names; merge_entries never saw them"
        );
        assert_eq!(stored(store.as_ref(), &tenant).await.len(), 5);
        assert_eq!(metrics.snapshot().metadata_entries_dropped_total, 9_995);
    }

    /// `max_tracked_tenants` bounds the number of distinct tenants the local
    /// fingerprint map holds. A tenant evicted to make room is not lost data:
    /// its durable record is untouched, and re-observing an unchanged metric
    /// for it costs one extra GET (the map "forgot" it, exactly what a
    /// process restart already does), never an extra PUT.
    #[tokio::test]
    async fn max_tracked_tenants_evicts_the_idle_tenant_and_self_heals() {
        let store = counting_store();
        let metrics = Arc::new(IngestMetrics::default());
        let config = MetadataSinkConfig {
            max_tracked_tenants: 2,
            ..MetadataSinkConfig::default()
        };
        let sink = Arc::new(
            MetadataSink::new(store.clone(), config, metrics.clone())
                .with_clock(Arc::new(InstantClock))
                .with_rng(Arc::new(NoJitter)),
        );
        let tenant_a = TenantId::new("tenant-a");
        let tenant_b = TenantId::new("tenant-b");
        let tenant_c = TenantId::new("tenant-c");

        // A and B each get one metric, and are flushed (so neither has
        // pending work and both are eviction-eligible). A is touched first,
        // so it is the least-recently-touched of the two once C arrives.
        sink.observe(&tenant_a, tenant_a.hash(), [meta("m", "help", "bytes")]);
        sink.flush_once(NOW).await;
        sink.observe(&tenant_b, tenant_b.hash(), [meta("m", "help", "bytes")]);
        sink.flush_once(NOW).await;

        // A third tenant, with the map already at max_tracked_tenants,
        // evicts A (not B, which was touched more recently).
        sink.observe(&tenant_c, tenant_c.hash(), [meta("m", "help", "bytes")]);
        let summary = sink.flush_once(NOW).await;
        assert_eq!(summary.tenants_flushed, 1, "only C had pending work");

        // B, never evicted, is still a pure local hit: no GET, no PUT.
        let gets_before = gets(store.as_ref());
        let puts_before = puts(store.as_ref());
        sink.observe(&tenant_b, tenant_b.hash(), [meta("m", "help", "bytes")]);
        let summary = sink.flush_once(NOW).await;
        assert_eq!(summary.tenants_flushed, 0, "B is still tracked locally");
        assert_eq!(gets(store.as_ref()), gets_before);
        assert_eq!(puts(store.as_ref()), puts_before);

        // Re-observing A's already-durable, unchanged metric looks "new"
        // locally (A was evicted), so it flushes: one more GET, but the merge
        // is a no-op against the durable record, so zero more PUTs. This
        // insert itself evicts whichever of {B, C} is now least-recently
        // touched (B, touched before C arrived) -- LRU eviction keeps
        // running exactly the same way once the map is back at capacity;
        // this call only asserts A's own self-healing GET-not-PUT, not B's
        // fate afterward.
        let gets_before = gets(store.as_ref());
        let puts_before = puts(store.as_ref());
        sink.observe(&tenant_a, tenant_a.hash(), [meta("m", "help", "bytes")]);
        let summary = sink.flush_once(NOW).await;
        assert_eq!(summary.tenants_flushed, 1);
        assert_eq!(gets(store.as_ref()), gets_before + 1);
        assert_eq!(
            puts(store.as_ref()),
            puts_before,
            "the durable record already had A's unchanged entry; eviction cost a GET, not a PUT"
        );
    }

    /// `observe` is synchronous (this test is not `async` and never awaits it)
    /// and does no I/O: a store that panics on every access is untouched.
    #[test]
    fn observe_never_awaits() {
        let sink = MetadataSink::new(
            Arc::new(PanicStore),
            MetadataSinkConfig::default(),
            Arc::new(IngestMetrics::default()),
        );
        let tenant = TenantId::new("tenant-a");
        for _ in 0..100 {
            sink.observe(&tenant, tenant.hash(), families("alpha", 20));
        }
        // Pending work accumulated without a single store call; only a flush
        // (which this test never runs) would touch the store.
        let state = sink.state.lock().expect("state lock");
        assert_eq!(state.pending.len(), 1);
        assert_eq!(
            state
                .pending
                .values()
                .next()
                .expect("one tenant pending")
                .entries
                .len(),
            20
        );
    }

    /// An empty family name is dropped rather than sent to a writer that would
    /// refuse the whole record.
    #[tokio::test]
    async fn empty_family_name_is_dropped_defensively() {
        let store = counting_store();
        let sink = sink(store.clone(), MetadataSinkConfig::default());
        let tenant = TenantId::new("tenant-a");
        sink.observe(&tenant, tenant.hash(), [meta("", "help", "bytes")]);
        let summary = sink.flush_once(NOW).await;
        assert_eq!(summary.tenants_flushed, 0);
        assert_eq!(puts(store.as_ref()), 0);
    }

    /// A disabled sink is a no-op on both halves.
    #[tokio::test]
    async fn disabled_sink_never_reads_or_writes() {
        let config = MetadataSinkConfig {
            disabled: true,
            ..MetadataSinkConfig::default()
        };
        let sink = sink(Arc::new(PanicStore), config);
        let tenant = TenantId::new("tenant-a");
        sink.observe(&tenant, tenant.hash(), families("alpha", 5));
        assert_eq!(sink.flush_once(NOW).await, FlushSummary::default());
    }

    /// Shutdown flushes the window in progress rather than discarding it. The
    /// clock's window sleep never completes, so only the shutdown arm can fire:
    /// whatever is written came from the final flush.
    #[tokio::test]
    async fn shutdown_does_a_final_flush() {
        let store = counting_store();
        let sink = Arc::new(
            MetadataSink::new(
                store.clone(),
                MetadataSinkConfig::default(),
                Arc::new(IngestMetrics::default()),
            )
            .with_clock(Arc::new(NeverClock))
            .with_rng(Arc::new(NoJitter)),
        );
        let tenant = TenantId::new("tenant-a");
        sink.observe(&tenant, tenant.hash(), families("alpha", 4));

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(sink.clone().run(rx));
        tx.send(()).expect("shutdown receiver alive");
        task.await.expect("flush loop joined");

        assert_eq!(stored(store.as_ref(), &tenant).await.len(), 4);
    }

    #[test]
    fn fingerprint_covers_kind_help_and_unit_and_nothing_else() {
        let base = meta("m", "help", "bytes");
        assert_eq!(
            fingerprint_of(&base),
            fingerprint_of(&meta("other", "help", "bytes"))
        );
        assert_ne!(
            fingerprint_of(&base),
            fingerprint_of(&meta("m", "help2", "bytes"))
        );
        assert_ne!(
            fingerprint_of(&base),
            fingerprint_of(&meta("m", "help", "seconds"))
        );
        let gauge = MetricMetadata {
            kind: WireMetricKind::Gauge,
            ..base.clone()
        };
        assert_ne!(fingerprint_of(&base), fingerprint_of(&gauge));
        // Length-prefixed field encoding: a boundary shift is a different
        // fingerprint, not a collision.
        assert_ne!(
            fingerprint_of(&meta("m", "ab", "c")),
            fingerprint_of(&meta("m", "a", "bc"))
        );
    }

    #[test]
    fn every_wire_kind_maps_to_its_catalog_twin() {
        assert_eq!(
            catalog_metric_kind(WireMetricKind::Counter),
            CatalogMetricKind::Counter
        );
        assert_eq!(
            catalog_metric_kind(WireMetricKind::Gauge),
            CatalogMetricKind::Gauge
        );
        assert_eq!(
            catalog_metric_kind(WireMetricKind::Histogram),
            CatalogMetricKind::Histogram
        );
        assert_eq!(
            catalog_metric_kind(WireMetricKind::Summary),
            CatalogMetricKind::Summary
        );
        assert_eq!(
            catalog_metric_kind(WireMetricKind::Unknown),
            CatalogMetricKind::Unknown
        );
    }
}
