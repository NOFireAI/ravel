//! ADR-0046 cache warmup: populate the read cache with each tenant's most
//! recent parts before `/readyz` latches, so the first real query after a
//! restart is not the one that pays every cold-fetch cost.
//!
//! "Most recent" means most recently ingested, measured from each tenant's
//! own latest ingest hour ([`Catalog::latest_ingest_hour`]), not from
//! wall-clock `now` (issue #1233, ADR-0046 amendment 2026-09-05): a tenant
//! whose last ingest predates `now` by more than [`WARM_LOOKBACK_HOURS`]
//! hours still gets its most recent parts warmed, not zero. The warm set is
//! every part filed under an ingest-hour bucket in `[latest -
//! WARM_LOOKBACK_HOURS, latest]` ([`Catalog::resolve_ingest_hours`]),
//! whatever its event times (issue #4257): a tenant loaded with event times
//! years before its ingest time is warmed exactly like one whose event times
//! sit at its ingest hours. The set is ordered newest ingest hour first and,
//! within an hour, by greatest `max_event_ts_ns` first; the whole-part warm
//! and the metadata pass below both take parts in that order, the whole-part
//! warm taking the parts at or under the plan bound and the metadata pass the
//! parts above it (#4307). "Ingest hour"
//! here means any hour holding a commit-record-shaped key of any kind (a raw
//! commit record, a compaction record, a rewrite record, or a tombstone): a
//! tombstone-only or rewrite-only hour can anchor the window exactly as a
//! fresh commit record would.
//!
//! The pass runs in this order (issue #4257): tenant discovery, then part
//! resolution, the whole-part warm and the logs metadata pass, all three
//! concurrently. Part resolution resolves each tenant's warm set for both
//! signals, at most [`PART_RESOLUTIONS_IN_FLIGHT`] tenants at once, each
//! resolution holding one permit of the shared store GET limiter while it
//! runs, and hands the sets on in discovery order as they complete: a tenant
//! is warmed while later tenants are still resolving, so a slow listing
//! store spends the deadline on warming the first tenants, not on resolving
//! every tenant before any warm starts. The whole-part warm takes tenants in
//! discovery order as their sets arrive and, within a tenant, fetches its
//! metric and logs parts concurrently, at most
//! [`WHOLE_PARTS_IN_FLIGHT_PER_SIGNAL`] parts of each signal at once, each
//! GET through the shared store GET limiter. The metadata pass takes tenants
//! in the same order on its own future and reads at most one object fewer at
//! once than the GET limiter's permit count (at least one). With at least 2
//! permits, one permit stays free of the pass, so neither step waits for the
//! other to finish and a pass hung on one tenant does not hold back a later
//! tenant's whole-part warm. With 1 permit the pass reads on the only permit,
//! and a hung probe holds it, and with it every later resolution and
//! whole-part GET, until the deadline.
//!
//! The whole-part warm reads a logs part the way the query path does (#4307):
//! whole at or under [`LogSegmentFetcher::plan_whole_object_bound`], and not at
//! all above it. Above the bound the query path reads only the part's
//! directories, which the metadata pass below warms; a whole read here would
//! fetch and decode blocks no plan needs. A part above the bound is dropped
//! from the whole-part set before the [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] cap
//! and counted in `parts_above_plan_bound`, so the cap takes the most recent
//! parts at or under the bound. The whole-part fetcher is built with the same
//! block-range threshold, request cost and projection break-even the metadata
//! pass uses, so the two steps agree on the bound: a logs part is warmed whole
//! here or has its metadata warmed below, never both. The two do not cover
//! every part, so this is not a strict partition: a part at or under the bound
//! beyond the whole-part cap ([`MAX_PARTS_PER_TENANT_PER_SIGNAL`]) is warmed by
//! neither (the metadata pass reads only parts above the bound), counted in
//! `parts_beyond_cap`; and when the metadata pass is absent (an invalid fetch
//! bound, see [`LogsMetadataPass::new`]) a part above the bound is warmed by
//! neither either. (The metric side is unchanged: a metric part above
//! [`DEFAULT_WHOLE_OBJECT_THRESHOLD`] is still warmed, on its footer tail,
//! never skipped.)
//!
//! The whole-part warm holds to the cache budget minus the metadata pass's
//! ceiling (below), or to the whole budget when the pass does not run. Each
//! part reserves what its fetch reads before the fetch starts: a logs part
//! its object size (the logs funnel reads the object whole), a metric part
//! what the metrics funnel's first GET reads (the whole object at or below
//! [`DEFAULT_WHOLE_OBJECT_THRESHOLD`], else a [`DEFAULT_SUFFIX_LEN`] footer
//! tail). A part that does not fit waits for its signal's fetches in flight,
//! which settle to the bytes they read and can free room; one that does not
//! fit with none in flight is skipped, counted in `parts_over_budget`. Once a
//! fetch completes, its reservation becomes the bytes it read from the store.
//! Together the two steps insert no more than the cache holds, so neither
//! evicts what the other counted, as long as each stays inside its share: a
//! metric part whose catalog sections reach past its footer tail reads more
//! than it reserved, and the metadata pass's admission check counts an
//! in-flight object's probe length, not the directory reads after it, so
//! reads past a reservation can carry either step over.
//!
//! Bounded on two axes. Per (tenant, signal), at most
//! [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] of the most recent parts are warmed
//! whole: this pass exists to take the edge off a cold restart, not to size
//! a working set only real query traffic can size correctly. The whole pass,
//! every step above included, is wrapped in `tokio::time::timeout(`
//! [`WARM_DEADLINE`]`, ..)`, so a large tenant count or a slow store can
//! never hold `/readyz` back indefinitely.
//!
//! The logs metadata pass (ADR-4203 decision 8) is not bounded by that part
//! cap. For every logs object in the warm set that the query path plans
//! from its directories (above
//! [`LogSegmentFetcher::plan_whole_object_bound`]), it reads the footer and
//! the four directories (STREAM_DIR, FIELD_DIR, SKIP_IDX, PAGE_DIR) through
//! [`BlockRangeFetcher::fetch_plan_directories`], on a fetcher built from
//! the query path's own logs fetch quantities ([`LogsMetadataWarm`]): the
//! suffix probe, then, for sections the probe does not hold whole, one
//! coalesced read of the missing SKIP_IDX and PAGE_DIR and one of the
//! missing STREAM_DIR and FIELD_DIR. Those are the extents and cache keys an
//! unkeyed plan read of the same object uses. A keyed plan
//! (`fetch_keyed_plan`, a statement with key-equality arms) hits the same
//! probe key, but also reads KEY_IDX and the BLOOM and POSTINGS sections its
//! filter needs, none of which the pass warms. A BLOOM or POSTINGS section
//! outside the probe and within the coalesce gap of the missing SKIP_IDX and
//! PAGE_DIR joins them in one read, whose extent then differs from the
//! warmed one and misses the cache. The pass reads no column page. Its
//! ceiling is [`METADATA_CACHE_SHARE_DIVISOR`]'s share of the cache budget,
//! charged only with the bytes the pass's own reads fetch from the store:
//! the whole-part warm's residents do not consume it, and an object already
//! cached charges nothing. It admits an object only while the bytes its
//! reads fetched, plus the probe lengths of the objects in flight, plus the
//! object's own probe length stay within the ceiling.
//!
//! The `cache warmup: pass complete` line reports how long each step took
//! (`tenant_discovery_elapsed_ms`, `part_resolution_elapsed_ms`,
//! `whole_part_warm_elapsed_ms`, `metadata_pass_elapsed_ms`). The deadline
//! warning reports what was warmed when the deadline hit (`parts_warmed`,
//! `metadata_objects_warmed`, `metadata_bytes_warmed`), read from counters
//! each fetch updates as it completes, and the steps still running (`step`).
//! Both lines carry the metadata pass's concurrency
//! (`metadata_get_permits`), the bytes the whole-part warm holds against
//! its share (`whole_part_bytes_reserved`), the count of logs parts the
//! whole-part step skipped for being above the plan bound
//! (`parts_above_plan_bound`), and the count it dropped at the per-(tenant,
//! signal) cap (`parts_beyond_cap`); the per-(tenant, signal) line carries
//! both counts too. With both, whole-part plus metadata objects reconcile
//! against a tenant's logs object count: a part is warmed whole
//! (`parts_warmed`), left over budget (`parts_over_budget`), dropped at the cap
//! (`parts_beyond_cap`), or warmed by the metadata pass for being above the
//! bound (`parts_above_plan_bound`) (#4307).
//!
//! Every failure degrades to "warm less than planned," never to a startup
//! failure: tenant discovery erroring, a resolve erroring, a fetch erroring,
//! and the deadline itself are all handled identically, by logging and
//! moving on. This pass is an optimization, never a correctness or
//! availability dependency; a node with a cold cache answers every query
//! correctly and only more slowly (ADR-0046 consequences).
//!
//! Warming calls exactly the funnels the real query paths already call --
//! [`SegmentFetcher::fetch_series`] with no matchers (footer + catalog
//! sections only, no page data: see its own doc, "labels only, no
//! samples") and [`LogSegmentFetcher::fetch_accounted_with_tenant`] (RLOG's
//! only funnel, always a whole-object fetch) -- so warming a part costs
//! exactly what a real query resolving that part would cost, and nothing
//! here reimplements or changes either fetcher's cache behavior. The
//! metadata pass calls the plan phase's directory read the same way.
//!
//! [`BlockRangeFetcher::fetch_plan_directories`]: ravel_query::BlockRangeFetcher::fetch_plan_directories

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt as _};
use ravel_catalog::{Catalog, SegmentRef};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_query::{
    DEFAULT_SUFFIX_LEN, DEFAULT_WHOLE_OBJECT_THRESHOLD, EngineConfig, LogQuery, LogSegmentFetcher,
    ReadCache, SegmentFetcher,
};
use ravel_types::{Signal, TenantHash};

/// Per (tenant, signal), the number of most-recent parts warmed whole.
const MAX_PARTS_PER_TENANT_PER_SIGNAL: usize = 4;

/// The tenants whose warm sets resolve at once, each resolution holding one
/// GET limiter permit, so resolution never takes every permit from the warms
/// running beside it on a limiter with more permits than this.
const PART_RESOLUTIONS_IN_FLIGHT: usize = 4;

/// Per (tenant, signal), the whole-part fetches in flight at once. Each one
/// decodes the part it reads, and the memory budget it reserves against covers
/// the fetched bytes, not the decoded records, so this bounds the decoded
/// memory the warm holds at startup.
const WHOLE_PARTS_IN_FLIGHT_PER_SIGNAL: usize = 2;

/// The logs metadata pass's ceiling is `1 / METADATA_CACHE_SHARE_DIVISOR` of
/// the read cache's byte budget, charged with the bytes the pass itself
/// fetches. The whole-part warm holds to the rest, so the warm never fills the
/// cache past its budget: a full cache would evict warmed metadata a statement
/// has not read yet.
const METADATA_CACHE_SHARE_DIVISOR: u64 = 2;

/// The query path's logs fetch quantities and the read cache's byte budget:
/// what the metadata pass needs to read the same cache keys a statement reads
/// and to stay inside the cache. Build it from the same [`EngineConfig`] both
/// query surfaces are built from.
#[derive(Clone, Copy, Debug)]
pub struct LogsMetadataWarm {
    block_range_threshold: u64,
    request_cost_bytes: u64,
    projection_break_even_bytes: Option<u64>,
    max_fetch_run_bytes: u64,
    cache_max_bytes: u64,
}

impl LogsMetadataWarm {
    /// `cache_max_bytes` is the read cache's RAM tier budget
    /// (`--cache-max-bytes`).
    #[must_use]
    pub fn new(engine: &EngineConfig, cache_max_bytes: u64) -> Self {
        Self {
            block_range_threshold: engine.logs_block_range_threshold,
            request_cost_bytes: engine.logs_request_cost_bytes,
            projection_break_even_bytes: engine.logs_projection_break_even_bytes,
            max_fetch_run_bytes: engine.logs_max_fetch_run_bytes,
            cache_max_bytes,
        }
    }
}

/// Overall wall-clock budget for the whole pass, across every tenant and
/// signal and every step. A single deadline over the whole pass, not a
/// per-fetch one: a store with many tenants must not warm zero of them just
/// because the first several were slow. Part resolution, the whole-part warm
/// and the logs metadata pass run concurrently, all under this one deadline.
pub(crate) const WARM_DEADLINE: Duration = Duration::from_secs(10);

/// How many ingest hours before the tenant's own latest ingest hour the warm
/// set reaches back: the parts filed in `[latest - WARM_LOOKBACK_HOURS,
/// latest]`, whatever their event times. Measured from the latest ingest hour,
/// not wall-clock `now` (issue #1233, ADR-0046 amendment 2026-09-05), and in
/// ingest hours, not event time (issue #4257). Wide enough to find a recent
/// part for a slow ingest cadence without resolving a tenant's entire history.
const WARM_LOOKBACK_HOURS: u32 = 24;

/// One step of the pass, as a bit in [`WarmProgress::running`] and as the
/// `step` value the deadline warning reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WarmStep {
    TenantDiscovery,
    PartResolution,
    WholePartWarm,
    MetadataPass,
}

impl WarmStep {
    const ALL: [WarmStep; 4] = [
        WarmStep::TenantDiscovery,
        WarmStep::PartResolution,
        WarmStep::WholePartWarm,
        WarmStep::MetadataPass,
    ];

    fn bit(self) -> u8 {
        match self {
            WarmStep::TenantDiscovery => 1,
            WarmStep::PartResolution => 2,
            WarmStep::WholePartWarm => 4,
            WarmStep::MetadataPass => 8,
        }
    }

    fn name(self) -> &'static str {
        match self {
            WarmStep::TenantDiscovery => "tenant_discovery",
            WarmStep::PartResolution => "part_resolution",
            WarmStep::WholePartWarm => "whole_part_warm",
            WarmStep::MetadataPass => "metadata_pass",
        }
    }
}

/// What the pass has warmed so far and which steps are running, updated as
/// each fetch completes, so the deadline warning can report a pass the
/// timeout dropped mid-step.
#[derive(Debug, Default)]
struct WarmProgress {
    parts_warmed: AtomicU64,
    /// Logs parts the whole-part step skipped for being above the query path's
    /// `plan_whole_object_bound` (#4307): the query path reads only their
    /// directories, which the metadata pass warms, so a whole read here would
    /// fetch and decode blocks no plan needs. Not a warm and not over budget.
    parts_above_plan_bound: AtomicU64,
    /// Parts at or under the plan bound dropped by the per-(tenant, signal)
    /// cap (#4307): the cap takes only [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] of
    /// the most recent candidates, and a logs candidate beyond it is warmed by
    /// neither step (the metadata pass reads only parts above the bound). Not a
    /// warm, not over budget, not above the bound; counted here so whole-part
    /// plus metadata objects reconcile against a tenant's object count.
    parts_beyond_cap: AtomicU64,
    metadata_objects_warmed: AtomicU64,
    metadata_bytes_warmed: AtomicU64,
    /// The bytes the whole-part warm holds against its share of the cache:
    /// each in-flight fetch's reservation ([`whole_part_reservation`]), then
    /// each completed fetch's bytes read from the store.
    whole_part_bytes_reserved: AtomicU64,
    /// The objects the metadata pass reads at once, each holding one GET
    /// permit; 0 when the pass does not run.
    metadata_get_permits: AtomicU64,
    running: AtomicU8,
}

impl WarmProgress {
    fn begin(&self, step: WarmStep) {
        self.running.fetch_or(step.bit(), Ordering::Relaxed);
    }

    fn end(&self, step: WarmStep) {
        self.running.fetch_and(!step.bit(), Ordering::Relaxed);
    }

    fn part_warmed(&self) {
        self.parts_warmed.fetch_add(1, Ordering::Relaxed);
    }

    fn part_above_plan_bound(&self) {
        self.parts_above_plan_bound.fetch_add(1, Ordering::Relaxed);
    }

    fn parts_beyond_cap_add(&self, n: u64) {
        self.parts_beyond_cap.fetch_add(n, Ordering::Relaxed);
    }

    fn metadata_object_warmed(&self, bytes: u64) {
        self.metadata_objects_warmed.fetch_add(1, Ordering::Relaxed);
        self.metadata_bytes_warmed
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Reserves `bytes` for one whole-part fetch, unless the reservation would
    /// take the whole-part warm past `limit`.
    fn reserve_whole_part(&self, limit: u64, bytes: u64) -> bool {
        self.whole_part_bytes_reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                reserved.checked_add(bytes).filter(|after| *after <= limit)
            })
            .is_ok()
    }

    /// Replaces a completed fetch's `reserved` bytes with the bytes it read
    /// from the store, which is what it added to the cache.
    fn settle_whole_part(&self, reserved: u64, fetched: u64) {
        let _ = self.whole_part_bytes_reserved.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |held| Some(held.saturating_sub(reserved).saturating_add(fetched)),
        );
    }

    /// The running steps' names, comma-separated in pass order; `"none"`
    /// when no step is running.
    fn running_steps(&self) -> String {
        let running = self.running.load(Ordering::Relaxed);
        let names: Vec<&str> = WarmStep::ALL
            .into_iter()
            .filter(|step| running & step.bit() != 0)
            .map(WarmStep::name)
            .collect();
        if names.is_empty() {
            "none".to_string()
        } else {
            names.join(",")
        }
    }
}

fn elapsed_ms(since: tokio::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Discovers every tenant storage holds data for and warms the read cache
/// with each one's most recent metric and log parts. Returns nothing: this
/// is a best-effort side effect, called for its effect on `cache`, not for
/// any result a caller needs to branch on. `cache` must already be the
/// instance attached to the real query fetchers (`with_cache`); a cache
/// warmed here and never consulted elsewhere warms nothing that matters.
#[allow(clippy::too_many_arguments)]
pub async fn warm_cache(
    store: Arc<dyn ObjectStoreBackend>,
    catalog: Arc<Catalog>,
    cache: ReadCache,
    clock: &dyn Clock,
    get_limiter: Arc<ravel_query::GetLimiter>,
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    read_gate: Arc<ravel_cpu_gate::ReadGate>,
    logs_metadata: LogsMetadataWarm,
) {
    warm_cache_within(
        WARM_DEADLINE,
        store,
        catalog,
        cache,
        clock,
        get_limiter,
        memory_budget,
        read_gate,
        logs_metadata,
    )
    .await;
}

/// [`warm_cache`] under `deadline` instead of [`WARM_DEADLINE`].
#[allow(clippy::too_many_arguments)]
async fn warm_cache_within(
    deadline: Duration,
    store: Arc<dyn ObjectStoreBackend>,
    catalog: Arc<Catalog>,
    cache: ReadCache,
    clock: &dyn Clock,
    get_limiter: Arc<ravel_query::GetLimiter>,
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    read_gate: Arc<ravel_cpu_gate::ReadGate>,
    logs_metadata: LogsMetadataWarm,
) {
    let now_ns = clock.now_ns();
    let progress = WarmProgress::default();
    let outcome = tokio::time::timeout(
        deadline,
        warm_cache_inner(
            store,
            catalog,
            cache,
            now_ns,
            get_limiter,
            memory_budget,
            read_gate,
            logs_metadata,
            &progress,
        ),
    )
    .await;
    if outcome.is_err() {
        let step = progress.running_steps();
        tracing::warn!(
            deadline_secs = deadline.as_secs(),
            parts_warmed = progress.parts_warmed.load(Ordering::Relaxed),
            parts_above_plan_bound = progress.parts_above_plan_bound.load(Ordering::Relaxed),
            parts_beyond_cap = progress.parts_beyond_cap.load(Ordering::Relaxed),
            metadata_objects_warmed = progress.metadata_objects_warmed.load(Ordering::Relaxed),
            metadata_bytes_warmed = progress.metadata_bytes_warmed.load(Ordering::Relaxed),
            metadata_get_permits = progress.metadata_get_permits.load(Ordering::Relaxed),
            whole_part_bytes_reserved = progress.whole_part_bytes_reserved.load(Ordering::Relaxed),
            step = step.as_str(),
            "cache warmup did not finish before its deadline; continuing to readiness with \
             whatever it warmed so far"
        );
    }
}

/// One tenant's resolved warm set, per signal.
struct TenantWarmSet {
    tenant: TenantHash,
    metrics: (Vec<SegmentRef>, LatestHour),
    logs: (Vec<SegmentRef>, LatestHour),
}

#[allow(clippy::too_many_arguments)]
async fn warm_cache_inner(
    store: Arc<dyn ObjectStoreBackend>,
    catalog: Arc<Catalog>,
    cache: ReadCache,
    now_ns: i64,
    get_limiter: Arc<ravel_query::GetLimiter>,
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    read_gate: Arc<ravel_cpu_gate::ReadGate>,
    logs_metadata: LogsMetadataWarm,
    progress: &WarmProgress,
) {
    let started = tokio::time::Instant::now();
    progress.begin(WarmStep::TenantDiscovery);
    let tenants = match ravel_maintain::discover_tenants(store.as_ref()).await {
        Ok(tenants) => tenants,
        Err(err) => {
            tracing::warn!(error = %err, "cache warmup: tenant discovery failed; skipping warmup");
            progress.end(WarmStep::TenantDiscovery);
            return;
        }
    };
    progress.end(WarmStep::TenantDiscovery);
    let tenant_discovery_elapsed_ms = elapsed_ms(started);

    // ADR-1195: shares the process-wide `GetLimiter` rather than a private
    // pool, same as every other fetcher this process constructs.
    // Issue #1255: a warm fetch decodes the part it reads into a buffer the
    // pass holds until the fetch returns, so it reserves against the same
    // process-wide `MemoryBudget` a query's fetch does.
    // ADR-1702 decision 4: a warm fetch decodes the catalogs it reads, on the
    // same read gate a query's fetch uses.
    let metrics_fetcher = SegmentFetcher::new(store.clone())
        .with_cache(cache.clone())
        .with_get_limiter(get_limiter.clone())
        .with_memory_budget(memory_budget.clone())
        .with_read_gate(read_gate.clone());
    // #4307: the whole-part warm reads a logs part the way the query path does,
    // whole at or under `plan_whole_object_bound` and not at all above it (the
    // query path reads only the directories the metadata pass warms there).
    // Build this fetcher with the same block-range threshold, request cost and
    // projection break-even the metadata pass uses, so both agree on that bound
    // and `warm_log_parts` can partition the warm set against it.
    let logs_fetcher = LogSegmentFetcher::new(store.clone())
        .with_block_range_threshold(logs_metadata.block_range_threshold)
        .with_request_cost_bytes(logs_metadata.request_cost_bytes)
        .with_projection_break_even_bytes(logs_metadata.projection_break_even_bytes)
        .with_cache(cache.clone())
        .with_get_limiter(get_limiter.clone())
        .with_memory_budget(memory_budget.clone())
        .with_read_gate(read_gate.clone());
    let mut metadata_pass = LogsMetadataPass::new(
        store,
        cache,
        get_limiter.clone(),
        memory_budget,
        read_gate,
        logs_metadata,
    );
    // The whole-part warm and the metadata pass insert beside each other, so
    // each holds to its own share and together they never pass the cache's
    // budget: an eviction would drop entries the pass has already counted.
    let whole_part_budget = logs_metadata
        .cache_max_bytes
        .saturating_sub(metadata_pass.as_ref().map_or(0, |pass| pass.ceiling));
    if let Some(pass) = metadata_pass.as_ref() {
        progress
            .metadata_get_permits
            .store(pass.concurrency as u64, Ordering::Relaxed);
    }

    let (whole_part_tx, mut whole_part_rx) = futures::channel::mpsc::unbounded();
    let (metadata_tx, mut metadata_rx) = futures::channel::mpsc::unbounded();
    let metadata_tx = metadata_pass.is_some().then_some(metadata_tx);
    let resolution = async move {
        let step_started = tokio::time::Instant::now();
        progress.begin(WarmStep::PartResolution);
        let catalog = &catalog;
        let get_limiter = &get_limiter;
        let mut resolved = futures::stream::iter(tenants)
            .map(|tenant| async move {
                // The limiter is never closed (see `GetLimiterClosed`); were
                // it, resolution would run unthrottled rather than not at all.
                let _permit = get_limiter.acquire().await.ok();
                Arc::new(TenantWarmSet {
                    tenant,
                    metrics: most_recent_parts(catalog, tenant, Signal::Metrics, now_ns).await,
                    logs: most_recent_parts(catalog, tenant, Signal::Logs, now_ns).await,
                })
            })
            .buffered(PART_RESOLUTIONS_IN_FLIGHT);
        while let Some(set) = resolved.next().await {
            if let Some(tx) = metadata_tx.as_ref() {
                let _ = tx.unbounded_send(set.clone());
            }
            let _ = whole_part_tx.unbounded_send(set);
        }
        progress.end(WarmStep::PartResolution);
        elapsed_ms(step_started)
    };

    let whole_part_warm = async {
        let step_started = tokio::time::Instant::now();
        progress.begin(WarmStep::WholePartWarm);
        let mut warmed = 0usize;
        while let Some(set) = whole_part_rx.next().await {
            let (metrics, logs) = futures::join!(
                warm_metric_parts(
                    &metrics_fetcher,
                    set.tenant,
                    &set.metrics.0,
                    &set.metrics.1,
                    whole_part_budget,
                    progress
                ),
                warm_log_parts(
                    &logs_fetcher,
                    set.tenant,
                    &set.logs.0,
                    &set.logs.1,
                    whole_part_budget,
                    progress
                ),
            );
            warmed += metrics + logs;
        }
        progress.end(WarmStep::WholePartWarm);
        (warmed, elapsed_ms(step_started))
    };
    let metadata = async {
        let step_started = tokio::time::Instant::now();
        let Some(pass) = metadata_pass.as_mut() else {
            return 0;
        };
        progress.begin(WarmStep::MetadataPass);
        while let Some(set) = metadata_rx.next().await {
            let planned: Vec<SegmentRef> = set
                .logs
                .0
                .iter()
                .filter(|part| pass.fetcher.plans_segment(part))
                .cloned()
                .collect();
            if planned.is_empty() {
                continue;
            }
            let warmed = pass.warm(set.tenant, &planned, progress).await;
            tracing::info!(
                tenant_hash = %set.tenant.to_hex(),
                signal = ?Signal::Logs,
                metadata_objects_warmed = warmed.objects as u64,
                metadata_bytes_warmed = warmed.bytes,
                "cache warmup: warmed tenant logs metadata"
            );
        }
        progress.end(WarmStep::MetadataPass);
        elapsed_ms(step_started)
    };
    let (
        part_resolution_elapsed_ms,
        (total_parts_warmed, whole_part_warm_elapsed_ms),
        metadata_pass_elapsed_ms,
    ) = futures::join!(resolution, whole_part_warm, metadata);

    let metadata = metadata_pass
        .as_ref()
        .map_or_else(MetadataWarmed::default, |pass| pass.total);
    tracing::info!(
        parts_warmed = total_parts_warmed,
        parts_above_plan_bound = progress.parts_above_plan_bound.load(Ordering::Relaxed),
        parts_beyond_cap = progress.parts_beyond_cap.load(Ordering::Relaxed),
        metadata_objects_warmed = metadata.objects,
        metadata_bytes_warmed = metadata.bytes,
        metadata_budget_reached = metadata_pass
            .as_ref()
            .is_some_and(|pass| pass.budget_reached),
        metadata_get_permits = progress.metadata_get_permits.load(Ordering::Relaxed),
        whole_part_bytes_reserved = progress.whole_part_bytes_reserved.load(Ordering::Relaxed),
        tenant_discovery_elapsed_ms,
        part_resolution_elapsed_ms,
        whole_part_warm_elapsed_ms,
        metadata_pass_elapsed_ms,
        elapsed_ms = elapsed_ms(started),
        "cache warmup: pass complete"
    );

    // #4307: when the metadata pass is absent (an invalid fetch bound made
    // `LogsMetadataPass::new` return `None`), a logs part above the plan bound
    // is warmed by neither step: the whole-part warm skips it and no pass warms
    // its directories. Those tenants start fully cold on logs. Warn once per
    // pass so the gap is visible, since each per-(tenant, signal) line logs the
    // skip at `info!` (an above-bound part is normally warmed by the pass).
    if metadata_pass.is_none() {
        let above = progress.parts_above_plan_bound.load(Ordering::Relaxed);
        if above > 0 {
            tracing::warn!(
                parts_above_plan_bound = above,
                "cache warmup: logs metadata pass absent (invalid fetch bound); logs parts above \
                 the plan bound were left unwarmed, so those tenants start cold on logs"
            );
        }
    }
}

/// What the logs metadata pass warmed: objects whose footer and four
/// directories it read into the cache, and the wire bytes it fetched for them
/// (cache hits fetch none).
#[derive(Clone, Copy, Debug, Default)]
struct MetadataWarmed {
    objects: usize,
    bytes: u64,
}

/// The logs metadata pass's fetcher and budget, kept across tenants: the
/// budget is the one read cache every tenant shares.
struct LogsMetadataPass {
    fetcher: LogSegmentFetcher,
    ceiling: u64,
    /// The bytes the pass's completed reads fetched from the store, failed
    /// ones included: what the pass has added to the cache, charged against
    /// `ceiling`.
    charged: u64,
    concurrency: usize,
    budget_reached: bool,
    total: MetadataWarmed,
}

impl LogsMetadataPass {
    /// `None`, with a warning, when the query path's fetch bound is one the
    /// fetcher refuses: the pass is skipped, never startup. With the pass
    /// absent, a logs part above the plan bound is warmed by neither step (the
    /// whole-part warm still skips it), so those tenants start cold on logs;
    /// `warm_cache_inner` logs that at `warn!` once per pass when any
    /// above-bound part was seen (#4307).
    fn new(
        store: Arc<dyn ObjectStoreBackend>,
        cache: ReadCache,
        get_limiter: Arc<ravel_query::GetLimiter>,
        memory_budget: Arc<ravel_memory::MemoryBudget>,
        read_gate: Arc<ravel_cpu_gate::ReadGate>,
        shape: LogsMetadataWarm,
    ) -> Option<Self> {
        // One object fewer than the limiter's permits, leaving room for the
        // whole-part warm that runs beside this pass.
        let concurrency = get_limiter.permits().saturating_sub(1).max(1);
        // Built like the query path's logs fetchers (`QueryEngine::new` and
        // `build_sql_state`): the request cost sets the coalescing gap and the
        // two thresholds set the plan bound, so any other value here would read
        // the directories under keys the query path does not look up.
        let mut fetcher = LogSegmentFetcher::new(store)
            .with_block_range_threshold(shape.block_range_threshold)
            .with_request_cost_bytes(shape.request_cost_bytes)
            .with_projection_break_even_bytes(shape.projection_break_even_bytes)
            .with_get_limiter(get_limiter)
            .with_memory_budget(memory_budget)
            .with_read_gate(read_gate)
            .with_cache(cache);
        if shape.max_fetch_run_bytes != 0 {
            fetcher = match fetcher.with_max_fetch_run_bytes(shape.max_fetch_run_bytes) {
                Ok(fetcher) => fetcher,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "cache warmup: invalid logs fetch bound; skipping the logs metadata pass"
                    );
                    return None;
                }
            };
        }
        Some(Self {
            fetcher,
            ceiling: shape.cache_max_bytes / METADATA_CACHE_SHARE_DIVISOR,
            charged: 0,
            concurrency,
            budget_reached: false,
            total: MetadataWarmed::default(),
        })
    }

    /// Reads the footer and four directories of every part in `planned`, which
    /// holds only parts the query path plans from their directories, most
    /// recent first, up to `concurrency` objects at once. An object is
    /// admitted while the bytes the pass has charged, the in-flight objects'
    /// probe lengths and its own probe length stay within the ceiling. One
    /// that does not fit waits for the reads in flight, which can come back
    /// as cache hits that charge nothing; one that does not fit beside the
    /// completed reads alone ends the pass for every later tenant too. A
    /// failed read skips that object. Each warmed object is added to
    /// `progress` as its read completes.
    async fn warm(
        &mut self,
        tenant: TenantHash,
        planned: &[SegmentRef],
        progress: &WarmProgress,
    ) -> MetadataWarmed {
        let mut warmed = MetadataWarmed::default();
        let fetcher = &self.fetcher;
        let mut pending = planned.iter().peekable();
        let mut in_flight = FuturesUnordered::new();
        let mut in_flight_bytes: u64 = 0;
        loop {
            while !self.budget_reached && in_flight.len() < self.concurrency {
                let Some(&part) = pending.peek() else {
                    break;
                };
                let estimate = ravel_query::derive_suffix_len(part.object_size)
                    .min(part.object_size)
                    .max(1);
                let charged = self
                    .charged
                    .saturating_add(in_flight_bytes)
                    .saturating_add(estimate);
                if charged > self.ceiling {
                    self.budget_reached = in_flight.is_empty();
                    break;
                }
                pending.next();
                in_flight_bytes = in_flight_bytes.saturating_add(estimate);
                in_flight.push(async move {
                    let accounting = warm_accounting();
                    let result = fetcher
                        .block_range_fetcher()
                        .fetch_plan_directories(part, tenant, &accounting)
                        .await;
                    let fetched = accounting.snapshot().total_s3_bytes();
                    (part, estimate, result.map(|_| ()), fetched)
                });
            }
            let Some((part, estimate, result, fetched)) = in_flight.next().await else {
                break;
            };
            in_flight_bytes = in_flight_bytes.saturating_sub(estimate);
            self.charged = self.charged.saturating_add(fetched);
            match result {
                Ok(()) => {
                    warmed.objects += 1;
                    warmed.bytes = warmed.bytes.saturating_add(fetched);
                    progress.metadata_object_warmed(fetched);
                }
                Err(err) => {
                    tracing::debug!(
                        tenant_hash = %tenant.to_hex(),
                        key = %part.data_object_key,
                        error = %err,
                        "cache warmup: log part metadata read failed; skipping"
                    );
                }
            }
        }
        self.total.objects += warmed.objects;
        self.total.bytes = self.total.bytes.saturating_add(warmed.bytes);
        warmed
    }
}

/// What [`most_recent_parts`] learned about a (tenant, signal)'s latest
/// ingest, distinguishing "never ingested this signal" from "the listing
/// that would tell us failed" (issue #1233 finding 2): the two used to share
/// a single `None`/`Ok(None)`-shaped return, so a mid-listing store fault
/// was silently reported to operators identically to a signal the tenant
/// never uses. They now log differently and must stay distinguishable
/// through every caller.
enum LatestHour {
    /// The tenant has no commit-record parts for this signal within
    /// [`Catalog::LATEST_HOUR_MAX_LOOKBACK_HOURS`]
    /// (`latest_ingest_hour` returned `Ok(None)`). A normal outcome: logs at
    /// `info!`.
    None,
    /// `latest_ingest_hour` itself returned `Err`: a store fault mid-listing,
    /// not evidence the tenant has no parts. Not a normal outcome: logged at
    /// `warn!` where it is discovered, inside [`most_recent_parts`].
    Error,
    /// The newest ingest-hour bucket found for this signal, and whether that
    /// hour's listing carried at least one non-tombstone entry
    /// (`has_ingest_part`). When it did not -- the anchor hour is
    /// tombstone-only -- the catalog drops that bucket outright, so an empty
    /// resolve for the warm window is expected, not evidence of a fetch
    /// failure; [`log_warm_result`] uses this to avoid logging that ordinary
    /// case as a `warn!`.
    Found { hour: u32, has_ingest_part: bool },
}

/// Every part of one (tenant, signal) filed in the ingest hours
/// `[latest - WARM_LOOKBACK_HOURS, latest]`, whatever their event times
/// ([`Catalog::resolve_ingest_hours`]), newest ingest hour first and, within
/// an hour, greatest `max_event_ts_ns` first (callers bound the whole-part
/// warm to [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] of them), alongside what was
/// learned about the
/// tenant's latest ingest hour ([`LatestHour`]). A resolve error still
/// reports [`LatestHour::Found`] alongside empty parts, so it can be told
/// apart from "no parts for this signal at all" ([`LatestHour::None`]) or a
/// `latest_ingest_hour` listing failure ([`LatestHour::Error`]). Every
/// failure -- `latest_ingest_hour` erroring or resolve erroring -- warms
/// nothing for this (tenant, signal); `latest_ingest_hour` erroring also
/// warns immediately, here, since the caller only sees [`LatestHour::Error`]
/// and must not log it again as a plain "nothing to warm".
async fn most_recent_parts(
    catalog: &Catalog,
    tenant: TenantHash,
    signal: Signal,
    now_ns: i64,
) -> (Vec<SegmentRef>, LatestHour) {
    // A fresh, discarded accounting handle: this pass is a best-effort
    // optimization outside the query-cost-accounting surface (ADR-0044), the
    // same convention `warm_logs` already uses for its own fetch below.
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let (latest_hour, has_ingest_part) = match catalog
        .latest_ingest_hour(&tenant, signal, now_ns, &accounting)
        .await
    {
        Ok(Some((hour, has_ingest_part))) => (hour, has_ingest_part),
        Ok(None) => return (Vec::new(), LatestHour::None),
        Err(err) => {
            tracing::warn!(
                tenant_hash = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                latest_ingest_hour = "error",
                "cache warmup: latest_ingest_hour failed; skipping this tenant and signal"
            );
            return (Vec::new(), LatestHour::Error);
        }
    };

    let snapshot = match catalog
        .resolve_ingest_hours(
            &tenant,
            signal,
            latest_hour.saturating_sub(WARM_LOOKBACK_HOURS),
            latest_hour,
            now_ns,
            &accounting,
        )
        .await
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::debug!(
                tenant_hash = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "cache warmup: resolve failed; skipping this tenant and signal"
            );
            return (
                Vec::new(),
                LatestHour::Found {
                    hour: latest_hour,
                    has_ingest_part,
                },
            );
        }
    };
    let mut segments = snapshot.segments;
    segments.sort_by_key(|s| std::cmp::Reverse((s.ingest_hour_bucket, s.max_event_ts_ns)));
    (
        segments,
        LatestHour::Found {
            hour: latest_hour,
            has_ingest_part,
        },
    )
}

/// One `info!` per (tenant, signal) after warming, or a `warn!` instead when
/// the signal's at-or-under-bound candidate set is non-empty, none of its parts
/// warmed, and the reason is not an expected skip: that combination means a
/// resolve or fetch failure ate the candidates, not that there was nothing to
/// warm. When the latest hour is anchored by a tombstone- or rewrite-only
/// bucket with no ingest part of its own (`has_ingest_part = false`), an empty
/// warm is the expected outcome (the catalog drops a tombstoned bucket
/// outright), so this logs at `info!` instead of `warn!` for that case. A
/// signal whose parts were skipped because they did not fit the whole-part
/// warm's cache share (`parts_over_budget` above zero) or because the
/// per-(tenant, signal) cap dropped them (`parts_beyond_cap` above zero) logs
/// at `info!` too: nothing failed. A logs signal whose parts were all above the
/// plan bound (`whole_candidates == 0`) logs at `info!` too: those parts are
/// warmed by the metadata pass, not here, so none warming whole is expected,
/// and an above-bound part never masks a genuine all-candidates-failed warning
/// (#4307).
fn log_warm_result(
    tenant: TenantHash,
    signal: Signal,
    parts: WholePartsWarmed,
    latest_hour: Option<(u32, bool)>,
) {
    let latest_ingest_hour =
        latest_hour.map_or_else(|| "none".to_string(), |(hour, _)| hour.to_string());
    let has_ingest_part = latest_hour.is_some_and(|(_, has_ingest_part)| has_ingest_part);
    // The `warn!` keys on the at-or-under-bound candidate set, not on the whole
    // signal (#4307): candidates existed, none warmed, and the reason was not
    // an expected skip (budget or cap). An above-bound part must not mask this
    // -- one above the bound with every at-or-under fetch failed is still a
    // failure that ate the candidates. A signal whose parts are all above the
    // bound (`whole_candidates == 0`) warms whole nothing by design and stays
    // at `info!`.
    let candidates_existed_but_none_warmed = parts.whole_candidates > 0
        && parts.warmed == 0
        && parts.over_budget == 0
        && parts.beyond_cap == 0;
    if candidates_existed_but_none_warmed && has_ingest_part {
        tracing::warn!(
            tenant_hash = %tenant.to_hex(),
            signal = ?signal,
            parts_warmed = parts.warmed,
            parts_over_budget = parts.over_budget,
            parts_above_plan_bound = parts.above_plan_bound,
            parts_beyond_cap = parts.beyond_cap,
            latest_ingest_hour,
            "cache warmup: tenant has parts for this signal but none were warmed"
        );
    } else {
        tracing::info!(
            tenant_hash = %tenant.to_hex(),
            signal = ?signal,
            parts_warmed = parts.warmed,
            parts_over_budget = parts.over_budget,
            parts_above_plan_bound = parts.above_plan_bound,
            parts_beyond_cap = parts.beyond_cap,
            latest_ingest_hour,
            "cache warmup: warmed tenant signal"
        );
    }
}

/// A fresh, discarded accounting handle for one warm fetch, tagged
/// interactive: warming fills the cache with the newest parts so the next
/// query's reads hit, which is the interactive working set, not a bulk pass.
fn warm_accounting() -> ravel_types::accounting::QueryAccounting {
    let accounting = ravel_types::accounting::QueryAccounting::new();
    accounting.set_access_intent(ravel_types::accounting::AccessIntent::Interactive);
    accounting
}

/// Resolves and warms one tenant's metric parts: [`most_recent_parts`] then
/// [`warm_metric_parts`], unbudgeted.
#[cfg(test)]
async fn warm_metrics(
    catalog: &Catalog,
    fetcher: &SegmentFetcher,
    tenant: TenantHash,
    now_ns: i64,
) -> usize {
    let (parts, latest_hour) = most_recent_parts(catalog, tenant, Signal::Metrics, now_ns).await;
    warm_metric_parts(
        fetcher,
        tenant,
        &parts,
        &latest_hour,
        u64::MAX,
        &WarmProgress::default(),
    )
    .await
}

/// Resolves and warms one tenant's logs parts: [`most_recent_parts`] then
/// [`warm_log_parts`]. Returns the count warmed whole and every part in the
/// warm set.
#[cfg(test)]
async fn warm_logs(
    catalog: &Catalog,
    fetcher: &LogSegmentFetcher,
    tenant: TenantHash,
    now_ns: i64,
) -> (usize, Vec<SegmentRef>) {
    let (parts, latest_hour) = most_recent_parts(catalog, tenant, Signal::Logs, now_ns).await;
    let warmed = warm_log_parts(
        fetcher,
        tenant,
        &parts,
        &latest_hour,
        u64::MAX,
        &WarmProgress::default(),
    )
    .await;
    (warmed, parts)
}

/// What the whole-part warm did with one (tenant, signal)'s parts.
#[derive(Clone, Copy, Debug, Default)]
struct WholePartsWarmed {
    /// Parts whose fetch succeeded.
    warmed: usize,
    /// Parts skipped because their reservation did not fit what was left of
    /// the whole-part warm's cache share with none of the signal's fetches in
    /// flight.
    over_budget: usize,
    /// Logs parts skipped for being above the query path's
    /// `plan_whole_object_bound` (#4307), so the whole-part step reads none of
    /// their blocks; the metadata pass warms their directories instead. Always
    /// 0 for metrics, whose large parts read only a footer tail rather than
    /// being skipped.
    above_plan_bound: usize,
    /// Candidate parts (at or under the bound, for logs) dropped by the
    /// [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] cap (#4307): the cap keeps only the
    /// most recent candidates, and a logs candidate beyond it is warmed by
    /// neither step. Counted so the object count reconciles.
    beyond_cap: usize,
    /// The candidates that entered the whole-part warm: for logs the parts at
    /// or under the bound, for metrics every resolved part. `warmed +
    /// over_budget + beyond_cap` plus fetch failures. Used to tell "candidates
    /// existed but none warmed" (a fetch failure, worth a `warn!`) from "no
    /// candidates at all" (every part was above the bound, an expected
    /// `info!`).
    whole_candidates: usize,
}

/// The fetcher one signal's whole-part warm reads through.
#[derive(Clone, Copy)]
enum WholePartFetcher<'a> {
    Metrics(&'a SegmentFetcher),
    Logs(&'a LogSegmentFetcher),
}

impl WholePartFetcher<'_> {
    /// Reads `part` whole through the query path's funnel for its signal and
    /// returns whether the read succeeded and the bytes it read from the store.
    async fn fetch(self, tenant: TenantHash, part: &SegmentRef) -> (bool, u64) {
        let accounting = warm_accounting();
        let (error, message) = match self {
            WholePartFetcher::Metrics(fetcher) => (
                fetcher
                    .fetch_series_accounted(tenant, part, &[], &accounting)
                    .await
                    .err()
                    .map(|err| err.to_string()),
                "cache warmup: metric part fetch failed; skipping",
            ),
            WholePartFetcher::Logs(fetcher) => {
                let query = LogQuery::new(part.min_event_ts_ns, part.max_event_ts_ns);
                (
                    fetcher
                        .fetch_accounted_with_tenant(part, tenant, &query, &accounting)
                        .await
                        .err()
                        .map(|err| err.to_string()),
                    "cache warmup: log part fetch failed; skipping",
                )
            }
        };
        if let Some(error) = &error {
            tracing::debug!(
                tenant_hash = %tenant.to_hex(),
                key = %part.data_object_key,
                error = %error,
                "{message}"
            );
        }
        (error.is_none(), accounting.snapshot().total_s3_bytes())
    }
}

/// The bytes `fetcher`'s read of `part` takes from the store on a cache miss,
/// reserved against the whole-part warm's share before the fetch starts. The
/// logs funnel reads the object whole. The metrics funnel, called with no
/// matchers, reads the footer and catalog sections: its first GET is the
/// whole object at or below [`DEFAULT_WHOLE_OBJECT_THRESHOLD`] and otherwise
/// a [`DEFAULT_SUFFIX_LEN`] footer tail, and a catalog section outside that
/// tail is read after it, past the reservation.
fn whole_part_reservation(fetcher: WholePartFetcher<'_>, part: &SegmentRef) -> u64 {
    match fetcher {
        WholePartFetcher::Logs(_) => part.object_size,
        WholePartFetcher::Metrics(_) if part.object_size == 0 => DEFAULT_SUFFIX_LEN,
        WholePartFetcher::Metrics(_) if part.object_size <= DEFAULT_WHOLE_OBJECT_THRESHOLD => {
            part.object_size
        }
        WholePartFetcher::Metrics(_) => DEFAULT_SUFFIX_LEN.min(part.object_size),
    }
}

/// Warms the [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] most recent of `parts`, at
/// most [`WHOLE_PARTS_IN_FLIGHT_PER_SIGNAL`] at once. Each part reserves what
/// its fetch reads ([`whole_part_reservation`]) against `budget` before the
/// fetch starts. A part that does not fit waits for the next fetch in flight
/// to complete, whose reservation is then replaced by the bytes it read from
/// the store (none on a cache hit), and is tested again; one that does not
/// fit with no fetch of its own in flight is skipped.
async fn warm_whole_parts(
    fetcher: WholePartFetcher<'_>,
    tenant: TenantHash,
    parts: &[SegmentRef],
    budget: u64,
    progress: &WarmProgress,
) -> WholePartsWarmed {
    let beyond_cap = parts.len().saturating_sub(MAX_PARTS_PER_TENANT_PER_SIGNAL);
    progress.parts_beyond_cap_add(beyond_cap as u64);
    let mut outcome = WholePartsWarmed {
        whole_candidates: parts.len(),
        beyond_cap,
        ..WholePartsWarmed::default()
    };
    let mut pending = parts
        .iter()
        .take(MAX_PARTS_PER_TENANT_PER_SIGNAL)
        .peekable();
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < WHOLE_PARTS_IN_FLIGHT_PER_SIGNAL {
            let Some(&part) = pending.peek() else {
                break;
            };
            let reservation = whole_part_reservation(fetcher, part);
            if !progress.reserve_whole_part(budget, reservation) {
                if !in_flight.is_empty() {
                    break;
                }
                tracing::debug!(
                    tenant_hash = %tenant.to_hex(),
                    key = %part.data_object_key,
                    reservation,
                    "cache warmup: whole part does not fit the whole-part cache share; skipping"
                );
                outcome.over_budget += 1;
                pending.next();
                continue;
            }
            pending.next();
            in_flight.push(async move {
                let (ok, fetched) = fetcher.fetch(tenant, part).await;
                (reservation, ok, fetched)
            });
        }
        let Some((reserved, ok, fetched)) = in_flight.next().await else {
            break;
        };
        progress.settle_whole_part(reserved, fetched);
        if ok {
            progress.part_warmed();
            outcome.warmed += 1;
        }
    }
    outcome
}

/// Warms a tenant's resolved metric `parts` through [`warm_whole_parts`],
/// each GET through the fetcher's GET limiter, and returns the count warmed.
async fn warm_metric_parts(
    fetcher: &SegmentFetcher,
    tenant: TenantHash,
    parts: &[SegmentRef],
    latest_hour: &LatestHour,
    budget: u64,
    progress: &WarmProgress,
) -> usize {
    let (hour, has_ingest_part) = match *latest_hour {
        LatestHour::Error => return 0,
        LatestHour::None => {
            log_warm_result(tenant, Signal::Metrics, WholePartsWarmed::default(), None);
            return 0;
        }
        LatestHour::Found {
            hour,
            has_ingest_part,
        } => (hour, has_ingest_part),
    };

    let warmed = warm_whole_parts(
        WholePartFetcher::Metrics(fetcher),
        tenant,
        parts,
        budget,
        progress,
    )
    .await;

    log_warm_result(
        tenant,
        Signal::Metrics,
        warmed,
        Some((hour, has_ingest_part)),
    );
    warmed.warmed
}

/// Warms a tenant's resolved logs `parts` whole through [`warm_whole_parts`],
/// each GET through the fetcher's GET limiter, and returns the count warmed.
///
/// #4307: a part above the query path's `plan_whole_object_bound` is read only
/// by the directories the metadata pass warms, so it is dropped from the
/// whole-part set here, before the [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] cap, and
/// counted in `parts_above_plan_bound`. The cap then takes the most recent
/// parts at or under the bound, the ones the query path opens whole. `fetcher`
/// carries the same bound the metadata pass's fetcher does (see
/// `warm_cache_inner`), so the two steps partition the warm set.
async fn warm_log_parts(
    fetcher: &LogSegmentFetcher,
    tenant: TenantHash,
    parts: &[SegmentRef],
    latest_hour: &LatestHour,
    budget: u64,
    progress: &WarmProgress,
) -> usize {
    let (hour, has_ingest_part) = match *latest_hour {
        LatestHour::Error => return 0,
        LatestHour::None => {
            log_warm_result(tenant, Signal::Logs, WholePartsWarmed::default(), None);
            return 0;
        }
        LatestHour::Found {
            hour,
            has_ingest_part,
        } => (hour, has_ingest_part),
    };

    let mut whole = Vec::with_capacity(parts.len());
    let mut above_plan_bound = 0usize;
    for part in parts {
        if fetcher.plans_segment(part) {
            above_plan_bound += 1;
            progress.part_above_plan_bound();
        } else {
            whole.push(part.clone());
        }
    }

    let mut warmed = warm_whole_parts(
        WholePartFetcher::Logs(fetcher),
        tenant,
        &whole,
        budget,
        progress,
    )
    .await;
    warmed.above_plan_bound = above_plan_bound;

    log_warm_result(tenant, Signal::Logs, warmed, Some((hour, has_ingest_part)));
    warmed.warmed
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use bytes::Bytes;
    use ravel_cache::{Cache, CacheLimits};
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, InstrumentedStore, ListPage, ObjectMeta,
        PageToken, PutOptions, PutOutcome, StoreError,
    };
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;

    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

    fn now_ns() -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch");
        let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
        (ns / NS_PER_SEC) * NS_PER_SEC
    }

    fn metadata_warm() -> LogsMetadataWarm {
        LogsMetadataWarm::new(&EngineConfig::default(), 16 * 1024 * 1024)
    }

    /// Publishes one metric segment with a single sample at `event_ts_ns`,
    /// filed under that timestamp's own ingest-hour bucket. Each call uses a
    /// fresh writer id, so publishing several segments for the same tenant
    /// (e.g. one per hour, to test [`Catalog::latest_ingest_hour`] and the
    /// warm window it drives) never collides.
    async fn publish_metric_segment_at(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
    ) {
        publish_metric_segment_filed(store, tenant_hash, event_ts_ns, event_ts_ns / NS_PER_HOUR)
            .await;
    }

    /// [`publish_metric_segment_at`] filed under `ingest_hour` (unix hours),
    /// which may be any hour at or after the sample's own.
    async fn publish_metric_segment_filed(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
        ingest_hour: i64,
    ) {
        publish_metric_segment_sized(store, tenant_hash, event_ts_ns, ingest_hour, 1).await;
    }

    /// [`publish_metric_segment_filed`] with `samples` samples 10 ms apart,
    /// ending at `event_ts_ns`, whose pseudo-random values compress poorly:
    /// a large object whose page data dwarfs its footer and catalog.
    async fn publish_metric_segment_sized(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
        ingest_hour: i64,
        samples: usize,
    ) {
        let created_unix_ns = event_ts_ns.max(ingest_hour * NS_PER_HOUR);
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let samples: Vec<Sample> = (0..samples)
            .rev()
            .map(|back| {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                Sample {
                    ts_ns: event_ts_ns - i64::try_from(back).expect("small") * 10_000_000,
                    value: f64::from_bits((seed >> 12) | 0x4000_0000_0000_0000),
                }
            })
            .collect();
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "http_requests_total".to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(&TenantId::new("acme"), "http_requests_total", &label_set)
                .expect("series id"),
            labels: label_set,
            samples,
        }];

        let writer_id = Uuid::new_v4();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket: u32::try_from(ingest_hour).expect("hour bucket"),
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    async fn publish_metric_segment(store: &MemoryStore, tenant_hash: TenantHash, now: i64) {
        publish_metric_segment_at(store, tenant_hash, now - NS_PER_MIN).await;
    }

    /// Publishes one real RLOG object (a single log record) plus its commit
    /// record, filed under `event_ts_ns`'s own ingest-hour bucket -- the same
    /// shape `fold_on_demand.rs`'s `seed_log_commit` seeds, needed here
    /// because `LogSegmentFetcher::fetch_accounted_with_tenant` decodes a
    /// real RLOG footer: a metrics-shaped segment filed under `Signal::Logs`
    /// (as `publish_metric_segment_at` builds) would fail to decode and
    /// never count as warmed.
    async fn publish_log_segment_at(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
    ) {
        publish_log_segment_hashed(store, tenant_hash, event_ts_ns, [0x5au8; 32]).await;
    }

    /// [`publish_log_segment_at`] under `content_hash`, which the read cache
    /// keys on: a tenant's parts need distinct hashes for distinct entries.
    async fn publish_log_segment_hashed(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
        content_hash: [u8; 32],
    ) {
        publish_log_segment_filed(
            store,
            tenant_hash,
            event_ts_ns,
            event_ts_ns / NS_PER_HOUR,
            content_hash,
        )
        .await;
    }

    /// [`publish_log_segment_hashed`] filed under `ingest_hour` (unix hours),
    /// which may be any hour at or after the record's own.
    async fn publish_log_segment_filed(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
        ingest_hour: i64,
        content_hash: [u8; 32],
    ) {
        publish_log_segment_sized(
            store,
            tenant_hash,
            event_ts_ns,
            ingest_hour,
            content_hash,
            0,
        )
        .await;
    }

    /// [`publish_log_segment_filed`] with `filler_bytes` of incompressible
    /// record bodies after the first record, for an object larger than its
    /// suffix probe.
    async fn publish_log_segment_sized(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        event_ts_ns: i64,
        ingest_hour: i64,
        content_hash: [u8; 32],
        filler_bytes: usize,
    ) {
        use ravel_logseg::{
            AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
        };
        use ravel_types::logstream::log_stream_id;

        let created_unix_ns = event_ts_ns.max(ingest_hour * NS_PER_HOUR);
        let shard = 0u32;
        let writer_id = Uuid::new_v4();
        let epoch = 1u64;
        let seq = 1u64;

        let resource_attrs = vec![(
            "service.name".to_string(),
            AttrValue::Str("checkout".to_string()),
        )];
        let stream_attrs = stream_attrs_bytes(&resource_attrs, "", "", &[]);
        let stream_id = log_stream_id(&resource_attrs, "", "", &[]);

        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        const FILLER_BODY_BYTES: usize = 1024;
        let mut seed = u64::from_le_bytes(content_hash[..8].try_into().expect("8 bytes")) | 1;
        let bodies = std::iter::once("checkout completed".to_string()).chain(
            (0..filler_bytes.div_ceil(FILLER_BODY_BYTES)).map(|_| {
                (0..FILLER_BODY_BYTES)
                    .map(|_| {
                        seed = seed
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407);
                        char::from(b'!' + ((seed >> 33) % 94) as u8)
                    })
                    .collect::<String>()
            }),
        );
        for body in bodies {
            writer
                .push(LogRecord {
                    stream_id,
                    stream_attrs: stream_attrs.clone(),
                    ts_ns: event_ts_ns,
                    observed_ts_ns: event_ts_ns,
                    severity_num: 9,
                    severity_text: "INFO".to_string(),
                    body,
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push log record");
        }
        let bytes = writer.finish().expect("finish RLOG object");
        let object_size = bytes.len() as u64;

        let data_key = keys::data_key(
            &tenant_hash,
            Signal::Logs,
            shard,
            writer_id,
            epoch,
            seq,
            &content_hash,
        )
        .expect("build data key");
        store
            .put(
                &data_key,
                Bytes::from(bytes),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put RLOG object");

        let commit = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
            object_size,
            content_hash,
            sample_count: 1 + filler_bytes.div_ceil(FILLER_BODY_BYTES) as u64,
            series_count: 1,
            min_event_ts_ns: event_ts_ns,
            max_event_ts_ns: event_ts_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns,
            ingest_hour_bucket: u32::try_from(ingest_hour).expect("hour bucket"),
        })
        .expect("valid commit record");
        publish::publish(store, &commit, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    async fn publish_log_segment(store: &MemoryStore, tenant_hash: TenantHash, now: i64) {
        publish_log_segment_at(store, tenant_hash, now - NS_PER_MIN).await;
    }

    struct FixedClock(i64);

    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn catalog_for(store: Arc<dyn ObjectStoreBackend>) -> Arc<Catalog> {
        crate::query::build_catalog(
            store,
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            std::time::Duration::from_secs(2),
        )
        .expect("catalog")
    }

    #[tokio::test]
    async fn warms_a_published_tenants_most_recent_metric_part() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant, now).await;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        assert!(cache.is_empty(), "sanity: nothing fetched yet");
        let gate = crate::cpu_gates::CpuGates::new(Default::default()).read;

        warm_cache(
            store,
            catalog,
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            gate.clone(),
            metadata_warm(),
        )
        .await;

        assert!(
            !cache.is_empty(),
            "warming a tenant with a real recent metric part must populate the cache"
        );
        // The one part's catalog decode ran through the warm pass's read gate,
        // inline under the byte floor (ADR-1702 decision 4).
        let section = gate
            .snapshot()
            .sites
            .iter()
            .find(|counts| counts.site == ravel_cpu_gate::ReadSite::SegmentSection)
            .map(|counts| (counts.jobs, counts.inline));
        assert_eq!(section, Some((0, 1)));
    }

    /// The warm pass's logs fetcher carries the same read gate: warming one
    /// recent RLOG part decodes it through the gate.
    #[tokio::test]
    async fn warms_a_published_tenants_most_recent_log_part_on_the_read_gate() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_log_segment(&memory, tenant, now).await;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let gate = crate::cpu_gates::CpuGates::new(Default::default()).read;

        warm_cache(
            store,
            catalog,
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            gate.clone(),
            metadata_warm(),
        )
        .await;

        assert_eq!(cache.len(), 1, "the one recent log part is warmed");
        let counted: Vec<_> = gate
            .snapshot()
            .sites
            .iter()
            .filter(|counts| counts.jobs + counts.inline > 0)
            .map(|counts| (counts.site, counts.jobs, counts.inline))
            .collect();
        // One postings block and one block decode, each inline under the
        // byte floor (ADR-1702 decision 4).
        assert_eq!(
            counted,
            vec![
                (ravel_cpu_gate::ReadSite::LogBlock, 0, 1),
                (ravel_cpu_gate::ReadSite::LogPostings, 0, 1),
            ]
        );
    }

    /// Issue #1255: the warm pass reserves against the budget it is handed. A
    /// log part's fetch reserves the whole object before its GET, so a 1-byte
    /// budget refuses it and nothing reaches the cache, while an unlimited
    /// budget over the same store warms it. Drop the logs fetcher's
    /// `.with_memory_budget` call in `warm_cache_inner` and the refused pass
    /// warms the part anyway. The fixture is logs only, so this does not cover
    /// the metrics fetcher's call.
    #[tokio::test]
    async fn warm_pass_reserves_against_the_budget_it_is_handed() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_log_segment(&memory, tenant, now).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);

        for (limit, entries) in [(1, 0), (u64::MAX, 1)] {
            let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
            let cache = Arc::new(Cache::new(CacheLimits::new(
                16 * 1024 * 1024,
                1024,
                1024 * 1024,
            )));
            warm_cache(
                store.clone(),
                catalog_for(store.clone()),
                ReadCache::Ram(cache.clone()),
                &FixedClock(now),
                Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
                budget.clone(),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                metadata_warm(),
            )
            .await;
            assert_eq!(
                cache.len(),
                entries,
                "a {limit}-byte budget: the log part is warmed only when the budget admits it"
            );
            assert_eq!(budget.reserved(), 0, "the pass releases what it reserved");
        }
    }

    /// Issue #1233: a tenant whose only parts are ~48h old must still get
    /// its most recent parts warmed, not zero. First asserts the pre-fix
    /// behavior directly -- old code resolved the fixed `[now - 24 h, now]`
    /// window regardless of when the tenant last
    /// ingested, and that window finds none of these parts -- then asserts
    /// the fixed behavior: exactly `min(MAX_PARTS_PER_TENANT_PER_SIGNAL,
    /// parts)` warmed, and that they are the newest by `max_event_ts_ns`.
    #[tokio::test]
    async fn old_ingest_tenant_still_gets_its_most_recent_parts_warmed() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        let latest_hour = now / NS_PER_HOUR - 48;

        // Six parts at hours H-5..=H (H = latest_hour), one sample each, so
        // each part's max_event_ts_ns falls in a distinct hour and sorts
        // unambiguously.
        for offset in 0..6i64 {
            let hour = latest_hour - offset;
            let event_ts_ns = hour * NS_PER_HOUR + NS_PER_MIN;
            publish_metric_segment_at(&memory, tenant, event_ts_ns).await;
        }

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());

        let old_fixed_window = ravel_types::TimeRange {
            start_ns: now - i64::from(WARM_LOOKBACK_HOURS) * NS_PER_HOUR,
            end_ns: now,
        };
        let old_window_snapshot = catalog
            .resolve(&tenant, Signal::Metrics, old_fixed_window, &[], now)
            .await
            .expect("resolve");
        assert_eq!(
            old_window_snapshot.segments.len(),
            0,
            "pre-fix behavior: a fixed [now - lookback, now] window finds none of this tenant's \
             48h-old parts -- this is the #1233 bug"
        );

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = SegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache));
        let warmed = warm_metrics(&catalog, &fetcher, tenant, now).await;
        assert_eq!(
            warmed, 4,
            "post-fix behavior: min(MAX_PARTS_PER_TENANT_PER_SIGNAL, parts) of a 48h-old \
             tenant's parts must warm"
        );

        let (parts, found_hour) = most_recent_parts(&catalog, tenant, Signal::Metrics, now).await;
        let found_hour = match found_hour {
            LatestHour::Found {
                hour,
                has_ingest_part,
            } => {
                assert!(
                    has_ingest_part,
                    "every published part here is a real commit record, not a tombstone"
                );
                hour
            }
            _ => panic!("tenant has parts for this signal"),
        };
        let latest_hour = u32::try_from(latest_hour).expect("hour");
        assert_eq!(
            found_hour, latest_hour,
            "latest ingest hour must be H, not now's hour"
        );
        let warmed_hours: std::collections::BTreeSet<u32> = parts
            .iter()
            .take(MAX_PARTS_PER_TENANT_PER_SIGNAL)
            .map(|part| part.ingest_hour_bucket)
            .collect();
        let expected_hours: std::collections::BTreeSet<u32> = [
            latest_hour,
            latest_hour - 1,
            latest_hour - 2,
            latest_hour - 3,
        ]
        .into_iter()
        .collect();
        assert_eq!(
            warmed_hours, expected_hours,
            "the 4 warmed parts must be the 4 newest by max_event_ts_ns: hours H, H-1, H-2, H-3"
        );
    }

    /// The change in #1233 widens the lookback side of the window without
    /// narrowing it, so a tenant that ingested within the last day (where
    /// old and new windows agree) must still warm exactly as many parts as
    /// before. This is not an unconditional superset: it holds only because
    /// a part's `max_event_ts_ns` stays within `max_ingest_lag_ns` of the
    /// ingest-hour bucket it was filed under, which is the assumption this
    /// warm-up serves, not a guarantee `latest_ingest_hour` enforces (see the
    /// module doc comment and ADR-0046's amendment).
    #[tokio::test]
    async fn tenant_with_recent_ingest_warms_exactly_as_before() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant, now).await;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = SegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache));

        let warmed = warm_metrics(&catalog, &fetcher, tenant, now).await;
        assert_eq!(
            warmed, 1,
            "a tenant ingesting within the last day must warm its one recent part, unchanged \
             from before #1233"
        );
    }

    /// 2013-07-15T00:00:00Z: years before any ingest hour these tests use.
    const EVENT_2013_NS: i64 = 1_373_846_400 * NS_PER_SEC;

    /// Issue #4257, the event-time window resolving nothing: the window the
    /// warm-up resolved before the fix, `[latest - 24 h, latest + 1 h]` in
    /// event time, finds none of a backdated tenant's parts.
    async fn assert_event_time_window_finds_nothing(
        catalog: &Catalog,
        tenant: TenantHash,
        signal: Signal,
        latest_hour: i64,
        now: i64,
    ) {
        let event_window = ravel_types::TimeRange {
            start_ns: latest_hour * NS_PER_HOUR - i64::from(WARM_LOOKBACK_HOURS) * NS_PER_HOUR,
            end_ns: (latest_hour + 1) * NS_PER_HOUR,
        };
        let snapshot = catalog
            .resolve(&tenant, signal, event_window, &[], now)
            .await
            .expect("resolve");
        assert_eq!(
            snapshot.segments.len(),
            0,
            "the pre-#4257 event-time window finds none of the tenant's 2013 parts"
        );
    }

    /// Issue #4257: a logs tenant whose event times are in 2013 and whose
    /// parts are filed in its two latest ingest hours gets
    /// `min(MAX_PARTS_PER_TENANT_PER_SIGNAL, parts)` parts warmed whole, and
    /// they are the newest ingest hour's. The older hour's two parts carry
    /// the later event times, so the order is by ingest hour, not event time.
    ///
    /// Seen failing against the event-time window (0 warmed, not 4), against a
    /// selection taking the oldest ingest hour first, and against one ordered
    /// by `max_event_ts_ns` alone (both put the older hour's parts first).
    #[tokio::test]
    async fn a_backdated_logs_tenant_warms_its_newest_ingest_hour_whole() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        let latest_hour = now / NS_PER_HOUR;
        let filed: [(i64, i64); 6] = [
            (latest_hour - 1, EVENT_2013_NS + 50 * NS_PER_MIN),
            (latest_hour - 1, EVENT_2013_NS + 40 * NS_PER_MIN),
            (latest_hour, EVENT_2013_NS + 30 * NS_PER_MIN),
            (latest_hour, EVENT_2013_NS + 20 * NS_PER_MIN),
            (latest_hour, EVENT_2013_NS + 10 * NS_PER_MIN),
            (latest_hour, EVENT_2013_NS),
        ];
        for (i, (hour, event_ts_ns)) in filed.into_iter().enumerate() {
            publish_log_segment_filed(&memory, tenant, event_ts_ns, hour, [i as u8 + 1; 32]).await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        assert_event_time_window_finds_nothing(&catalog, tenant, Signal::Logs, latest_hour, now)
            .await;

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = LogSegmentFetcher::new(store).with_cache(ReadCache::Ram(cache.clone()));
        let (warmed, parts) = warm_logs(&catalog, &fetcher, tenant, now).await;

        assert_eq!(
            warmed, MAX_PARTS_PER_TENANT_PER_SIGNAL,
            "parts warmed whole"
        );
        assert_eq!(
            cache.len(),
            MAX_PARTS_PER_TENANT_PER_SIGNAL,
            "cache entries"
        );
        let order: Vec<(u32, i64)> = parts
            .iter()
            .map(|part| (part.ingest_hour_bucket, part.max_event_ts_ns))
            .collect();
        let expected: Vec<(u32, i64)> = [2, 3, 4, 5, 0, 1]
            .into_iter()
            .map(|i| {
                let (hour, event_ts_ns) = filed[i];
                (u32::try_from(hour).expect("hour"), event_ts_ns)
            })
            .collect();
        assert_eq!(
            order, expected,
            "the warm set is every part, newest ingest hour first; the whole-part warm takes \
             the first four"
        );
    }

    /// Issue #4257, metrics: the same backdated shape warms
    /// `min(MAX_PARTS_PER_TENANT_PER_SIGNAL, parts)` metric parts, the newest
    /// ingest hour's. Seen failing against the event-time window (0, not 4).
    #[tokio::test]
    async fn a_backdated_metrics_tenant_warms_its_newest_ingest_hour() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        let latest_hour = now / NS_PER_HOUR;
        for i in 0..6i64 {
            let hour = if i < 2 { latest_hour - 1 } else { latest_hour };
            let event_ts_ns = EVENT_2013_NS + (6 - i) * NS_PER_MIN;
            publish_metric_segment_filed(&memory, tenant, event_ts_ns, hour).await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        assert_event_time_window_finds_nothing(&catalog, tenant, Signal::Metrics, latest_hour, now)
            .await;

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = SegmentFetcher::new(store).with_cache(ReadCache::Ram(cache));
        assert_eq!(
            warm_metrics(&catalog, &fetcher, tenant, now).await,
            MAX_PARTS_PER_TENANT_PER_SIGNAL
        );
        let (parts, _) = most_recent_parts(&catalog, tenant, Signal::Metrics, now).await;
        let hours: Vec<u32> = parts.iter().map(|part| part.ingest_hour_bucket).collect();
        let newest = u32::try_from(latest_hour).expect("hour");
        assert_eq!(
            hours,
            [newest, newest, newest, newest, newest - 1, newest - 1],
            "newest ingest hour first"
        );
    }

    #[tokio::test]
    async fn empty_store_warms_nothing_and_does_not_error() {
        let memory = MemoryStore::new();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        warm_cache(
            store,
            catalog,
            ReadCache::Ram(cache.clone()),
            &FixedClock(now_ns()),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            metadata_warm(),
        )
        .await;

        assert!(cache.is_empty(), "no tenants means nothing to warm");
    }

    /// `discover_tenants` calls `list_delimited`; a fault there must degrade
    /// this whole pass to a no-op, not panic or propagate.
    #[tokio::test]
    async fn tenant_discovery_failure_is_a_silent_no_op() {
        let inner = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        publish_metric_segment(&inner, tenant, now_ns()).await;

        let plan = FaultPlan::empty().with_rule(Rule::new(Op::List, ScriptedFault::Timeout));
        let fault_store = Arc::new(FaultStore::new(inner, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        warm_cache(
            store.clone(),
            catalog,
            ReadCache::Ram(cache.clone()),
            &FixedClock(now_ns()),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            metadata_warm(),
        )
        .await;

        assert!(
            cache.is_empty(),
            "a discovery failure must warm nothing, not partially warm or panic"
        );
        assert_eq!(
            fault_store.fault_count(Op::List, FaultKind::Timeout),
            1,
            "the discovery failure must actually come from the injected fault, not some other cause"
        );
    }

    /// Records every field of every WARN event as one combined string (`"
    /// name=value"` per field), so a test can prove a specific warn! did or
    /// did not fire and inspect its structured fields (e.g.
    /// `latest_ingest_hour="error"`), not just its message text.
    #[derive(Default, Clone)]
    struct WarnEventCapture(Arc<parking_lot::Mutex<Vec<String>>>);

    impl<S> tracing_subscriber::Layer<S> for WarnEventCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            #[derive(Default)]
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }

                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }
            }
            let mut visitor = Visitor::default();
            event.record(&mut visitor);
            self.0.lock().push(visitor.0);
        }
    }

    /// #1233: a signal the tenant has never ingested must short-circuit at
    /// `latest_ingest_hour` (`Ok(None)`) with no `resolve` call after it, and
    /// `cache_warm`'s own `log_warm_result` must log at `info!`, never its
    /// "has parts but none warmed" `warn!` -- "nothing to warm" is a normal
    /// outcome for this pass, not the "parts exist but none warmed" failure
    /// case.
    ///
    /// `latest_ingest_hour` itself now costs exactly 7 bounded `list_after`
    /// calls per shard (finding 1's doubling probe sweep, `catalog_for`'s
    /// `shard_count` of 1) to conclude there is nothing within the lookback
    /// cap, rather than the single unbounded `list_delimited` the old
    /// implementation used -- so "no resolve call after it" is asserted as
    /// an exact `list.calls` delta of 7, not 0: `list_after` and `list` share
    /// the `list.calls` counter (`resolve`'s own listing would add to the
    /// same counter, which is exactly why this test needs an exact number
    /// rather than "some listing happened"). `catalog_for` builds with
    /// provisioning enforcement on, so `latest_ingest_hour` also costs
    /// exactly one `get.calls` for the generation-history read
    /// (`read_scan_generations`, finding 2) -- asserted separately, since a
    /// GET and a LIST are different counters and a miscount in either one
    /// would otherwise hide behind the other.
    ///
    /// #1233 finding 3: exhausting the 7-probe sweep without finding
    /// anything now logs at `debug!`, not `warn!` -- a signal the tenant has
    /// simply never used is not a failure. This test therefore asserts ZERO
    /// warn-level events overall, not merely the absence of one specific
    /// message.
    #[tokio::test]
    async fn tenant_with_no_parts_for_a_signal_issues_no_resolve_and_no_warn() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant, now).await;

        let instrumented = Arc::new(InstrumentedStore::new(memory));
        let store: Arc<dyn ObjectStoreBackend> = instrumented.clone();
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = LogSegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache));

        let captured: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
        let subscriber = tracing_subscriber::registry().with(WarnEventCapture(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let before = instrumented.metrics().snapshot();
        let (warmed, _) = warm_logs(&catalog, &fetcher, tenant, now).await;
        let after = instrumented.metrics().snapshot();

        assert_eq!(warmed, 0, "a tenant with no log parts must warm zero");
        assert_eq!(
            after.list.calls - before.list.calls,
            7,
            "latest_ingest_hour returning Ok(None) must cost exactly its 7-probe sweep (shard_count \
             1) and short-circuit before any resolve-issued list() call -- a resolve call here \
             would push this delta past 7"
        );
        assert_eq!(
            after.get.calls - before.get.calls,
            1,
            "read_scan_generations (provisioning enforcement is on in catalog_for) costs exactly \
             one GET per latest_ingest_hour call, accounted separately from the 7 list_after calls \
             above (#1233 finding 2)"
        );
        assert!(
            captured.lock().is_empty(),
            "no parts for a signal is a normal outcome for cache_warm: exhausting the lookback \
             sweep now logs at debug! (#1233 finding 3), so this pass must produce zero warn-level \
             events, not merely avoid one specific message"
        );
    }

    /// #1233 finding 2: a `latest_ingest_hour` listing fault for one tenant
    /// must warm nothing for that tenant/signal, log a `warn!` naming the
    /// failure with a distinct `latest_ingest_hour="error"` value (never the
    /// `"none"` value `log_warm_result` uses for a signal the tenant simply
    /// never ingested), and must not stop the other tenant from warming
    /// normally.
    #[tokio::test]
    async fn a_listing_fault_for_one_tenant_does_not_stop_the_other() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant_a = TenantId::new("faulty").hash();
        let tenant_b = TenantId::new("healthy").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant_a, now).await;
        publish_metric_segment(&memory, tenant_b, now).await;

        // Scoped to tenant_a's own commit-record prefix: tenant_b's listing,
        // and tenant discovery's bare "t/" listing, must be unaffected.
        let fault_prefix = format!(
            "t/{}/{}/c/",
            tenant_a.to_hex(),
            Signal::Metrics.key_prefix()
        );
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Transient("boom".to_string()))
                .with_key_contains(fault_prefix),
        );
        let fault_store = Arc::new(FaultStore::new(memory, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = SegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache));

        let captured: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
        let subscriber = tracing_subscriber::registry().with(WarnEventCapture(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let warmed_a = warm_metrics(&catalog, &fetcher, tenant_a, now).await;
        let warmed_b = warm_metrics(&catalog, &fetcher, tenant_b, now).await;

        assert_eq!(
            warmed_a, 0,
            "the faulted tenant must warm zero, not partially"
        );
        assert_eq!(
            warmed_b, 1,
            "the healthy tenant's own recent part must still warm, unaffected by tenant_a's fault"
        );
        assert_eq!(
            fault_store.fault_count(Op::List, FaultKind::Transient),
            1,
            "the fault must fire exactly once: latest_ingest_hour_for_shard returns on the first \
             list_after error, so it never retries into a second probe width"
        );
        assert!(
            captured
                .lock()
                .iter()
                .any(|msg| msg.contains("latest_ingest_hour failed")
                    && msg.contains("latest_ingest_hour=\"error\"")),
            "the faulted tenant must log a warn! naming the listing failure with a distinct \
             latest_ingest_hour=\"error\" value, not the \"none\" value a signal the tenant never \
             ingested would use"
        );
    }

    /// #1233 finding 6: a `latest_ingest_hour` listing fault scoped to one
    /// tenant's METRICS commit prefix must leave that same tenant's LOGS
    /// warming normally -- per-signal isolation within a single tenant, not
    /// just the cross-tenant isolation `a_listing_fault_for_one_tenant_does_not_stop_the_other`
    /// already pins.
    #[tokio::test]
    async fn a_listing_fault_for_one_signal_does_not_stop_the_other_signal() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant, now).await;
        publish_log_segment(&memory, tenant, now).await;

        // Scoped to this tenant's METRICS commit-record prefix only: the
        // LOGS commit-record prefix, and tenant discovery's bare "t/"
        // listing, must be unaffected.
        let fault_prefix = format!("t/{}/{}/c/", tenant.to_hex(), Signal::Metrics.key_prefix());
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Transient("boom".to_string()))
                .with_key_contains(fault_prefix),
        );
        let fault_store = Arc::new(FaultStore::new(memory, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let metrics_fetcher =
            SegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache.clone()));
        let logs_fetcher = LogSegmentFetcher::new(store.clone()).with_cache(ReadCache::Ram(cache));

        let captured: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
        let subscriber = tracing_subscriber::registry().with(WarnEventCapture(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let warmed_metrics = warm_metrics(&catalog, &metrics_fetcher, tenant, now).await;
        let (warmed_logs, _) = warm_logs(&catalog, &logs_fetcher, tenant, now).await;

        assert_eq!(
            warmed_metrics, 0,
            "the faulted signal must warm zero, not partially"
        );
        assert_eq!(
            warmed_logs, 1,
            "the tenant's own unfaulted LOGS signal must still warm its one recent part, \
             unaffected by the METRICS-scoped fault"
        );
        assert_eq!(
            fault_store.fault_count(Op::List, FaultKind::Transient),
            1,
            "the fault must fire exactly once: it is scoped to the METRICS commit prefix, so \
             warm_logs's own listing (a disjoint LOGS commit prefix) never triggers it"
        );
        assert!(
            captured
                .lock()
                .iter()
                .any(|msg| msg.contains("latest_ingest_hour failed")
                    && msg.contains("latest_ingest_hour=\"error\"")),
            "the faulted signal must log a warn! naming the listing failure"
        );
    }

    /// A never-resolving store: `list_delimited` and `get` both return a
    /// future that never completes. `FaultStore`'s `Timeout` fault fails
    /// instantly and cannot simulate a genuinely slow backend, so this pass's
    /// deadline can only be exercised with a hand-rolled double like this
    /// one, combined with `tokio::time::pause`/`advance` so the test does
    /// not actually wait ten seconds of real time.
    struct HangingStore;

    #[async_trait]
    impl ObjectStoreBackend for HangingStore {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            std::future::pending().await
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            std::future::pending().await
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            std::future::pending().await
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            std::future::pending().await
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            std::future::pending().await
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            std::future::pending().await
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// Delegates every call to a real `MemoryStore` except `list_delimited`
    /// on a commit-shard prefix (`.../c/.../`), which hangs forever. Tenant
    /// discovery (`list_delimited("t/")`) succeeds normally, so the pass
    /// reaches the new `latest_ingest_hour` listing step before hanging --
    /// unlike [`HangingStore`], which hangs before discovery ever completes
    /// and so cannot exercise this step's own deadline coverage.
    ///
    /// `list_after` hangs on the same prefix, mirroring `list_delimited`:
    /// [`Catalog::latest_ingest_hour`] probes each shard with `list_after`,
    /// not `list_delimited` (issue #1233 finding 1), so leaving `list_after`
    /// on the passthrough default (which forwards to `list`, never hanging)
    /// would make this double stop intercepting the very call this test
    /// means to hang.
    struct HangsOnCommitListing(MemoryStore);

    #[async_trait]
    impl ObjectStoreBackend for HangsOnCommitListing {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.0.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.0.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.0.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.0.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            if prefix.contains("/c/") {
                std::future::pending().await
            } else {
                self.0.list_after(prefix, start_after, page).await
            }
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            if prefix.contains("/c/") {
                std::future::pending().await
            } else {
                self.0.list_delimited(prefix).await
            }
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.0.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.0.capabilities()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn warm_pass_deadline_still_bounds_the_new_listing_step() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        publish_metric_segment(&memory, tenant, now_ns()).await;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(HangsOnCommitListing(memory));
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let cache_for_assertion = cache.clone();

        let warm = tokio::spawn(async move {
            warm_cache(
                store,
                catalog,
                ReadCache::Ram(cache),
                &FixedClock(now_ns()),
                Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                metadata_warm(),
            )
            .await;
        });

        tokio::time::timeout(WARM_DEADLINE * 3, warm)
            .await
            .expect(
                "warm_cache must return once its deadline elapses even when only the new \
                 latest_ingest_hour listing hangs, with tenant discovery already having \
                 succeeded",
            )
            .expect("warm_cache task must not panic");

        assert!(
            cache_for_assertion.is_empty(),
            "the hung latest_ingest_hour call must warm strictly less than planned (zero, not \
             the one available part), proving the deadline bounds this new listing step too"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn warm_pass_gives_up_at_its_deadline_instead_of_hanging_forever() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(HangingStore);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        let warm = tokio::spawn(async move {
            warm_cache(
                store,
                catalog,
                ReadCache::Ram(cache),
                &FixedClock(now_ns()),
                Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                metadata_warm(),
            )
            .await;
        });

        // No manual `advance()`: the child's own `WARM_DEADLINE` sleep is only
        // constructed once it is first polled, so advancing before that would
        // race against a deadline measured from a later baseline. Paused-clock
        // auto-advance (both tasks below are blocked purely on timers) jumps
        // straight to the child's deadline once we await it; the outer guard
        // is comfortably larger than `WARM_DEADLINE` so it can only fire if
        // the child genuinely never returns.
        tokio::time::timeout(WARM_DEADLINE * 3, warm)
            .await
            .expect("warm_cache must return once its internal deadline elapses, not hang forever")
            .expect("warm_cache task must not panic");
    }

    /// Delegates to a `MemoryStore`, except that a suffix GET of a data object
    /// under `hang_prefix` and not in `spared` never completes, and records
    /// every whole-object GET and every suffix GET. The whole-part warm reads
    /// small logs objects with [`GetRange::Full`]; the metadata pass opens each
    /// object with a suffix probe.
    struct HangsOnTenantProbe {
        inner: MemoryStore,
        hang_prefix: String,
        spared: parking_lot::Mutex<Vec<String>>,
        full_gets: parking_lot::Mutex<Vec<String>>,
        suffix_gets: parking_lot::Mutex<Vec<String>>,
        hung_probes: std::sync::atomic::AtomicUsize,
    }

    impl HangsOnTenantProbe {
        fn new(inner: MemoryStore, hang_prefix: String) -> Self {
            Self {
                inner,
                hang_prefix,
                spared: parking_lot::Mutex::new(Vec::new()),
                full_gets: parking_lot::Mutex::new(Vec::new()),
                suffix_gets: parking_lot::Mutex::new(Vec::new()),
                hung_probes: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        async fn read(&self, key: &str, range: GetRange) {
            if matches!(range, GetRange::Suffix(_)) {
                self.suffix_gets.lock().push(key.to_string());
            }
            if key.starts_with(&self.hang_prefix)
                && matches!(range, GetRange::Suffix(_))
                && !self.spared.lock().iter().any(|spared| spared == key)
            {
                self.hung_probes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            if matches!(range, GetRange::Full) {
                self.full_gets.lock().push(key.to_string());
            }
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for HangsOnTenantProbe {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.read(key, range).await;
            self.inner.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &ravel_object_store::Pin,
        ) -> Result<ravel_object_store::PinnedRead, StoreError> {
            self.read(key, range).await;
            self.inner.get_pinned(key, range, pin).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// The tenant discovered first has 4 small logs parts (at or under the plan
    /// bound, warmed whole) and 10 large ones (above it, metadata only); the
    /// second tenant has one small part. Every suffix probe of the first
    /// tenant's large objects hangs, so its metadata pass never finishes: it
    /// hangs on 7 probes at once, one fewer than the 8 GET permits. The
    /// whole-part warm runs beside it and still warms the first tenant's 4
    /// small parts and reads the second tenant's part whole through the permit
    /// left free. The small parts are read whole with a `GetRange::Full` GET,
    /// which the double never hangs, and are below the bound so the metadata
    /// pass never probes them. Seen failing against a metadata pass that runs
    /// to completion before any whole part (no whole read of the second
    /// tenant), against a pass that runs as many objects at once as there are
    /// permits (8 hung probes), and against a whole-part warm that reads only
    /// the first tenant's parts.
    #[tokio::test(start_paused = true)]
    async fn a_metadata_pass_that_spends_the_deadline_leaves_later_whole_part_warms_intact() {
        let memory = MemoryStore::new();
        let now = now_ns();
        for name in ["acme", "globex"] {
            publish_log_segment(&memory, TenantId::new(name).hash(), now).await;
        }
        let order = ravel_maintain::discover_tenants(&memory)
            .await
            .expect("discover tenants");
        assert_eq!(order.len(), 2, "two tenants: {order:?}");
        let (first, second) = (order[0], order[1]);
        // The first tenant: 3 more small recent parts (whole-warmed) and 10
        // large older parts above the plan bound (metadata only, probes hang).
        for k in 2..=4u8 {
            publish_log_segment_hashed(&memory, first, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        for k in 5..=14u8 {
            let at = now - i64::from(k) * NS_PER_MIN;
            publish_log_segment_sized(
                &memory,
                first,
                at,
                at / NS_PER_HOUR,
                [k; 32],
                LARGE_FILLER_BYTES,
            )
            .await;
        }
        let logs_prefix = |tenant: TenantHash| format!("t/{}/l/l0/", tenant.to_hex());

        let store = Arc::new(HangsOnTenantProbe::new(memory, logs_prefix(first)));
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
        let catalog = catalog_for(dyn_store.clone());
        let (first_parts, _) =
            most_recent_parts(&catalog_for(dyn_store.clone()), first, Signal::Logs, now).await;
        assert_eq!(first_parts.len(), 14);
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        // A plan bound between the small objects (whole-warmed) and the large
        // ones (metadata only), so the two steps split the first tenant's set.
        let engine = split_engine();
        let warm_cache_handle = cache.clone();
        let warm = tokio::spawn(async move {
            warm_cache(
                dyn_store,
                catalog,
                ReadCache::Ram(warm_cache_handle),
                &FixedClock(now),
                Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                LogsMetadataWarm::new(&engine, 16 * 1024 * 1024),
            )
            .await;
        });
        tokio::time::timeout(WARM_DEADLINE * 3, warm)
            .await
            .expect("warm_cache returns at its deadline")
            .expect("warm_cache task must not panic");

        assert_eq!(
            store.hung_probes.load(std::sync::atomic::Ordering::SeqCst),
            7,
            "the first tenant's metadata pass hung on one probe fewer than the 8 permits"
        );
        let full_gets = store.full_gets.lock().clone();
        let read_whole = |tenant: TenantHash| {
            full_gets
                .iter()
                .filter(|key| key.starts_with(&logs_prefix(tenant)))
                .count()
        };
        assert_eq!(
            read_whole(second),
            1,
            "the second tenant's one logs part was read whole: {full_gets:?}"
        );
        assert_eq!(
            cache.len(),
            5,
            "the first tenant's 4 most recent objects and the second tenant's part are \
             resident, and the hung probes inserted nothing"
        );
        let hung_keys: Vec<String> = first_parts[MAX_PARTS_PER_TENANT_PER_SIGNAL..]
            .iter()
            .map(|part| part.data_object_key.clone())
            .collect();
        let suffix_gets = store.suffix_gets.lock().clone();
        assert_eq!(
            suffix_gets
                .iter()
                .filter(|key| hung_keys.contains(key))
                .count(),
            7,
            "the pass issued no probe of an older object beyond the 7 hung ones: {suffix_gets:?}"
        );
    }

    /// The metadata pass's ceiling counts only the bytes the pass itself
    /// fetches. One tenant has six logs parts: the whole-part warm reads the
    /// 4 most recent whole, more bytes than the pass's ceiling, and the pass,
    /// run after it here so those 4 are resident, finds their probes cached (a
    /// fixture object is no bigger than its probe, so the probe key is the
    /// whole-object key) and probes the 2 older parts. With the ceiling at
    /// exactly the 2 older parts' sizes both are admitted; one byte lower,
    /// only the first is. A ceiling charged with the whole-part warm's
    /// resident bytes admits neither.
    #[tokio::test]
    async fn the_metadata_ceiling_counts_only_the_pass_s_own_bytes() {
        let now = now_ns();
        let tenant = TenantId::new("acme").hash();
        let memory = MemoryStore::new();
        for k in 1..=6u8 {
            publish_log_segment_hashed(&memory, tenant, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        let store = Arc::new(HangsOnTenantProbe::new(memory, "none/".to_string()));
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
        let (parts, _) =
            most_recent_parts(&catalog_for(dyn_store.clone()), tenant, Signal::Logs, now).await;
        assert_eq!(parts.len(), 6);
        let whole: u64 = parts[..4].iter().map(|part| part.object_size).sum();
        let older: u64 = parts[4..].iter().map(|part| part.object_size).sum();
        assert!(
            whole > older,
            "the whole-part warm alone exceeds the pass's ceiling: {whole} vs {older}"
        );
        let key = |i: usize| parts[i].data_object_key.clone();
        let data_prefix = format!("t/{}/l/l0/", tenant.to_hex());
        let data_gets = |gets: &parking_lot::Mutex<Vec<String>>| -> Vec<String> {
            std::mem::take(&mut *gets.lock())
                .into_iter()
                .filter(|key| key.starts_with(&data_prefix))
                .collect()
        };
        // Every fixture object is above this plan bound, so the metadata pass
        // reads each one.
        let engine = EngineConfig {
            logs_block_range_threshold: 1,
            logs_projection_break_even_bytes: Some(1),
            ..EngineConfig::default()
        };

        for (ceiling, probed) in [(older, vec![key(4), key(5)]), (older - 1, vec![key(4)])] {
            data_gets(&store.full_gets);
            data_gets(&store.suffix_gets);
            let cache = Arc::new(Cache::new(CacheLimits::new(
                16 * 1024 * 1024,
                1024,
                1024 * 1024,
            )));
            let limiter = Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits"));
            let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
            let gate = crate::cpu_gates::CpuGates::new(Default::default()).read;
            let logs_fetcher = LogSegmentFetcher::new(dyn_store.clone())
                .with_cache(ReadCache::Ram(cache.clone()))
                .with_get_limiter(limiter.clone());
            let (warmed, _) =
                warm_logs(&catalog_for(dyn_store.clone()), &logs_fetcher, tenant, now).await;
            assert_eq!(warmed, MAX_PARTS_PER_TENANT_PER_SIGNAL);
            let mut pass = LogsMetadataPass::new(
                dyn_store.clone(),
                ReadCache::Ram(cache.clone()),
                limiter,
                budget,
                gate,
                LogsMetadataWarm::new(&engine, ceiling * METADATA_CACHE_SHARE_DIVISOR),
            )
            .expect("valid fetch bound");
            let progress = WarmProgress::default();
            let pass_warmed = pass.warm(tenant, &parts, &progress).await;
            assert_eq!(
                (
                    pass_warmed.objects as u64,
                    progress.metadata_objects_warmed.load(Ordering::Relaxed)
                ),
                (4 + probed.len() as u64, 4 + probed.len() as u64),
                "the 4 resident probes and the admitted older parts are warmed"
            );

            assert_eq!(
                data_gets(&store.full_gets),
                (0..4).map(key).collect::<Vec<_>>(),
                "the whole-part warm reads the 4 most recent parts whole"
            );
            assert_eq!(
                data_gets(&store.suffix_gets),
                probed,
                "a {ceiling}-byte ceiling: the pass probes the older parts that fit it"
            );
            assert_eq!(cache.len(), 4 + probed.len());
        }
    }

    /// Delegates to a `MemoryStore`, delaying every GET of a data object under
    /// one of `data_prefixes` on the paused tokio clock: a whole-object GET (the
    /// whole-part warm's read of a small logs object) by `full_delay`, or
    /// forever when it is `None`; any other GET (the metadata pass's probe) by
    /// `range_delay`.
    struct DelayedDataStore {
        inner: MemoryStore,
        data_prefixes: Vec<String>,
        full_delay: Option<Duration>,
        range_delay: Duration,
    }

    impl DelayedDataStore {
        async fn delay(&self, key: &str, range: GetRange) {
            if !self
                .data_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
            {
                return;
            }
            match (range, self.full_delay) {
                (GetRange::Full, Some(delay)) => tokio::time::sleep(delay).await,
                (GetRange::Full, None) => std::future::pending::<()>().await,
                _ => tokio::time::sleep(self.range_delay).await,
            }
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for DelayedDataStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.delay(key, range).await;
            self.inner.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &ravel_object_store::Pin,
        ) -> Result<ravel_object_store::PinnedRead, StoreError> {
            self.delay(key, range).await;
            self.inner.get_pinned(key, range, pin).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// Records every event at INFO or above as its message plus `"
    /// name=value"` per field, the text an operator reads.
    #[derive(Default, Clone)]
    struct EventCapture(Arc<parking_lot::Mutex<Vec<String>>>);

    impl EventCapture {
        /// Every captured event whose message is `message`.
        fn events(&self, message: &str) -> Vec<String> {
            let quoted = format!(" message={message}");
            self.0
                .lock()
                .iter()
                .filter(|event| event.starts_with(&quoted))
                .cloned()
                .collect()
        }
    }

    impl<S> tracing_subscriber::Layer<S> for EventCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() > tracing::Level::INFO {
                return;
            }
            #[derive(Default)]
            struct Visitor {
                message: String,
                fields: String,
            }
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    use std::fmt::Write as _;
                    let _ = write!(self.fields, " {}={value:?}", field.name());
                }

                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    if field.name() == "message" {
                        let _ = write!(self.message, " message={value:?}");
                    } else {
                        let _ = write!(self.fields, " {}={value:?}", field.name());
                    }
                }
            }
            let mut visitor = Visitor::default();
            event.record(&mut visitor);
            self.0.lock().push(visitor.message + &visitor.fields);
        }
    }

    const PASS_COMPLETE: &str = "cache warmup: pass complete";
    const DEADLINE_WARNING: &str = "cache warmup did not finish before its deadline; continuing \
                                    to readiness with whatever it warmed so far";

    /// A plan bound (100 KiB) between a one-record logs object (warmed whole,
    /// at or under the bound) and a [`publish_log_segment_sized`] object with
    /// [`LARGE_FILLER_BYTES`] of filler (above the bound, metadata only): the
    /// two warm steps split such a tenant's set (#4307).
    const SPLIT_PLAN_BOUND: u64 = 100_000;
    const LARGE_FILLER_BYTES: usize = 400_000;

    /// Filler for a logs object above both the plan bound and the 512 KiB
    /// `DEFAULT_WHOLE_OBJECT_THRESHOLD`, so a whole read of it goes out as block
    /// ranges rather than one `GetRange::Full`
    /// (`the_whole_part_warm_issues_no_block_get_for_a_part_above_the_bound`).
    const ABOVE_WHOLE_OBJECT_FILLER_BYTES: usize = 700 * 1024;

    /// An [`EngineConfig`] whose logs plan bound is [`SPLIT_PLAN_BOUND`].
    fn split_engine() -> EngineConfig {
        EngineConfig {
            logs_block_range_threshold: SPLIT_PLAN_BOUND,
            logs_projection_break_even_bytes: Some(SPLIT_PLAN_BOUND),
            ..EngineConfig::default()
        }
    }

    /// The whole-warmed (at or under the bound) and metadata-only (above the
    /// bound) logs parts the single tenant below holds.
    const SMALL_WHOLE_PARTS: usize = 2;
    const LARGE_META_PARTS: usize = 8;

    /// One tenant with [`SMALL_WHOLE_PARTS`] small logs parts (warmed whole) and
    /// [`LARGE_META_PARTS`] large ones (above the plan bound, metadata only)
    /// behind a [`DelayedDataStore`], warmed under `deadline` with the events
    /// captured. A whole-object (`GetRange::Full`) GET of a small part is
    /// delayed by `full_delay` (or hangs when it is `None`); every metadata
    /// range read of a large part is delayed 200 ms.
    async fn warm_large_tenant(
        full_delay: Option<Duration>,
        deadline: Duration,
    ) -> (EventCapture, usize, Vec<SegmentRef>) {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        for k in 1..=SMALL_WHOLE_PARTS {
            let k = u8::try_from(k).expect("small");
            publish_log_segment_hashed(&memory, tenant, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        for k in 0..LARGE_META_PARTS {
            let k = u8::try_from(SMALL_WHOLE_PARTS + k + 1).expect("small");
            let at = now - i64::from(k) * NS_PER_MIN;
            publish_log_segment_sized(
                &memory,
                tenant,
                at,
                at / NS_PER_HOUR,
                [k; 32],
                LARGE_FILLER_BYTES,
            )
            .await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(DelayedDataStore {
            inner: memory,
            data_prefixes: vec![format!("t/{}/l/l0/", tenant.to_hex())],
            full_delay,
            range_delay: Duration::from_millis(200),
        });
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let (parts, _) =
            most_recent_parts(&catalog_for(store.clone()), tenant, Signal::Logs, now).await;
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache_within(
            deadline,
            store.clone(),
            catalog_for(store),
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            LogsMetadataWarm::new(&split_engine(), 16 * 1024 * 1024),
        )
        .await;
        (capture, cache.len(), parts)
    }

    /// Every value `event` carries for the field `name`, in order.
    fn field_values<'e>(event: &'e str, name: &str) -> Vec<&'e str> {
        let marker = format!(" {name}=");
        event
            .match_indices(&marker)
            .map(|(at, _)| {
                event[at + marker.len()..]
                    .split(' ')
                    .next()
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Issue #4257, #4307: one tenant with 2 small logs parts (warmed whole,
    /// each Full read 1.9 s of a 2 s deadline) and 8 large ones (above the plan
    /// bound, metadata only, each range read 200 ms). The metadata pass runs
    /// beside the whole-part warm and reads all 8 large objects' metadata before
    /// the deadline, while the 2 small parts load. Seen failing against a
    /// whole-parts-first order: the 2 whole reads spend 1.9 s and the 8 metadata
    /// reads then spend 2 waves of 200 ms after them, 2.3 s in all, so the
    /// deadline fires and no pass-complete line is logged.
    #[tokio::test(start_paused = true)]
    async fn the_metadata_pass_warms_a_large_tenant_while_its_whole_parts_load() {
        let (capture, cached, _) =
            warm_large_tenant(Some(Duration::from_millis(1900)), Duration::from_secs(2)).await;

        assert_eq!(capture.events(DEADLINE_WARNING), Vec::<String>::new());
        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        assert!(
            complete[0].contains(&format!(" metadata_objects_warmed={LARGE_META_PARTS} "))
                && complete[0].contains(&format!(" parts_warmed={SMALL_WHOLE_PARTS} "))
                && complete[0].contains(&format!(" parts_above_plan_bound={LARGE_META_PARTS} ")),
            "the small parts are warmed whole, the large ones skipped above the bound and their \
             metadata warmed beside them: {}",
            complete[0]
        );
        assert_eq!(cached, SMALL_WHOLE_PARTS + LARGE_META_PARTS);
    }

    /// The pass-complete line carries each step's elapsed field, the metadata
    /// pass's GET permits, the whole-part bytes reserved and the count of parts
    /// above the plan bound exactly once. The whole-part warm's elapsed time is
    /// the 1.9 s of its two small reads, run together on the paused clock, and
    /// the bytes reserved are those two objects' sizes.
    #[tokio::test(start_paused = true)]
    async fn the_pass_complete_line_reports_each_step_s_elapsed_time_once() {
        let (capture, _, parts) =
            warm_large_tenant(Some(Duration::from_millis(1900)), Duration::from_secs(2)).await;

        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        for field in [
            "tenant_discovery_elapsed_ms",
            "part_resolution_elapsed_ms",
            "whole_part_warm_elapsed_ms",
            "metadata_pass_elapsed_ms",
            "elapsed_ms",
            "parts_warmed",
            "parts_above_plan_bound",
            "metadata_objects_warmed",
            "metadata_bytes_warmed",
            "metadata_budget_reached",
            "metadata_get_permits",
            "whole_part_bytes_reserved",
        ] {
            assert_eq!(
                field_values(&complete[0], field).len(),
                1,
                "{field} appears exactly once: {}",
                complete[0]
            );
        }
        let first_two = (parts[0].object_size + parts[1].object_size).to_string();
        let above = LARGE_META_PARTS.to_string();
        for (field, value) in [
            ("whole_part_warm_elapsed_ms", "1900"),
            ("metadata_get_permits", "7"),
            ("whole_part_bytes_reserved", first_two.as_str()),
            ("parts_above_plan_bound", above.as_str()),
        ] {
            assert_eq!(
                field_values(&complete[0], field),
                [value],
                "{field}: {}",
                complete[0]
            );
        }
    }

    /// A pass whose whole-part reads never return times out. Its deadline
    /// warning reports the 8 large objects the metadata pass warmed meanwhile
    /// and names only the whole-part step as still running: the metadata pass,
    /// reading objects above the plan bound the whole-part warm never touches,
    /// finishes while the 2 small whole reads hang. It also reports the 7 GET
    /// permits the pass reads on, the bytes the 2 hung reads reserved, and the
    /// count of parts above the bound. Each field is matched whole and appears
    /// exactly once. Seen failing against a warning without the GET permits and
    /// the bytes reserved, and against one that carries the GET permits twice.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_pass_reports_what_it_warmed_and_the_running_step() {
        let (capture, _, parts) = warm_large_tenant(None, Duration::from_secs(2)).await;

        assert_eq!(capture.events(PASS_COMPLETE), Vec::<String>::new());
        let warning = capture.events(DEADLINE_WARNING);
        assert_eq!(warning.len(), 1, "one deadline warning: {warning:?}");
        let reserved = (parts[0].object_size + parts[1].object_size).to_string();
        let above = LARGE_META_PARTS.to_string();
        let metadata = LARGE_META_PARTS.to_string();
        for (field, value) in [
            ("deadline_secs", "2"),
            ("parts_warmed", "0"),
            ("parts_above_plan_bound", above.as_str()),
            ("metadata_objects_warmed", metadata.as_str()),
            ("metadata_get_permits", "7"),
            ("whole_part_bytes_reserved", reserved.as_str()),
            ("step", "\"whole_part_warm\""),
        ] {
            assert_eq!(
                field_values(&warning[0], field),
                [value],
                "{field}: {}",
                warning[0]
            );
        }
        let bytes = field_values(&warning[0], "metadata_bytes_warmed");
        assert_eq!(bytes.len(), 1, "{}", warning[0]);
        assert!(
            bytes[0].parse::<u64>().expect("a byte count") > 0,
            "{}",
            warning[0]
        );
    }

    /// Delegates to a `MemoryStore`, holding every GET of a data object under
    /// one of `prefixes` for `delay` on the paused tokio clock, and records,
    /// per prefix, the most distinct objects read at once.
    struct CountsFetchesInFlight {
        inner: MemoryStore,
        prefixes: Vec<String>,
        delay: Duration,
        in_flight: parking_lot::Mutex<Vec<std::collections::HashMap<String, usize>>>,
        peak: parking_lot::Mutex<Vec<usize>>,
    }

    impl CountsFetchesInFlight {
        fn new(inner: MemoryStore, prefixes: Vec<String>, delay: Duration) -> Self {
            let n = prefixes.len();
            Self {
                inner,
                prefixes,
                delay,
                in_flight: parking_lot::Mutex::new(vec![Default::default(); n]),
                peak: parking_lot::Mutex::new(vec![0; n]),
            }
        }

        async fn counted<T>(&self, key: &str, read: impl std::future::Future<Output = T>) -> T {
            let Some(slot) = self.prefixes.iter().position(|p| key.starts_with(p)) else {
                return read.await;
            };
            {
                let mut in_flight = self.in_flight.lock();
                *in_flight[slot].entry(key.to_string()).or_default() += 1;
                let mut peak = self.peak.lock();
                peak[slot] = peak[slot].max(in_flight[slot].len());
            }
            tokio::time::sleep(self.delay).await;
            let got = read.await;
            let mut in_flight = self.in_flight.lock();
            if let Some(reads) = in_flight[slot].get_mut(key) {
                *reads -= 1;
                if *reads == 0 {
                    in_flight[slot].remove(key);
                }
            }
            got
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for CountsFetchesInFlight {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.counted(key, self.inner.get(key, range)).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &ravel_object_store::Pin,
        ) -> Result<ravel_object_store::PinnedRead, StoreError> {
            self.counted(key, self.inner.get_pinned(key, range, pin))
                .await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// Issue #4257: a tenant's whole-part warm reads at most
    /// [`WHOLE_PARTS_IN_FLIGHT_PER_SIGNAL`] (2) parts of each signal at once.
    /// The tenant has 4 metric and 4 logs parts, every data GET held 100 ms,
    /// and 8 GET permits, enough for all 8 at once. Seen failing with the
    /// bound at 4: the peak is 4 parts per signal.
    #[tokio::test(start_paused = true)]
    async fn the_whole_part_warm_reads_at_most_two_parts_per_signal_at_once() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        for k in 1..=4u8 {
            let at = now - i64::from(k) * NS_PER_MIN;
            publish_metric_segment_at(&memory, tenant, at).await;
            publish_log_segment_hashed(&memory, tenant, at, [k; 32]).await;
        }
        let store = Arc::new(CountsFetchesInFlight::new(
            memory,
            vec![
                format!("t/{}/m/l0/", tenant.to_hex()),
                format!("t/{}/l/l0/", tenant.to_hex()),
            ],
            Duration::from_millis(100),
        ));
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache(
            dyn_store.clone(),
            catalog_for(dyn_store),
            ReadCache::Ram(cache),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            metadata_warm(),
        )
        .await;

        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        assert_eq!(
            field_values(&complete[0], "parts_warmed"),
            ["8"],
            "{}",
            complete[0]
        );
        assert_eq!(
            *store.peak.lock(),
            [2, 2],
            "the most metric and logs parts read at once"
        );
    }

    /// Issue #4257, #4307: two tenants, each with one medium logs part (at or
    /// under the plan bound, warmed whole) and one large part (above it,
    /// metadata only). The cache's whole-part share, half the cache, holds one
    /// medium object but not both; its metadata ceiling, the other half, holds
    /// both large parts' probes. The metadata pass warms both large probes
    /// while the whole-part reads, held 1 s, are in flight; the whole-part warm
    /// reads the first tenant's medium object and skips the second's over
    /// budget. After the pass, every metadata entry the pass counted is still
    /// in the cache: reading each large object's directories again fetches no
    /// byte from the store. Seen failing against a whole-part warm with no byte
    /// budget: it reads both medium objects into the cache and evicts the
    /// metadata entries.
    #[tokio::test(start_paused = true)]
    async fn the_whole_part_warm_leaves_every_counted_metadata_entry_resident() {
        use tracing_subscriber::layer::SubscriberExt as _;

        // A bound between the medium parts (400 KB filler, warmed whole) and
        // the large ones (900 KB filler, metadata only).
        const BOUND: u64 = 650_000;
        let memory = MemoryStore::new();
        let now = now_ns();
        let tenants = [TenantId::new("acme").hash(), TenantId::new("globex").hash()];
        let mut k = 0u8;
        for tenant in tenants {
            // One medium part (at or under the bound, warmed whole) and one
            // large part (above it, metadata only).
            k += 1;
            let at = now - i64::from(k) * NS_PER_MIN;
            publish_log_segment_sized(&memory, tenant, at, at / NS_PER_HOUR, [k; 32], 400_000)
                .await;
            k += 1;
            let at = now - i64::from(k) * NS_PER_MIN;
            publish_log_segment_sized(&memory, tenant, at, at / NS_PER_HOUR, [k; 32], 900_000)
                .await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(DelayedDataStore {
            inner: memory,
            data_prefixes: tenants
                .iter()
                .map(|tenant| format!("t/{}/l/l0/", tenant.to_hex()))
                .collect(),
            full_delay: Some(Duration::from_secs(1)),
            range_delay: Duration::from_millis(20),
        });
        let catalog = catalog_for(store.clone());
        let mut medium = Vec::new();
        let mut large = Vec::new();
        for tenant in tenants {
            let (resolved, _) = most_recent_parts(&catalog, tenant, Signal::Logs, now).await;
            for part in resolved {
                if part.object_size <= BOUND {
                    medium.push((tenant, part));
                } else {
                    large.push((tenant, part));
                }
            }
        }
        assert_eq!(
            (medium.len(), large.len()),
            (2, 2),
            "each tenant has one medium part (<= the bound) and one large part (> it)"
        );
        let large_probes: u64 = large
            .iter()
            .map(|(_, part)| ravel_query::derive_suffix_len(part.object_size).min(part.object_size))
            .sum();
        let medium_sizes: Vec<u64> = medium.iter().map(|(_, part)| part.object_size).collect();
        // Half the cache holds the larger medium object but not both, and the
        // other half holds both large parts' probes.
        let cache_max = 2 * medium_sizes
            .iter()
            .copied()
            .max()
            .expect("two medium parts");
        assert!(
            medium_sizes[0] + medium_sizes[1] > cache_max / 2,
            "the whole-part share holds one medium object only: {medium_sizes:?} vs {}",
            cache_max / 2
        );
        assert!(
            large_probes <= cache_max / 2,
            "the metadata ceiling holds both large probes: {large_probes} vs {}",
            cache_max / 2
        );

        let cache = Arc::new(Cache::new(CacheLimits::new(cache_max, 1024, cache_max)));
        let engine = EngineConfig {
            logs_block_range_threshold: BOUND,
            logs_projection_break_even_bytes: Some(BOUND),
            ..EngineConfig::default()
        };
        let shape = LogsMetadataWarm::new(&engine, cache_max);
        let limiter = Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits"));
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let gate = crate::cpu_gates::CpuGates::new(Default::default()).read;
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache_within(
            Duration::from_secs(30),
            store.clone(),
            catalog,
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            limiter.clone(),
            budget.clone(),
            gate.clone(),
            shape,
        )
        .await;

        let pass =
            LogsMetadataPass::new(store, ReadCache::Ram(cache), limiter, budget, gate, shape)
                .expect("valid fetch bound");
        let mut refetched = Vec::new();
        for (tenant, part) in &large {
            let accounting = warm_accounting();
            pass.fetcher
                .block_range_fetcher()
                .fetch_plan_directories(part, *tenant, &accounting)
                .await
                .expect("directories");
            refetched.push(accounting.snapshot().total_s3_bytes());
        }
        assert_eq!(
            refetched, [0; 2],
            "every metadata entry the pass counted is still resident"
        );

        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        for (field, value) in [
            ("metadata_objects_warmed", "2"),
            ("parts_warmed", "1"),
            ("parts_above_plan_bound", "2"),
        ] {
            assert_eq!(
                field_values(&complete[0], field),
                [value],
                "{field}: {}",
                complete[0]
            );
        }
        // The one warmed part's reservation settles to the bytes its fetch
        // read, which is at most its object size.
        let reserved = field_values(&complete[0], "whole_part_bytes_reserved");
        assert_eq!(reserved.len(), 1, "{}", complete[0]);
        let reserved: u64 = reserved[0].parse().expect("a byte count");
        assert!(
            reserved > 0 && reserved <= *medium_sizes.iter().max().expect("two medium parts"),
            "one part's bytes held: {reserved} vs {medium_sizes:?}"
        );
        // The tenant whose medium part was skipped for the budget, and whose
        // large part is above the bound, is not a failure to warn about.
        assert_eq!(
            capture.events("cache warmup: tenant has parts for this signal but none were warmed"),
            Vec::<String>::new()
        );
        let mut logs_lines: Vec<(String, String, String)> = capture
            .events("cache warmup: warmed tenant signal")
            .iter()
            .filter(|event| event.contains(" signal=Logs "))
            .map(|event| {
                (
                    field_values(event, "parts_warmed").concat(),
                    field_values(event, "parts_over_budget").concat(),
                    field_values(event, "parts_above_plan_bound").concat(),
                )
            })
            .collect();
        logs_lines.sort();
        assert_eq!(
            logs_lines,
            [
                ("0".to_string(), "1".to_string(), "1".to_string()),
                ("1".to_string(), "0".to_string(), "1".to_string())
            ],
            "each tenant's logs line: (parts_warmed, parts_over_budget, parts_above_plan_bound)"
        );
    }

    /// Delegates to a `MemoryStore`, holding every listing of a commit-shard
    /// prefix (`.../c/...`) for `delay` on the paused tokio clock and counting
    /// them. Tenant discovery's `t/` listing is not delayed.
    struct DelaysCommitListings {
        inner: MemoryStore,
        delay: Duration,
        listings: std::sync::atomic::AtomicUsize,
    }

    impl DelaysCommitListings {
        async fn delay(&self, prefix: &str) {
            if prefix.contains("/c/") {
                self.listings
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
            }
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for DelaysCommitListings {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.delay(prefix).await;
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.delay(prefix).await;
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.delay(prefix).await;
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// The tenants in the streamed-resolution test below.
    const SLOW_LISTING_TENANTS: usize = 32;

    /// Issue #4257 review: [`SLOW_LISTING_TENANTS`] tenants with one metric
    /// part each, every commit-shard listing held 100 ms, under a 1 s
    /// deadline. Resolving one tenant takes more than one listing, so
    /// resolving all of them one after another takes several times the
    /// deadline. Resolution streams into the warms, so the tenants resolved
    /// first are warmed while later ones still resolve: the deadline warning
    /// reports parts warmed and every resolved tenant's part is resident.
    /// Seen failing against the loop that resolves every tenant, one at a
    /// time, before any warm starts: the deadline fires during resolution
    /// with `parts_warmed=0`.
    #[tokio::test(start_paused = true)]
    async fn tenants_resolved_before_the_deadline_are_warmed_while_later_ones_resolve() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let now = now_ns();
        for i in 0..SLOW_LISTING_TENANTS {
            publish_metric_segment(&memory, TenantId::new(format!("tenant-{i}")).hash(), now).await;
        }
        let store = Arc::new(DelaysCommitListings {
            inner: memory,
            delay: Duration::from_millis(100),
            listings: std::sync::atomic::AtomicUsize::new(0),
        });
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();

        // One tenant's resolution on its own issues its listings one after
        // another, so it takes that many listing delays.
        let one = TenantId::new("tenant-0").hash();
        let catalog = catalog_for(dyn_store.clone());
        let _ = most_recent_parts(&catalog, one, Signal::Metrics, now).await;
        let _ = most_recent_parts(&catalog, one, Signal::Logs, now).await;
        let listings = store.listings.swap(0, std::sync::atomic::Ordering::SeqCst);
        assert!(
            listings >= 2,
            "one tenant's resolution lists {listings} times"
        );
        let per_tenant = store.delay * u32::try_from(listings).expect("few listings");
        let deadline = Duration::from_secs(1);
        assert!(
            per_tenant < deadline && per_tenant * SLOW_LISTING_TENANTS as u32 > deadline * 4,
            "one tenant resolves within the deadline, all of them one after another take \
             several deadlines: {per_tenant:?}"
        );

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache_within(
            deadline,
            dyn_store.clone(),
            catalog_for(dyn_store),
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            metadata_warm(),
        )
        .await;

        assert_eq!(capture.events(PASS_COMPLETE), Vec::<String>::new());
        let warning = capture.events(DEADLINE_WARNING);
        assert_eq!(warning.len(), 1, "one deadline warning: {warning:?}");
        let warmed: usize = field_values(&warning[0], "parts_warmed")
            .concat()
            .parse()
            .expect("a part count");
        // Resolutions run PART_RESOLUTIONS_IN_FLIGHT at a time and take the
        // same time each, so they complete in batches of that many, one batch
        // per `per_tenant`.
        let batches = (deadline.as_nanos() / per_tenant.as_nanos()) as usize;
        assert_eq!(
            warmed,
            batches * PART_RESOLUTIONS_IN_FLIGHT,
            "every tenant resolved before the deadline is warmed: {}",
            warning[0]
        );
        assert!(warmed > 0 && warmed < SLOW_LISTING_TENANTS);
        assert_eq!(
            cache.len(),
            warmed,
            "each warmed tenant's one part is resident"
        );
        assert_eq!(
            field_values(&warning[0], "step"),
            ["\"part_resolution,whole_part_warm,metadata_pass\""],
            "{}",
            warning[0]
        );
    }

    /// Issue #4257 review: a metric part's warm reads its footer and catalog,
    /// not the object, so it reserves the metrics funnel's footer tail. Four
    /// metric parts, each larger than [`DEFAULT_WHOLE_OBJECT_THRESHOLD`] and
    /// than the whole-part share, all warm, and the share then holds at least
    /// their 4 footer tails and no more than the budget. Seen failing against a reservation of the object
    /// size: every part is skipped over budget, `(0, 4)`.
    #[tokio::test]
    async fn metric_parts_larger_than_the_share_warm_on_their_footer_reservation() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        for k in 1..=4i64 {
            let at = now - k * NS_PER_MIN;
            publish_metric_segment_sized(&memory, tenant, at, at / NS_PER_HOUR, 100_000).await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let (parts, _) =
            most_recent_parts(&catalog_for(store.clone()), tenant, Signal::Metrics, now).await;
        assert_eq!(parts.len(), 4);
        let smallest = parts
            .iter()
            .map(|part| part.object_size)
            .min()
            .expect("parts");
        let budget = smallest - 1;
        assert!(
            smallest > DEFAULT_WHOLE_OBJECT_THRESHOLD && budget >= 4 * DEFAULT_SUFFIX_LEN,
            "each part is above the whole-object threshold and the share, and the share \
             holds 4 footer tails: {smallest} vs {budget}"
        );

        let cache = Arc::new(Cache::new(CacheLimits::new(
            64 * 1024 * 1024,
            1024,
            64 * 1024 * 1024,
        )));
        let fetcher = SegmentFetcher::new(store).with_cache(ReadCache::Ram(cache));
        let progress = WarmProgress::default();
        let warmed = warm_whole_parts(
            WholePartFetcher::Metrics(&fetcher),
            tenant,
            &parts,
            budget,
            &progress,
        )
        .await;
        assert_eq!(
            (warmed.warmed, warmed.over_budget),
            (4, 0),
            "(warmed, over_budget) for 4 metric parts each above a {budget}-byte share"
        );
        let held = progress.whole_part_bytes_reserved.load(Ordering::Relaxed);
        assert!(
            held >= 4 * DEFAULT_SUFFIX_LEN && held <= budget,
            "the share holds the 4 footer reads, inside the budget: {held} of {budget}"
        );
    }

    /// Issue #4257 review: a part that does not fit beside the fetches in
    /// flight waits for them instead of being skipped. Four logs parts are
    /// already cached, so each fetch reads no byte from the store and its
    /// reservation settles to 0. The share is one byte short of two parts:
    /// the second part does not fit while the first is in flight, then fits
    /// once the first settles, and so on for the rest, so all 4 warm. Seen
    /// failing against skipping a part that does not fit at once: `(1, 3)`.
    #[tokio::test]
    async fn a_part_that_does_not_fit_waits_for_the_fetches_in_flight_to_settle() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        for k in 1..=4u8 {
            publish_log_segment_hashed(&memory, tenant, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let (parts, _) =
            most_recent_parts(&catalog_for(store.clone()), tenant, Signal::Logs, now).await;
        assert_eq!(parts.len(), 4);
        let sizes: Vec<u64> = parts.iter().map(|part| part.object_size).collect();
        // Fixture sizes vary by a byte with the encoded timestamps, so the
        // share is one byte short of the smallest consecutive pair, not of
        // the first pair.
        let budget = sizes
            .windows(2)
            .map(|pair| pair[0] + pair[1])
            .min()
            .expect("four parts")
            - 1;
        assert!(
            sizes.iter().all(|size| *size <= budget)
                && sizes.windows(2).all(|pair| pair[0] + pair[1] > budget),
            "each part fits alone and no two consecutive parts fit together: {sizes:?} vs \
             {budget}"
        );

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let fetcher = LogSegmentFetcher::new(store).with_cache(ReadCache::Ram(cache.clone()));
        let prewarmed = warm_whole_parts(
            WholePartFetcher::Logs(&fetcher),
            tenant,
            &parts,
            u64::MAX,
            &WarmProgress::default(),
        )
        .await;
        assert_eq!(prewarmed.warmed, 4);
        assert_eq!(cache.len(), 4);

        let progress = WarmProgress::default();
        let warmed = warm_whole_parts(
            WholePartFetcher::Logs(&fetcher),
            tenant,
            &parts,
            budget,
            &progress,
        )
        .await;
        assert_eq!(
            (warmed.warmed, warmed.over_budget),
            (4, 0),
            "(warmed, over_budget) when every in-flight fetch settles to 0 bytes"
        );
        assert_eq!(
            progress.whole_part_bytes_reserved.load(Ordering::Relaxed),
            0,
            "cache hits hold nothing against the share"
        );
    }

    /// A GET that fetches an object's block/data region, as opposed to the
    /// metadata pass's footer and directory reads. The logs funnel reads a part
    /// whole: either one `GetRange::Full`, or -- for an object above the
    /// fetcher's block-range threshold -- block ranges, the first of which
    /// starts at offset 0 (the data section is at the front of the object). The
    /// metadata pass reads only the tail: a `GetRange::Suffix` footer probe and
    /// `GetRange::Range` directory extents at offsets above 0. So "touches
    /// offset 0" distinguishes a whole/block read from any metadata read,
    /// whether the whole read is `Full` or ranged.
    fn is_block_read(range: GetRange) -> bool {
        matches!(range, GetRange::Full | GetRange::Range(0, _))
    }

    /// Delegates to a `MemoryStore`, recording the `(key, range)` of every GET
    /// (ranged reads included, not only `GetRange::Full`), and delaying any
    /// block read ([`is_block_read`]) whose key is `slow_key` by `delay` on the
    /// paused clock. Recording ranged reads is what lets the test below catch a
    /// whole-part warm that reads an above-bound object by block ranges rather
    /// than one `Full` GET, which a `Full`-only counter would miss.
    struct RecordsGets {
        inner: MemoryStore,
        slow_key: parking_lot::Mutex<String>,
        delay: Duration,
        gets: parking_lot::Mutex<Vec<(String, GetRange)>>,
    }

    impl RecordsGets {
        async fn observe(&self, key: &str, range: GetRange) {
            self.gets.lock().push((key.to_string(), range));
            if is_block_read(range) && *self.slow_key.lock() == key {
                tokio::time::sleep(self.delay).await;
            }
        }

        /// GETs of `key` that fetched its block/data region (`Full` or a range
        /// from offset 0), whatever their count.
        fn block_reads(&self, key: &str) -> Vec<GetRange> {
            self.gets
                .lock()
                .iter()
                .filter(|(k, range)| k == key && is_block_read(*range))
                .map(|(_, range)| *range)
                .collect()
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for RecordsGets {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.observe(key, range).await;
            self.inner.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &ravel_object_store::Pin,
        ) -> Result<ravel_object_store::PinnedRead, StoreError> {
            self.observe(key, range).await;
            self.inner.get_pinned(key, range, pin).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// Issue #4307, the distinguishing test: a tenant with one small logs part
    /// (at or under the plan bound) and one large one above it, sized above the
    /// 512 KiB whole-object threshold so a whole read of it goes out as block
    /// ranges, not one `GetRange::Full`. Any block read of the large object
    /// (whole or ranged, [`is_block_read`]) is delayed 5 s, longer than the 1 s
    /// deadline. After the pass the whole-part step has issued no block GET of
    /// the large object: it read only the small one whole, and the pass
    /// completed inside the deadline because it never waited on a large block
    /// read. The large object's metadata is warmed by the metadata pass, whose
    /// footer-probe and directory reads touch the tail, never offset 0, so they
    /// are not block reads and are not delayed.
    ///
    /// Shown failing against a whole-part warm that reads above-bound parts
    /// (the pre-#4307 behaviour, reproduced by not partitioning the warm set):
    /// the large object's whole read goes out as block ranges, which
    /// `block_reads(&large_key)` now records (a `GetRange::Full`-only counter
    /// would miss them), and that read is delayed past the deadline, so the
    /// `block_reads(&large_key)` assertion fails and the deadline warning fires.
    #[tokio::test(start_paused = true)]
    async fn the_whole_part_warm_issues_no_block_get_for_a_part_above_the_bound() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        // One small part (recent, at or under the bound) and one large part
        // (older, above it and above the 512 KiB whole-object threshold, so a
        // whole read of it is ranged).
        publish_log_segment_hashed(&memory, tenant, now - NS_PER_MIN, [1; 32]).await;
        let at = now - 2 * NS_PER_MIN;
        publish_log_segment_sized(
            &memory,
            tenant,
            at,
            at / NS_PER_HOUR,
            [2; 32],
            ABOVE_WHOLE_OBJECT_FILLER_BYTES,
        )
        .await;

        let store = Arc::new(RecordsGets {
            inner: memory,
            slow_key: parking_lot::Mutex::new(String::new()),
            delay: Duration::from_secs(5),
            gets: parking_lot::Mutex::new(Vec::new()),
        });
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
        let (parts, _) =
            most_recent_parts(&catalog_for(dyn_store.clone()), tenant, Signal::Logs, now).await;
        assert_eq!(parts.len(), 2);
        let small = parts
            .iter()
            .find(|part| part.object_size <= SPLIT_PLAN_BOUND)
            .expect("one small part");
        let large = parts
            .iter()
            .find(|part| part.object_size > SPLIT_PLAN_BOUND)
            .expect("one large part");
        assert!(
            large.object_size > DEFAULT_WHOLE_OBJECT_THRESHOLD,
            "the large part must be above the 512 KiB whole-object threshold so a whole read of it \
             is ranged, not one Full GET: {} vs {DEFAULT_WHOLE_OBJECT_THRESHOLD}",
            large.object_size
        );
        let small_key = small.data_object_key.clone();
        let large_key = large.data_object_key.clone();
        *store.slow_key.lock() = large_key.clone();

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache_within(
            Duration::from_secs(1),
            dyn_store.clone(),
            catalog_for(dyn_store),
            ReadCache::Ram(cache),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            LogsMetadataWarm::new(&split_engine(), 16 * 1024 * 1024),
        )
        .await;

        assert_eq!(
            store.block_reads(&large_key),
            Vec::<GetRange>::new(),
            "the whole-part step issued no block GET (Full or a range from offset 0) for the part \
             above the bound; only the metadata pass's tail reads are allowed"
        );
        assert_eq!(
            store.block_reads(&small_key).len(),
            1,
            "the part at or under the bound was read whole, in one block GET"
        );
        assert_eq!(
            capture.events(DEADLINE_WARNING),
            Vec::<String>::new(),
            "no deadline warning: the pass never waited on a block read of the large object"
        );
        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        for (field, value) in [
            ("parts_warmed", "1"),
            ("parts_above_plan_bound", "1"),
            ("metadata_objects_warmed", "1"),
        ] {
            assert_eq!(
                field_values(&complete[0], field),
                [value],
                "{field}: {}",
                complete[0]
            );
        }
    }

    /// Issue #4307: a logs part whose object size is exactly the plan bound is
    /// warmed whole, not skipped. The engine's bound is set to the one object's
    /// size, so `plan_whole_object_bound` equals it. The whole-part step warms
    /// it (its skip is `object_size > bound`, false at the bound) and the
    /// metadata pass leaves it alone (same test). Seen failing against an
    /// off-by-one that skips at the bound (`object_size >= bound`): the object
    /// is counted above the bound and warmed by neither step, so
    /// `parts_above_plan_bound` is 1 and `cache.len()` 0.
    #[tokio::test(start_paused = true)]
    async fn a_logs_part_exactly_at_the_plan_bound_is_warmed_whole() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_log_segment(&memory, tenant, now).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let (parts, _) =
            most_recent_parts(&catalog_for(store.clone()), tenant, Signal::Logs, now).await;
        assert_eq!(parts.len(), 1);
        let size = parts[0].object_size;

        // The plan bound is exactly this object's size.
        let engine = EngineConfig {
            logs_block_range_threshold: size,
            logs_projection_break_even_bytes: Some(size),
            ..EngineConfig::default()
        };
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache(
            store.clone(),
            catalog_for(store),
            ReadCache::Ram(cache.clone()),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            LogsMetadataWarm::new(&engine, 16 * 1024 * 1024),
        )
        .await;

        assert_eq!(
            cache.len(),
            1,
            "the part exactly at the bound is warmed whole"
        );
        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        for (field, value) in [
            ("parts_warmed", "1"),
            ("parts_above_plan_bound", "0"),
            ("metadata_objects_warmed", "0"),
        ] {
            assert_eq!(
                field_values(&complete[0], field),
                [value],
                "{field}: {}",
                complete[0]
            );
        }
    }

    const NONE_WARMED_WARNING: &str =
        "cache warmup: tenant has parts for this signal but none were warmed";

    /// Issue #4307 item 3: the "has parts but none warmed" `warn!` keys on the
    /// at-or-under-bound candidate set, not on the whole signal. An above-bound
    /// part must not suppress it when every at-or-under fetch failed, and a
    /// signal whose parts are all above the bound (no whole candidates) must not
    /// raise it. Driven through `log_warm_result` directly so the condition is
    /// the only thing under test.
    ///
    /// Shown failing against the pre-fix condition (`warmed == 0 && over_budget
    /// == 0 && above_plan_bound == 0 && has_ingest_part`): the first call's
    /// `above_plan_bound = 1` makes that condition false, so the all-candidates-
    /// failed case logs at `info!` and the warning never fires, leaving
    /// `events(NONE_WARMED_WARNING).len()` at 0, not 1.
    #[test]
    fn an_above_bound_part_does_not_mask_the_none_warmed_warning() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        let tenant = TenantId::new("acme").hash();

        // Two at-or-under-bound candidates, both fetches failed (none warmed,
        // none over budget, none dropped at the cap), with one part above the
        // bound. The above-bound part must not mask the failure.
        let masked = WholePartsWarmed {
            warmed: 0,
            over_budget: 0,
            above_plan_bound: 1,
            beyond_cap: 0,
            whole_candidates: 2,
        };
        log_warm_result(tenant, Signal::Logs, masked, Some((7, true)));

        // A signal whose parts are all above the bound: no whole candidates, so
        // warming nothing whole is expected and must stay at `info!`.
        let all_above = WholePartsWarmed {
            warmed: 0,
            over_budget: 0,
            above_plan_bound: 3,
            beyond_cap: 0,
            whole_candidates: 0,
        };
        log_warm_result(tenant, Signal::Logs, all_above, Some((7, true)));

        assert_eq!(
            capture.events(NONE_WARMED_WARNING).len(),
            1,
            "an above-bound part must not mask the none-warmed warning, and an all-above-bound \
             signal must not raise it: {:?}",
            capture.events(NONE_WARMED_WARNING)
        );
    }

    /// Issue #4307 item 2: a logs candidate at or under the plan bound that the
    /// per-(tenant, signal) cap drops is warmed by neither step, so it is
    /// counted in `parts_beyond_cap` on the per-signal and pass-complete lines.
    /// With five small parts and a four-part cap, one is dropped. The counts
    /// then reconcile against the five objects: 4 warmed + 1 beyond the cap + 0
    /// over budget + 0 above the bound.
    ///
    /// Shown failing against an implementation that does not count the dropped
    /// candidate (the field stays 0): `parts_beyond_cap` reads 0, not 1, on both
    /// lines, and the five objects no longer reconcile.
    #[tokio::test(start_paused = true)]
    async fn a_candidate_dropped_at_the_cap_is_counted_in_parts_beyond_cap() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        // Five small logs parts, all at or under the plan bound, all in the
        // warm window. The four-part cap warms four and drops the fifth.
        for k in 1..=5u8 {
            publish_log_segment_hashed(&memory, tenant, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);

        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        warm_cache_within(
            Duration::from_secs(30),
            store.clone(),
            catalog_for(store),
            ReadCache::Ram(cache),
            &FixedClock(now),
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            LogsMetadataWarm::new(&split_engine(), 16 * 1024 * 1024),
        )
        .await;

        let complete = capture.events(PASS_COMPLETE);
        assert_eq!(complete.len(), 1, "one pass-complete line: {complete:?}");
        for (field, value) in [
            ("parts_warmed", "4"),
            ("parts_beyond_cap", "1"),
            ("parts_above_plan_bound", "0"),
            ("metadata_objects_warmed", "0"),
        ] {
            assert_eq!(
                field_values(&complete[0], field),
                [value],
                "{field}: {}",
                complete[0]
            );
        }

        let logs_line: Vec<String> = capture
            .events("cache warmup: warmed tenant signal")
            .into_iter()
            .filter(|event| event.contains(" signal=Logs "))
            .collect();
        assert_eq!(
            logs_line.len(),
            1,
            "one logs per-signal line: {logs_line:?}"
        );
        for (field, value) in [
            ("parts_warmed", "4"),
            ("parts_beyond_cap", "1"),
            ("parts_over_budget", "0"),
            ("parts_above_plan_bound", "0"),
        ] {
            assert_eq!(
                field_values(&logs_line[0], field),
                [value],
                "{field}: {}",
                logs_line[0]
            );
        }
    }
}
