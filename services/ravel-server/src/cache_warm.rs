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
//! within an hour, by greatest `max_event_ts_ns` first, and both bounds below
//! take parts in that order. "Ingest hour" here means any hour holding a
//! commit-record-shaped key of any kind (a raw commit record, a compaction
//! record, a rewrite record, or a tombstone): a tombstone-only or
//! rewrite-only hour can anchor the window exactly as a fresh commit record
//! would.
//!
//! Bounded on two axes. Per (tenant, signal), at most
//! [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] of the most recent parts are warmed
//! whole: this pass exists to take the edge off a cold restart, not to size
//! a working set only real query traffic can size correctly. The whole pass
//! is wrapped in `tokio::time::timeout(`[`WARM_DEADLINE`]`, ..)`, so a large
//! tenant count or a slow store can never hold `/readyz` back indefinitely.
//!
//! The logs metadata pass (ADR-4203 decision 8) is not bounded by that part
//! cap, and runs only after every tenant's whole-part warm has finished, so
//! it spends only the deadline time that warm leaves. For every logs
//! object in the warm set that the query path plans from its
//! directories (above [`LogSegmentFetcher::plan_whole_object_bound`]), it
//! reads the footer and the four directories (STREAM_DIR, FIELD_DIR,
//! SKIP_IDX, PAGE_DIR) through [`BlockRangeFetcher::fetch_plan_directories`],
//! on a fetcher built from the query path's own logs fetch quantities
//! ([`LogsMetadataWarm`]): the suffix probe, then, for sections the probe
//! does not hold whole, one coalesced read of the missing SKIP_IDX and
//! PAGE_DIR and one of the missing STREAM_DIR and FIELD_DIR. Those are the
//! extents and cache keys an unkeyed plan read of the same object uses. A
//! keyed plan (`fetch_keyed_plan`, a statement with key-equality arms) hits
//! the same probe key, but also reads KEY_IDX and the BLOOM and POSTINGS
//! sections its filter needs, none of which the pass warms. A BLOOM or
//! POSTINGS section outside the probe and within the coalesce gap of the
//! missing SKIP_IDX and PAGE_DIR joins them in one read, whose extent then
//! differs from the warmed one and misses the cache. The pass reads no column
//! page. Its ceiling is [`METADATA_CACHE_SHARE_DIVISOR`]'s share of the cache
//! budget, charged only with the bytes the pass's own reads fetch from the
//! store: the whole-part warm's residents do not consume it, and an object
//! already cached charges nothing. It stops admitting objects once the bytes
//! its completed reads fetched plus its own probe length would pass the
//! ceiling, and runs at most the store GET concurrency's permit count of
//! objects at once.
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
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt as _};
use ravel_catalog::{Catalog, SegmentRef};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_query::{EngineConfig, LogQuery, LogSegmentFetcher, ReadCache, SegmentFetcher};
use ravel_types::{Signal, TenantHash};

/// Per (tenant, signal), the number of most-recent parts warmed whole.
const MAX_PARTS_PER_TENANT_PER_SIGNAL: usize = 4;

/// The logs metadata pass's ceiling is `1 / METADATA_CACHE_SHARE_DIVISOR` of
/// the read cache's byte budget, charged with the bytes the pass itself
/// fetches. The rest is left for the column pages the first statements insert:
/// a full cache would evict warmed metadata a statement has not read yet.
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
/// signal. A single deadline over the whole loop, not a per-fetch one: a
/// store with many tenants must not warm zero of them just because the
/// first several were slow. The logs metadata pass starts only after every
/// tenant's whole-part warm, so it spends only the time that warm leaves.
pub(crate) const WARM_DEADLINE: Duration = Duration::from_secs(10);

/// How many ingest hours before the tenant's own latest ingest hour the warm
/// set reaches back: the parts filed in `[latest - WARM_LOOKBACK_HOURS,
/// latest]`, whatever their event times. Measured from the latest ingest hour,
/// not wall-clock `now` (issue #1233, ADR-0046 amendment 2026-09-05), and in
/// ingest hours, not event time (issue #4257). Wide enough to find a recent
/// part for a slow ingest cadence without resolving a tenant's entire history.
const WARM_LOOKBACK_HOURS: u32 = 24;

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
    let now_ns = clock.now_ns();
    match tokio::time::timeout(
        WARM_DEADLINE,
        warm_cache_inner(
            store,
            catalog,
            cache,
            now_ns,
            get_limiter,
            memory_budget,
            read_gate,
            logs_metadata,
        ),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            tracing::warn!(
                deadline_secs = WARM_DEADLINE.as_secs(),
                "cache warmup did not finish before its deadline; continuing to readiness with \
                 whatever it warmed so far"
            );
        }
    }
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
) {
    let started = tokio::time::Instant::now();
    let tenants = match ravel_maintain::discover_tenants(store.as_ref()).await {
        Ok(tenants) => tenants,
        Err(err) => {
            tracing::warn!(error = %err, "cache warmup: tenant discovery failed; skipping warmup");
            return;
        }
    };

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
    let logs_fetcher = LogSegmentFetcher::new(store.clone())
        .with_cache(cache.clone())
        .with_get_limiter(get_limiter.clone())
        .with_memory_budget(memory_budget.clone())
        .with_read_gate(read_gate.clone());
    let mut metadata_pass = LogsMetadataPass::new(
        store,
        cache,
        get_limiter,
        memory_budget,
        read_gate,
        logs_metadata,
    );

    // Every tenant's whole-part warm runs before any metadata pass, so the
    // uncapped pass of one tenant cannot spend the deadline a later tenant's
    // whole-part warm needs. Each tenant keeps only the parts its pass probes.
    let mut total_parts_warmed: usize = 0;
    let mut planned_windows: Vec<(TenantHash, Vec<SegmentRef>)> = Vec::new();
    for tenant in tenants {
        total_parts_warmed += warm_metrics(&catalog, &metrics_fetcher, tenant, now_ns).await;
        let (warmed, mut parts) = warm_logs(&catalog, &logs_fetcher, tenant, now_ns).await;
        total_parts_warmed += warmed;
        if let Some(pass) = metadata_pass.as_ref() {
            parts.retain(|part| pass.fetcher.plans_segment(part));
            if !parts.is_empty() {
                parts.shrink_to_fit();
                planned_windows.push((tenant, parts));
            }
        }
    }
    if let Some(pass) = metadata_pass.as_mut() {
        for (tenant, planned) in &planned_windows {
            let warmed = pass.warm(*tenant, planned).await;
            tracing::info!(
                tenant_hash = %tenant.to_hex(),
                signal = ?Signal::Logs,
                metadata_objects_warmed = warmed.objects as u64,
                metadata_bytes_warmed = warmed.bytes,
                "cache warmup: warmed tenant logs metadata"
            );
        }
    }

    let metadata = metadata_pass
        .as_ref()
        .map_or_else(MetadataWarmed::default, |pass| pass.total);
    tracing::info!(
        parts_warmed = total_parts_warmed,
        metadata_objects_warmed = metadata.objects,
        metadata_bytes_warmed = metadata.bytes,
        metadata_budget_reached = metadata_pass
            .as_ref()
            .is_some_and(|pass| pass.budget_reached),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "cache warmup: pass complete"
    );
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
    /// fetcher refuses: the pass is skipped, never startup.
    fn new(
        store: Arc<dyn ObjectStoreBackend>,
        cache: ReadCache,
        get_limiter: Arc<ravel_query::GetLimiter>,
        memory_budget: Arc<ravel_memory::MemoryBudget>,
        read_gate: Arc<ravel_cpu_gate::ReadGate>,
        shape: LogsMetadataWarm,
    ) -> Option<Self> {
        let concurrency = get_limiter.permits().max(1);
        // Built like the query path's logs fetchers (`QueryEngine::new` and
        // `build_sql_state`): the request cost sets the coalescing gap and the
        // two thresholds set the plan bound, so any other value here would
        // read the directories under keys no statement looks up.
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
    /// failed read skips that object.
    async fn warm(&mut self, tenant: TenantHash, planned: &[SegmentRef]) -> MetadataWarmed {
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
/// the tenant is known to have an ingest part for this signal at its latest
/// hour but none were warmed: that combination means a resolve or fetch
/// failure ate the whole signal, not that there was nothing to warm. When
/// the latest hour is anchored by a tombstone- or rewrite-only bucket with
/// no ingest part of its own (`has_ingest_part = false`), an empty warm is
/// the expected outcome (the catalog drops a tombstoned bucket outright),
/// so this logs at `info!` instead of `warn!` for that case.
fn log_warm_result(
    tenant: TenantHash,
    signal: Signal,
    parts_warmed: usize,
    latest_hour: Option<(u32, bool)>,
) {
    let latest_ingest_hour =
        latest_hour.map_or_else(|| "none".to_string(), |(hour, _)| hour.to_string());
    let has_ingest_part = latest_hour.is_some_and(|(_, has_ingest_part)| has_ingest_part);
    if parts_warmed == 0 && has_ingest_part {
        tracing::warn!(
            tenant_hash = %tenant.to_hex(),
            signal = ?signal,
            parts_warmed,
            latest_ingest_hour,
            "cache warmup: tenant has parts for this signal but none were warmed"
        );
    } else {
        tracing::info!(
            tenant_hash = %tenant.to_hex(),
            signal = ?signal,
            parts_warmed,
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

async fn warm_metrics(
    catalog: &Catalog,
    fetcher: &SegmentFetcher,
    tenant: TenantHash,
    now_ns: i64,
) -> usize {
    let (parts, latest_hour) = most_recent_parts(catalog, tenant, Signal::Metrics, now_ns).await;
    let (hour, has_ingest_part) = match latest_hour {
        LatestHour::Error => return 0,
        LatestHour::None => {
            log_warm_result(tenant, Signal::Metrics, 0, None);
            return 0;
        }
        LatestHour::Found {
            hour,
            has_ingest_part,
        } => (hour, has_ingest_part),
    };

    let mut warmed = 0usize;
    for part in parts.iter().take(MAX_PARTS_PER_TENANT_PER_SIGNAL) {
        match fetcher
            .fetch_series_accounted(tenant, part, &[], &warm_accounting())
            .await
        {
            Ok(_) => warmed += 1,
            Err(err) => {
                tracing::debug!(
                    tenant_hash = %tenant.to_hex(),
                    key = %part.data_object_key,
                    error = %err,
                    "cache warmup: metric part fetch failed; skipping"
                );
            }
        }
    }

    log_warm_result(
        tenant,
        Signal::Metrics,
        warmed,
        Some((hour, has_ingest_part)),
    );
    warmed
}

/// Warms the `MAX_PARTS_PER_TENANT_PER_SIGNAL` most recent logs parts whole.
/// Returns the count of parts warmed whole and every part in the warm set, in
/// [`most_recent_parts`]'s order, for the metadata pass that runs after every
/// tenant's whole-part warm.
async fn warm_logs(
    catalog: &Catalog,
    fetcher: &LogSegmentFetcher,
    tenant: TenantHash,
    now_ns: i64,
) -> (usize, Vec<SegmentRef>) {
    let (parts, latest_hour) = most_recent_parts(catalog, tenant, Signal::Logs, now_ns).await;
    let (hour, has_ingest_part) = match latest_hour {
        LatestHour::Error => return (0, parts),
        LatestHour::None => {
            log_warm_result(tenant, Signal::Logs, 0, None);
            return (0, parts);
        }
        LatestHour::Found {
            hour,
            has_ingest_part,
        } => (hour, has_ingest_part),
    };

    let mut warmed = 0usize;
    for part in parts.iter().take(MAX_PARTS_PER_TENANT_PER_SIGNAL) {
        let query = LogQuery::new(part.min_event_ts_ns, part.max_event_ts_ns);
        match fetcher
            .fetch_accounted_with_tenant(part, tenant, &query, &warm_accounting())
            .await
        {
            Ok(_) => warmed += 1,
            Err(err) => {
                tracing::debug!(
                    tenant_hash = %tenant.to_hex(),
                    key = %part.data_object_key,
                    error = %err,
                    "cache warmup: log part fetch failed; skipping"
                );
            }
        }
    }

    log_warm_result(tenant, Signal::Logs, warmed, Some((hour, has_ingest_part)));
    (warmed, parts)
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
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "http_requests_total".to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(&TenantId::new("acme"), "http_requests_total", &label_set)
                .expect("series id"),
            labels: label_set,
            samples: vec![Sample {
                ts_ns: event_ts_ns,
                value: 42.0,
            }],
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
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: event_ts_ns,
            ingest_hour_bucket: u32::try_from(event_ts_ns / NS_PER_HOUR).expect("hour bucket"),
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
        use ravel_logseg::{
            AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
        };
        use ravel_types::logstream::log_stream_id;

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
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs,
                ts_ns: event_ts_ns,
                observed_ts_ns: event_ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: "checkout completed".to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
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
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: event_ts_ns,
            max_event_ts_ns: event_ts_ns,
            min_ingest_ts_ns: event_ts_ns,
            max_ingest_ts_ns: event_ts_ns,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: event_ts_ns,
            ingest_hour_bucket: u32::try_from(event_ts_ns / NS_PER_HOUR).expect("hour bucket"),
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
    /// under `hang_prefix` never completes, and records every whole-object GET
    /// and every suffix GET. The whole-part warm reads small logs objects with
    /// [`GetRange::Full`]; the metadata pass opens each object with a suffix
    /// probe.
    struct HangsOnTenantProbe {
        inner: MemoryStore,
        hang_prefix: String,
        full_gets: parking_lot::Mutex<Vec<String>>,
        suffix_gets: parking_lot::Mutex<Vec<String>>,
        hung_probes: std::sync::atomic::AtomicUsize,
    }

    impl HangsOnTenantProbe {
        fn new(inner: MemoryStore, hang_prefix: String) -> Self {
            Self {
                inner,
                hang_prefix,
                full_gets: parking_lot::Mutex::new(Vec::new()),
                suffix_gets: parking_lot::Mutex::new(Vec::new()),
                hung_probes: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        async fn read(&self, key: &str, range: GetRange) {
            if matches!(range, GetRange::Suffix(_)) {
                self.suffix_gets.lock().push(key.to_string());
            }
            if key.starts_with(&self.hang_prefix) && matches!(range, GetRange::Suffix(_)) {
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

    /// The tenant discovered first has five logs parts and the second one.
    /// The whole-part warm reads the first tenant's 4 most recent parts and
    /// the second tenant's part; the first tenant's metadata pass then finds
    /// those 4 probes resident (a fixture object is no bigger than its probe,
    /// so the probe key is the whole-object key) and hangs on the fifth
    /// part's suffix probe until [`WARM_DEADLINE`] fires. The second tenant's
    /// part is still warmed whole: every tenant's whole-part warm runs before
    /// any metadata pass. A pass run inside each tenant's turn spends the
    /// deadline before the second tenant's turn, and the whole-read count
    /// below is `(4, 0)`.
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
        // The first tenant's oldest part is outside the whole-part window of 4.
        for k in 2..=5u8 {
            publish_log_segment_hashed(&memory, first, now - i64::from(k) * NS_PER_MIN, [k; 32])
                .await;
        }
        let logs_prefix = |tenant: TenantHash| format!("t/{}/l/l0/", tenant.to_hex());

        let store = Arc::new(HangsOnTenantProbe::new(memory, logs_prefix(first)));
        let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
        let catalog = catalog_for(dyn_store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        // Every fixture object is above this plan bound, so the metadata pass
        // reads each one.
        let engine = EngineConfig {
            logs_block_range_threshold: 1,
            logs_projection_break_even_bytes: Some(1),
            ..EngineConfig::default()
        };
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
            1,
            "the first tenant's metadata pass reached its probe and hung there"
        );
        let full_gets = store.full_gets.lock().clone();
        let read_whole = |tenant: TenantHash| {
            full_gets
                .iter()
                .filter(|key| key.starts_with(&logs_prefix(tenant)))
                .count()
        };
        assert_eq!(
            (read_whole(first), read_whole(second)),
            (4, 1),
            "each tenant's most recent logs parts were read whole: {full_gets:?}"
        );
        assert_eq!(
            cache.len(),
            5,
            "the 5 whole parts are resident, and the hung pass inserted nothing"
        );
    }

    /// The metadata pass's ceiling counts only the bytes the pass itself
    /// fetches. One tenant has six logs parts: the whole-part warm reads the
    /// 4 most recent whole, more bytes than the pass's ceiling, and the pass
    /// then finds those 4 probes resident (a fixture object is no bigger than
    /// its probe) and probes the 2 older parts. With the ceiling at exactly
    /// the 2 older parts' sizes both are admitted; one byte lower, only the
    /// first is. A ceiling charged with the whole-part warm's resident bytes
    /// admits neither.
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
            warm_cache(
                dyn_store.clone(),
                catalog_for(dyn_store.clone()),
                ReadCache::Ram(cache.clone()),
                &FixedClock(now),
                Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                LogsMetadataWarm::new(&engine, ceiling * METADATA_CACHE_SHARE_DIVISOR),
            )
            .await;

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
}
