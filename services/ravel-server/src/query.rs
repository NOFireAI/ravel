//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`,
//! and, behind the `sql` feature, the `SqlState` for `/api/v1/sql`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ravel_cache::CacheLimits;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{EngineConfig, GetLimiter, QueryAdmissionController, QueryEngine, ReadCache};
use ravel_types::accounting::{AccountedOp, CostEstimate, QueryAccountingSnapshot};

use crate::ServerConfig;

/// The per-query `stats` object attached beside a query response's data
/// (ADR-0044 sections 1 and 3): this query's actual accounting
/// counters and its pre-execution cost estimate, rendered as camelCase JSON to
/// match the field names `ravel-query`'s PromQL `stats.accounting`/`stats.estimate`
/// already use (crates/ravel-query/src/http/json.rs).
///
/// It deliberately omits the `rawF64Pages`/`rawF64Bytes` and
/// `segmentsFetched`/`segmentsPruned` fields the PromQL shape carries: those
/// come from `ravel-query`'s internal per-segment `FetchStats`/`QueryStats`,
/// which the SQL executor's `SqlOutcome` and the analytics range call do not
/// surface. The accounting snapshot and the cost estimate are the shape every
/// query path can supply, so both server-owned handlers report exactly that,
/// and the divergence between the estimate and the actual stays computable from
/// the response as well as from `/metrics`.
pub fn accounting_stats_json(
    accounting: &QueryAccountingSnapshot,
    peak_asked_intermediate_bytes: u64,
    estimate: &CostEstimate,
) -> serde_json::Value {
    serde_json::json!({
        "accounting": {
            "s3GetRequests": accounting.s3_requests(AccountedOp::Get),
            "s3GetBytes": accounting.s3_bytes(AccountedOp::Get),
            "s3ListRequests": accounting.s3_requests(AccountedOp::List),
            "s3ListBytes": accounting.s3_bytes(AccountedOp::List),
            "s3HeadRequests": accounting.s3_requests(AccountedOp::Head),
            "s3HeadBytes": accounting.s3_bytes(AccountedOp::Head),
            "cacheHits": accounting.cache_hits,
            "cacheMisses": accounting.cache_misses,
            "cacheBytes": accounting.cache_bytes,
            "lateServedBytes": accounting.late_served_bytes,
            "decompressedBytes": accounting.decompressed_bytes,
            "segmentsOpened": accounting.segments_opened,
            "seriesMatched": accounting.series_matched,
            "bytesReused": accounting.bytes_reused,
            "peakIntermediateBytes": accounting.peak_intermediate_bytes,
            "peakAskedIntermediateBytes": peak_asked_intermediate_bytes,
        },
        "estimate": {
            "estimatedRequests": estimate.estimated_requests,
            "estimatedStoreBytes": estimate.estimated_store_bytes,
            "estimatedDecompressedBytes": estimate.estimated_decompressed_bytes,
            "segments": estimate.segments,
            "series": estimate.series,
        },
    })
}

/// Render the per-slice `stats.fragments[]` array for a distributed query
/// (ADR-0071 observability deliverable): one object per dispatched
/// slice, in camelCase to match the rest of the stats JSON. A query handler
/// attaches this beside `accounting`/`estimate` only when a distributed run
/// collected entries; a non-distributed query collects none, so the field is
/// absent entirely. Each entry carries the slice's worker endpoint, its pinned
/// segment count, the store bytes the worker reported, the response frame bytes
/// the coordinator accepted off the wire for it (issue #1687 part B), and the
/// routing outcome (`ok` / `fallback` / `error`). No per-shard cardinality
/// beyond the entries themselves: this is the response body, not the metric
/// allowlist.
pub fn fragments_json(entries: &[crate::distrib::FragmentStatEntry]) -> serde_json::Value {
    serde_json::Value::Array(
        entries
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "workerEndpoint": entry.worker_endpoint,
                    "segmentCount": entry.segment_count,
                    "bytesReported": entry.bytes_reported,
                    "wireBytesConsumed": entry.wire_bytes_consumed,
                    "status": entry.status,
                })
            })
            .collect(),
    )
}

/// Builds the shared [`Catalog`] used both for query resolve and for the
/// background fold task: one instance
/// per process so its decoded HEAD/part caches serve both paths.
///
/// `disable_cache` is the CLI's `--disable-cache`, the same flag that governs
/// the fetcher cache in [`crate::store::build_cache`]. `cache_max_bytes` here
/// is `ServerConfig::catalog_cache_max_bytes`: the catalog byte cache's OWN
/// resolved budget, from `--catalog-cache-max-bytes` or its own derived share
/// (ADR-2023) -- a separate ceiling from the fetcher cache's
/// `--cache-max-bytes`, which does not reach this parameter. `--disable-cache`
/// builds a catalog with no byte cache at all (the `byte_cache_max_bytes: 0`
/// sentinel), so a memory-constrained `--disable-cache` deployment no longer
/// silently keeps a 512 MiB catalog byte cache; otherwise `cache_max_bytes` is
/// the catalog byte cache's total budget. The other two byte-cache bounds keep
/// their catalog defaults (the CLI has no flag for them).
///
/// The column-statistics cache keeps the catalog's default ceiling here;
/// [`build_catalog_for_server`] passes the server's resolved
/// `--column-stats-cache-max-bytes` instead (ADR-1413 decision 5). Either way
/// `--disable-cache` builds it with its `0` disabled sentinel.
///
/// `cache_dir` is the CLI's `--cache-dir` (#97). When present and the byte cache
/// is not disabled, the catalog byte cache gains an ADR-0046 local-disk tier at
/// that path. Its [`CacheLimits`] reuse the SAME `cache_max_bytes` number for
/// both the RAM and disk tiers (there is no separate disk-tier capacity flag),
/// with this crate's own byte-cache entry-count/max-entry-bytes defaults. The
/// fetcher cache's own disk tier is sized from `--cache-max-bytes` instead
/// (a different cache; see [`crate::store::build_cache`]). `--disable-cache`
/// (the `0` sentinel) wins over a configured `cache_dir`: no cache of either
/// tier is built.
///
/// `resolve_ceiling` is the per-process ceiling on resolve-path object-store
/// requests (ADR-1733 decision 2). It is a resolved value, not a raw flag:
/// `ravel-server` derives it from the process's query concurrency and passes
/// the derived number here, and an explicit `--catalog-resolve-concurrency`
/// reaches this parameter only because it won that resolution. `None` leaves
/// `ravel_catalog::CatalogConfig`'s own default in place, which is the
/// per-prefix bound, and is the path callers with no query concurrency to
/// derive from take. The per-prefix bound is not exposed here: it has no flag,
/// and a request holds a permit of each.
///
/// `max_ingest_lag_ns` overrides the catalog listing window, `None` keeping
/// `CatalogConfig`'s own 2h default; see [`crate::resolve_ingest_lag`].
///
/// `max_flush_delay` is the server's resolved `--max-flush-delay` (issue
/// #1735), the same cadence every ingest pipeline flushes on. It sizes
/// `cache_capacity_per_tenant` via
/// [`ravel_catalog::derive_cache_capacity_per_tenant`]: `shard_count * 6
/// signals * ceil(3600 / max_flush_delay_secs) * 3 unsealed hours`, clamped
/// between the 10,000-entry floor and the cap that holds one actively-queried
/// tenant to 45 MB across the two record caches the bound sizes. Neither half
/// of that 45 MB is an entry count: both record types hold a repeated field
/// the format does not cap (a commit record's declared column statistics, a
/// compaction record's input list), so each cache is bounded in BYTES at an
/// equal per-cache share of 22.5 MB
/// ([`ravel_catalog::CatalogConfig::commit_cache_max_bytes_per_tenant`] and
/// [`ravel_catalog::CatalogConfig::compaction_cache_max_bytes_per_tenant`]),
/// with the entry count capped at the capacity as a second bound.
/// So the record caches hold as much of a tenant's unsealed hot region, across
/// every signal it ingests, as the cap covers, rather than a flat cadence-blind
/// constant, and a wide deployment still cannot make one tenant cost
/// gigabytes. It is up to 25,000 records of tail, not the whole tail: the cap
/// binds before the estimate at the shipped defaults, the cadence term
/// counts the age flush trigger only, so a size-triggered tenant seals more
/// records per shard-hour than it assumes, the three unsealed hours assume
/// the seal parameters of the catalog this server folds with
/// ([`server_catalog_config_base`]), which no flag moves today
/// (`--gc-max-flush-lifetime` feeds `CompactorConfig::max_flush_lifetime_ns`,
/// not the catalog's), and the capacity is an entry CAP rather than a guaranteed
/// residency: a tenant whose records carry declared column statistics hits its
/// byte budget first and holds proportionally fewer. See
/// [`ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT`] for both limits and
/// the memory figures. Callers must pass the
/// configured value, never
/// `ravel_ingest::IngestConfig::default().max_flush_delay`, or the derived
/// capacity stops tracking the deployment's actual flush cadence. At the
/// shipped defaults (4 shards, 2s) the cap is what binds, so this is 25,000
/// entries, and at that cadence it binds for every shard count; the shard and
/// cadence terms only decide the value at coarser cadences.
///
/// `disable_cache` skips the derivation and leaves the capacity at
/// [`ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT`]. The flag is
/// documented as the one for a memory-constrained container, so it must not
/// be the path that raises record-cache memory; it does not turn the record
/// caches off, which is why it lands on the floor and not on `0`.
///
/// The derivation runs under the default
/// [`ravel_catalog::MAX_RECORD_CACHE_BYTES_PER_TENANT`] budget.
/// [`build_catalog_for_server`] is the path that applies
/// `--catalog-record-cache-max-bytes-per-tenant` (ADR-3513).
#[allow(clippy::too_many_arguments)]
pub fn build_catalog(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    disable_cache: bool,
    cache_max_bytes: u64,
    cache_dir: Option<PathBuf>,
    resolve_ceiling: Option<usize>,
    max_ingest_lag_ns: Option<i64>,
    max_flush_delay: Duration,
) -> anyhow::Result<Arc<Catalog>> {
    let base = server_catalog_config_base();
    let column_stats_cache_max_bytes = base.column_stats_cache_max_bytes;
    build_catalog_from(
        base,
        store,
        shard_count,
        disable_cache,
        cache_max_bytes,
        column_stats_cache_max_bytes,
        cache_dir,
        resolve_ceiling,
        max_ingest_lag_ns,
        max_flush_delay,
        ravel_catalog::MAX_RECORD_CACHE_BYTES_PER_TENANT,
    )
}

/// [`build_catalog`] over `base` instead of [`server_catalog_config_base`],
/// with the record-cache entry bound derived under
/// `record_cache_max_bytes_per_tenant` (ADR-3513) instead of the default
/// budget.
#[allow(clippy::too_many_arguments)]
fn build_catalog_from(
    base: CatalogConfig,
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    disable_cache: bool,
    cache_max_bytes: u64,
    column_stats_cache_max_bytes: u64,
    cache_dir: Option<PathBuf>,
    resolve_ceiling: Option<usize>,
    max_ingest_lag_ns: Option<i64>,
    max_flush_delay: Duration,
    record_cache_max_bytes_per_tenant: u64,
) -> anyhow::Result<Arc<Catalog>> {
    // `0` is the byte cache's disabled sentinel (ravel_catalog::CatalogConfig):
    // Catalog::new then constructs no byte cache. Mirrors how build_cache turns
    // --disable-cache into a `None` fetcher cache.
    let byte_cache_max_bytes = if disable_cache { 0 } else { cache_max_bytes };
    // The column-statistics cache has the same `0` sentinel, and
    // `memory_hard_caps_bytes` charges it nothing under --disable-cache.
    let column_stats_cache_max_bytes = if disable_cache {
        0
    } else {
        column_stats_cache_max_bytes
    };
    // `--disable-cache` is documented as the flag for a memory-constrained
    // container, so it must not raise record-cache memory: under it the
    // capacity stays at the flat floor the cache ran on before the derivation
    // existed. The record caches are not ADR-0046 read caches and the flag does
    // not turn them off; a `0` capacity would put every resolve back on a
    // per-record GET, which is a resolve-path change the flag does not promise.
    let cache_capacity_per_tenant = if disable_cache {
        ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT
    } else {
        ravel_catalog::derive_cache_capacity_per_tenant_within(
            shard_count,
            max_flush_delay,
            record_cache_max_bytes_per_tenant,
        )
    };
    let mut catalog_config = CatalogConfig {
        shard_count,
        byte_cache_max_bytes,
        column_stats_cache_max_bytes,
        cache_capacity_per_tenant,
        ..base
    };
    // Override the catalog listing window; see `crate::resolve_ingest_lag`.
    if let Some(ns) = max_ingest_lag_ns {
        catalog_config.max_ingest_lag_ns = ns;
    }
    // The per-process ceiling on resolve-path object-store requests (ADR-1733
    // decision 2). `ravel-server` derives it from the process's query
    // concurrency and passes it here, so the production catalog never runs on
    // the constant; `None` leaves `CatalogConfig`'s own default, which is the
    // per-prefix bound, and is the path CLI, bench and test callers that know
    // no `Q` take.
    if let Some(ceiling) = resolve_ceiling {
        catalog_config.resolve_get_concurrency = ceiling;
    }
    // Durable shard_count enforcement on the read path (ADR-0050 section 5,
    // EC5): the first resolve for each (tenant, signal) validates this
    // catalog's configured shard_count against the tenant's provisioning
    // record, so a query never silently resolves over a subset of shards. The
    // check is read-only (it never writes a record), so a query-only node with
    // write-restricted credentials is unaffected.
    let catalog = Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    // #97: attach the ADR-0046 disk tier to the byte cache when --cache-dir is
    // set and the byte cache is enabled. Both tiers reuse the same
    // cache_max_bytes number and this crate's byte-cache entry/entry-byte
    // defaults, not the ravel-server fetcher constants (a different cache).
    // `with_disk_byte_cache` is a no-op when the byte cache is disabled, so the
    // `byte_cache_max_bytes != 0` guard here matches its own disabled-wins rule.
    let catalog = match cache_dir {
        Some(dir) if byte_cache_max_bytes != 0 => {
            let limits = CacheLimits::new(
                cache_max_bytes,
                ravel_catalog::DEFAULT_BYTE_CACHE_MAX_ENTRIES,
                ravel_catalog::DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES,
            );
            catalog.with_disk_byte_cache(limits, dir, limits)
        }
        _ => catalog,
    };
    Ok(Arc::new(catalog))
}

/// The `CatalogConfig` the server's catalog starts from, before the fields
/// [`build_catalog`] derives from a deployment's flags.
///
/// The fold's three seal durations (`max_flush_lifetime_ns`,
/// `clock_skew_allowance_ns`, `fold_safety_margin_ns`) are decided here and
/// nowhere else on the server's path: [`build_catalog`] overrides none of
/// them, so their sum is the seal margin the fold and resolve actually run
/// with (ADR-1306 decision 3). The derived per-query request budget reads that
/// margin off [`server_seal_margin`], which reads it off this function, so
/// there is no second copy of the three durations on the budget path and a
/// flag that later sets one of them here carries the budget with it.
pub fn server_catalog_config_base() -> CatalogConfig {
    CatalogConfig::default()
}

/// The seal margin of the catalog [`build_catalog`] constructs, for the
/// derived per-query S3 request budget (ADR-1306 decision 3).
///
/// [`crate::config::Cli::resolve_max_s3_requests`] passes this to
/// `ravel_query::derive_max_s3_requests_for` instead of letting the derivation
/// fall back to `SealMargin::REFERENCE`, so the span the budget covers is the
/// one the running catalog's fold seals against.
/// `derived_request_budget_uses_the_catalogs_seal_margin` fails if this and
/// the catalog [`build_catalog_for_server`] returns ever diverge.
pub fn server_seal_margin() -> ravel_query::SealMargin {
    ravel_query::SealMargin::from_catalog_config(&server_catalog_config_base())
}

/// The one place a `ServerConfig` becomes a `Catalog`: every field
/// [`build_catalog`] reads is taken from `config` here, and [`crate::start`]
/// calls only this.
///
/// It exists so that the field mapping is reachable from a test. `start`
/// builds a whole process and cannot be called from one, so a mapping
/// assembled inside it (`config.catalog_resolve_concurrency` silently
/// becoming `None`, or the per-prefix constant) would type-check and leave
/// every test green. `catalog_window_ns` is the one input that is not a
/// `ServerConfig` field: `start` resolves it through `resolve_ingest_lag`
/// first.
///
/// It logs the record caches' sizing once at startup: the per-tenant byte
/// budget, the entry bound derived under it, and the worst case for one
/// actively-queried tenant across both caches.
pub fn build_catalog_for_server(
    store: Arc<dyn ObjectStoreBackend>,
    config: &ServerConfig,
    catalog_window_ns: i64,
) -> anyhow::Result<Arc<Catalog>> {
    let catalog = build_catalog_from(
        CatalogConfig {
            build_key_index_leaves: config.fold.build_key_index_leaves,
            key_index_build_max_pairs: config.fold.key_index_build_max_pairs,
            metrics_label_index: config.fold.metrics_label_index,
            ..server_catalog_config_base()
        },
        store,
        config.shard_count,
        config.disable_cache,
        config.catalog_cache_max_bytes,
        config.query_budgets.column_stats_cache_max_bytes,
        config.cache_dir.clone(),
        config.catalog_resolve_concurrency,
        Some(catalog_window_ns),
        config.max_flush_delay,
        config.query_budgets.record_cache_max_bytes_per_tenant,
    )?;
    let sizing = catalog.config();
    tracing::info!(
        record_cache_budget_bytes_per_tenant =
            config.query_budgets.record_cache_max_bytes_per_tenant,
        cache_capacity_per_tenant = sizing.cache_capacity_per_tenant,
        record_cache_worst_case_bytes_per_tenant = sizing
            .commit_cache_max_bytes_per_tenant()
            .saturating_add(sizing.compaction_cache_max_bytes_per_tenant()),
        disable_cache = config.disable_cache,
        "catalog record caches sized: worst case per actively-queried tenant, outside the \
         process memory budget"
    );
    Ok(catalog)
}

/// Why `start` refused `query_budgets.fold_lag_interval`. Nothing is spawned.
#[derive(Debug, thiserror::Error)]
pub enum FoldLagIntervalError {
    /// `Some(Duration::ZERO)` names a fold that runs back to back, which no
    /// maintain tier runs, so the fold-lag threshold it would set is
    /// meaningless.
    #[error(
        "--fold-lag-interval-secs must be non-zero: a zero interval names a fold that runs back to back, which --fold-interval-secs refuses"
    )]
    ZeroFoldLagInterval,
}

/// Refuses a zero `query_budgets.fold_lag_interval`, the library-side match of
/// the `Cli::validate` refusal of `--fold-lag-interval-secs 0`. `start` runs it
/// before spawning anything; an unset interval passes.
pub fn check_fold_lag_interval(
    budgets: &crate::config::QueryBudgets,
) -> Result<(), FoldLagIntervalError> {
    if budgets.fold_lag_interval.is_some_and(|i| i.is_zero()) {
        return Err(FoldLagIntervalError::ZeroFoldLagInterval);
    }
    Ok(())
}

/// The one place a `ServerConfig` and the catalog it built become the
/// `EngineConfig` both query surfaces enforce (ADR-1306 follow-up task 5, and
/// the `fold_interval` half the 2026-09-27 refusal-threshold amendment left
/// open).
///
/// [`crate::start`] calls only this. The base it builds carries the deadline
/// `main` validated against `sys/gc` (ADR-0050 section 4, EC4), the
/// bytes-scanned budget resolved from `--limits-file`'s `[defaults]` table
/// (ADR-0061 decision 1) and the derived S3 request budget (ADR-0075);
/// [`crate::config::QueryBudgets::apply_to_engine`] then folds the ADR-0088
/// budgets and the RESOLVED ADR-0996 logs fetch quantities onto it. Without
/// that fold the engine would keep `EngineConfig::default()`'s compiled-in
/// 8 / 1024 and `--logs-fetch-policy` would be inert.
///
/// The three ADR-1306 inputs of the fold-lag refusal threshold are read off
/// their running sources rather than restated:
///
/// - `seal_margin` from `catalog_config`, the config of the `Catalog`
///   [`build_catalog_for_server`] returned and `start` hands to both resolve
///   and `fold::spawn`;
/// - `fold_interval` from `config.fold`, the same [`crate::FoldTaskConfig`]
///   value `start` passes to `fold::spawn`, so it is the interval the fold
///   loop really sleeps; or, when `query_budgets.fold_lag_interval` is set,
///   from that, the interval of the maintain tier's fold (ADR-1306 decision
///   6, amendment of 2026-10-01). This function applies it in any mode; it is
///   `Cli::parse_validated_from` that admits `--fold-lag-interval-secs` only
///   in `--mode query`, which spawns no scheduled fold;
/// - `head_cache_ttl` from the same `catalog_config`, the TTL the HEAD cache
///   a resolve reads its watermark through really runs on.
///
/// None of the three is `ravel_query`'s reference constant here. Those stay
/// what an `EngineConfig` built with no deployment context falls back to
/// ([`ravel_query::SealMargin::REFERENCE`],
/// [`ravel_query::REFERENCE_FOLD_INTERVAL`],
/// [`ravel_query::REFERENCE_HEAD_CACHE_TTL`]); a flag that later moves any of
/// them on the fold or the catalog carries the threshold with it, with no
/// second copy to update.
///
/// It exists as a function for the same reason
/// [`build_catalog_for_server`] does: `start` builds a whole process and
/// cannot be called from a test, so a mapping assembled inline there (a
/// `head_cache_ttl` silently left at the reference constant) would type-check
/// and leave every test green.
/// `derived_engine_config_uses_the_running_fold_and_catalog_values` drives
/// this function.
///
/// Fallible for the same reason [`crate::config::QueryBudgets::apply_to_engine`]
/// is: a zero `--logs-max-fetch-run-bytes` is refused at startup rather than
/// at a division inside the fetch layer.
pub fn build_engine_config(
    config: &ServerConfig,
    catalog_config: &CatalogConfig,
) -> Result<EngineConfig, ravel_query::EngineConfigError> {
    config.query_budgets.apply_to_engine(EngineConfig {
        deadline: config.query_deadline,
        max_bytes_scanned: config.limits.query_defaults.max_bytes_scanned,
        // The shard-aware S3 request budget (ADR-0075, ADR-1306), resolved in
        // `main` from `--max-s3-requests` (verbatim) or derived from
        // `--shards`, the flush cadence and [`server_seal_margin`].
        max_s3_requests: config.max_s3_requests,
        seal_margin: ravel_query::SealMargin::from_catalog_config(catalog_config),
        fold_interval: config
            .query_budgets
            .fold_lag_interval
            .unwrap_or(config.fold.fold_interval),
        head_cache_ttl: Duration::from_nanos(catalog_config.head_cache_ttl_ns.unsigned_abs()),
        ..EngineConfig::default()
    })
}

/// Build the query `AppState`. `engine_config` carries the resolved query
/// deadline (ADR-0050 section 4, EC4): the caller passes the SAME
/// `EngineConfig` whose `deadline` was validated against `sys/gc` in `main`, so
/// the engine that actually enforces the deadline uses the validated value
/// rather than an independent `EngineConfig::default()`.
///
/// `process_memory_budget` is the SAME `Arc<ravel_memory::MemoryBudget>`
/// instance [`build_sql_state`] wires into its `SqlExecutor` (ADR-1170
/// decisions 1/3/4): one process-wide accountant shared by the PromQL and
/// SQL/Flight SQL paths, so a fetch this engine issues and a reservation
/// `SqlExecutor` makes both draw down the same limit rather than each
/// enforcing its own independent ceiling.
///
/// `read_gate` is the server's ADR-1702 read CPU gate: the engine runs its
/// RSEG and RLOG catalog decodes and each PromQL evaluation at or above the
/// gate's evaluation floor on it (decisions 1 and 4).
#[allow(clippy::too_many_arguments)]
pub fn build_app_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    get_limiter: Arc<GetLimiter>,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    distributed: Option<Arc<ravel_query::distrib::Distributed>>,
    federation: Option<Arc<ravel_query::distrib::Federation>>,
    metadata_cache: Option<Arc<ravel_query::http::MetadataCache>>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    read_gate: Arc<ravel_cpu_gate::ReadGate>,
) -> AppState {
    let mut engine = QueryEngine::new(catalog, store, engine_config)
        .with_get_limiter(get_limiter)
        .with_memory_budget(process_memory_budget)
        .with_read_gate(read_gate);
    if let Some(cache) = cache {
        engine = engine.with_cache(cache);
    }
    // ADR-0071 distributed read fan-out: attach a coordinator
    // context only under `--distributed-query`. Absent it, the engine keeps the
    // byte-identical local path (`with_distributed` is the sole opt-in seam).
    if let Some(distributed) = distributed {
        engine = engine.with_distributed(distributed);
    }
    // ADR-0071 cross-cluster federation: attach the remote-cluster
    // fan-out only when `--remote-cluster` was configured. Absent it, the engine
    // resolves only local data (`with_federation` is the sole opt-in seam).
    if let Some(federation) = federation {
        engine = engine.with_federation(federation);
    }
    // Fold every completed Prometheus-shaped query into the same process
    // aggregator the SQL and analytics paths use (ADR-0044 section 4), so
    // `/metrics` covers PromQL read traffic too. The shared query
    // concurrency controller (ADR-0061 decision 2) gates every handler before
    // it resolves or fetches.
    // ADR-0085 decision 1 read path: attach the per-process metric metadata
    // cache that backs `/api/v1/metadata`. `None` in a mode that serves no
    // Prometheus-shaped query routes; when absent the endpoint keeps its
    // pre-ADR behavior byte-for-byte (a `200` with an empty `data` object).
    // The same aggregator is the usage sink as well as the cost recorder: the
    // cost recorder only ever sees a query that produced an answer, so without
    // this the drop guard's cancelled, timed-out, and failed records from every
    // Prometheus-shaped route would be folded into a sink that discards them.
    let state = AppState::new(Arc::new(engine), tenant_resolver)
        .with_cost_recorder(query_accounting.clone())
        .with_usage_sink(query_accounting)
        .with_query_admission(query_admission);
    match metadata_cache {
        Some(cache) => state.with_metadata_cache(cache),
        None => state,
    }
}

/// Default per-tenant SQL memory ceiling: 1 GiB across a tenant's concurrent
/// queries, four times the per-query default in `ravel_sql::SqlConfig`. This is
/// the shipped default an operator overrides per process with
/// `--sql-tenant-max-bytes` (ADR-0088), not a guess awaiting a number; changing
/// the compiled-in value itself is a separate measurement-backed follow-up.
/// Defined as [`crate::config::DEFAULT_SQL_TENANT_MAX_BYTES`] so the flag's
/// default and this ceiling are one constant.
#[cfg(feature = "sql")]
pub const DEFAULT_MAX_TENANT_BYTES: usize = crate::config::DEFAULT_SQL_TENANT_MAX_BYTES;

/// Re-export of the per-query SQL memory-pool default so callers of
/// [`build_sql_state`] (including external test crates that do not depend on
/// `ravel-sql` directly) can name the compiled-in default without threading the
/// dependency. Equals `ravel_sql::DEFAULT_MAX_QUERY_BYTES` and
/// [`crate::config::DEFAULT_SQL_MAX_QUERY_BYTES`].
#[cfg(feature = "sql")]
pub const DEFAULT_MAX_QUERY_BYTES: usize = ravel_sql::DEFAULT_MAX_QUERY_BYTES;

/// Build the state for `POST /api/v1/sql`.
///
/// Takes the same `Catalog` instance the PromQL engine and `/metrics` use
/// (ADR-0050 section 2): a second, independent `Catalog` here would carry
/// its own `isolation_breaches` counter, so a tenant_hash or LIST-prefix
/// breach hit only through the SQL path would never reach
/// `ravel_catalog_isolation_breach_total` and the alert rule built on it.
///
/// `engine_config` carries the resolved query deadline (ADR-0050 section 4,
/// EC4), the SAME value passed to [`build_app_state`]: SQL and Flight SQL
/// must enforce the deadline `main` validated against `sys/gc`, not an
/// independent `EngineConfig::default()` (without this, PromQL is wired to
/// the validated deadline but SQL/Flight SQL would not be).
///
/// `max_query_bytes` (the per-query DataFusion memory-pool ceiling) and
/// `max_tenant_bytes` (the per-tenant ceiling across a tenant's concurrent
/// queries) are the ADR-0088 operator-configurable SQL budgets, resolved from
/// `--sql-max-query-bytes` / `--sql-tenant-max-bytes` (defaulting to
/// [`ravel_sql::DEFAULT_MAX_QUERY_BYTES`] and [`DEFAULT_MAX_TENANT_BYTES`]).
/// Threading them here rather than taking `SqlConfig::default()` is what makes an
/// operator's override the value the executor's pool and per-tenant accountant
/// actually enforce.
///
/// `parallel_final_aggregation` (ADR-0094 decision 4, amended by issue #741) is
/// the process-wide switch, from `--sql-parallel-final-aggregation`, that lets
/// an exact-typed query repartition its final aggregation. Default `true`;
/// threaded here so an operator's `=false` opt-out is the value the executor's
/// classification gate actually reads rather than `SqlConfig::default()`'s on.
///
/// `declared_columns` is the source of each tenant's declared typed attribute
/// columns (ADR-0090 decision 2), installed on the executor with
/// `SqlExecutor::with_declared_column_source`. `None` leaves the executor's
/// built-in empty `StaticDeclaredColumns`, i.e. the `logs` table's
/// zero-declaration base schema for every tenant regardless of what any durable
/// `TenantConfig` says; [`crate::start`] always passes
/// `Some(TenantConfigDeclaredColumns)`. It is a parameter rather than something
/// built here so the caller owns the concrete overlay: `start` registers it with
/// the idle-tenant sweep, and a test can build one with a short staleness
/// horizon and still exercise this exact wiring.
///
/// `process_memory_budget` is the ADR-1170 decisions 1/3 process-wide
/// accountant: the shared remainder left after the three hard cache carves
/// (`ResolvedPerformanceDefaults::memory_remainder_bytes`). It is installed
/// on the executor via `SqlExecutor::with_process_memory_budget` so every
/// tenant's SQL memory reservation counts against the SAME instance
/// [`crate::lib`]'s `/metrics` gauges read, AND on the metrics, logs, and
/// spans fetchers via their own `with_memory_budget` so a SQL fetch
/// reservation counts against, and can be refused by, that same instance
/// (issue #2086) rather than each fetcher's private unlimited default.
///
/// The executor this builds reads no Parquet table: a name that is not a
/// signal table resolves as it did before Parquet tables existed.
/// [`build_sql_state_with_parquet`] is the same state with Parquet sources
/// installed, which is what [`crate::start`] builds.
#[cfg(feature = "sql")]
#[allow(clippy::too_many_arguments)]
pub fn build_sql_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    get_limiter: Arc<GetLimiter>,
    max_query_bytes: usize,
    max_tenant_bytes: usize,
    parallel_final_aggregation: bool,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    declared_columns: Option<Arc<dyn ravel_sql::DeclaredColumnSource>>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
) -> anyhow::Result<crate::sql::SqlState> {
    build_sql_state_inner(
        catalog,
        store,
        tenant_resolver,
        cache,
        engine_config,
        get_limiter,
        max_query_bytes,
        max_tenant_bytes,
        parallel_final_aggregation,
        false,
        query_accounting,
        query_admission,
        declared_columns,
        process_memory_budget,
        None,
        None,
        false,
        ravel_sql::DEFAULT_MIN_GRACE_MS,
        &SqlSpillInputs::default(),
        ravel_sql::IpcryptDeployment::default(),
        None,
    )
}

/// The minimum DDL sweep grace, in milliseconds, for a deployment whose
/// bootstrapped `sys/gc` records `max_query_duration_ns`. This is the floor
/// `ravel_pqtable::sweep::plan` enforces for `ravel-cli parquet sweep`
/// (ADR-2040), not protection for a running query: DDL itself never deletes a
/// manifest version, only a sweep does, run out-of-band by an operator. The
/// floor instead bounds a writer's own resolve-to-put window --
/// `ravel_pqtable::writer::apply` finishes its put within half of it. A
/// negative record (a corrupt or hand-edited `sys/gc`), and a value that
/// converts to less than [`WRITER_MIN_USABLE_GRACE_MS`] -- 0 included --
/// refuse startup naming `sys/gc`, rather than letting every future
/// `CREATE`/`CREATE OR REPLACE` fail with `NoPutBudget` instead.
#[cfg(feature = "sql")]
pub(crate) const WRITER_MIN_USABLE_GRACE_MS: u64 = 2;

#[cfg(feature = "sql")]
pub(crate) fn ddl_min_grace_ms(max_query_duration_ns: i64) -> anyhow::Result<u64> {
    if max_query_duration_ns < 0 {
        anyhow::bail!("sys/gc records a negative max_query_duration_ns ({max_query_duration_ns})");
    }
    let grace_ms = u64::try_from(max_query_duration_ns / 1_000_000)?;
    if grace_ms < WRITER_MIN_USABLE_GRACE_MS {
        anyhow::bail!(
            "sys/gc's max_query_duration_ns ({max_query_duration_ns}) converts to a DDL sweep \
             grace of {grace_ms} ms, below the {WRITER_MIN_USABLE_GRACE_MS} ms \
             ravel_pqtable::writer::apply needs to leave any resolve-to-put budget"
        );
    }
    Ok(grace_ms)
}

/// `source` of a `cache_bulk_scan_min_bytes` line whose threshold is
/// `--cache-bulk-scan-min-bytes` as given: no read cache, or a flag at or
/// above the read cache's `--cache-max-bytes`.
pub const BULK_SCAN_SOURCE_FLAG: &str = "flag";
/// `source` of a `cache_bulk_scan_min_bytes` line whose threshold is the read
/// cache's `--cache-max-bytes`, because the flag was below it.
pub const BULK_SCAN_SOURCE_CACHE_FLOOR: &str = "flag-floored-to-cache-max-bytes";
/// `source` of a `cache_bulk_scan_min_bytes` line when the flag is unset and
/// the size trigger is off.
pub const BULK_SCAN_SOURCE_UNSET: &str = "unset";

/// The bulk-scan size trigger threshold startup resolved, and where it came
/// from.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBulkScanMinBytes {
    /// What the executor applies; `None` is the size trigger off.
    pub value: Option<u64>,
    /// `--cache-bulk-scan-min-bytes` as the operator set it.
    pub flag: Option<u64>,
    /// One of the `BULK_SCAN_SOURCE_*` constants.
    pub source: &'static str,
}

/// Resolve the size trigger's threshold from `--cache-bulk-scan-min-bytes`
/// and the in-memory read cache's `max_bytes` (`None` when the process runs
/// without a read cache), through [`ravel_sql::effective_bulk_scan_min_bytes`].
#[cfg(feature = "sql")]
pub fn resolve_cache_bulk_scan_min_bytes(
    flag: Option<u64>,
    cache_max_bytes: Option<u64>,
) -> ResolvedBulkScanMinBytes {
    let value = ravel_sql::effective_bulk_scan_min_bytes(flag, cache_max_bytes);
    let source = match flag {
        None => BULK_SCAN_SOURCE_UNSET,
        Some(_) if value != flag => BULK_SCAN_SOURCE_CACHE_FLOOR,
        Some(_) => BULK_SCAN_SOURCE_FLAG,
    };
    ResolvedBulkScanMinBytes {
        value,
        flag,
        source,
    }
}

#[cfg(feature = "sql")]
impl ResolvedBulkScanMinBytes {
    /// The `cache_bulk_scan_min_bytes` startup line, in the
    /// `performance default resolved` layout
    /// [`crate::config::ResolvedPerformanceDefaults::emit`] uses, carrying the
    /// flag as given beside the value in force.
    pub fn emit(&self) {
        match self.value {
            Some(value) => tracing::info!(
                setting = "cache_bulk_scan_min_bytes",
                value,
                source = self.source,
                flag = self.flag,
                "performance default resolved"
            ),
            None => tracing::info!(
                setting = "cache_bulk_scan_min_bytes",
                value = "none",
                source = self.source,
                "performance default resolved"
            ),
        }
    }
}

/// `source` of a `sql_spill_dir`/`sql_spill_max_bytes` line whose value came
/// from the `RAVEL_SQL_SPILL_DIR`/`RAVEL_SQL_SPILL_MAX_BYTES` pair.
pub const SQL_SPILL_SOURCE_ENV: &str = "env";
/// `source` of a `sql_spill_dir` line whose directory derives from
/// `--cache-dir`.
pub const SQL_SPILL_SOURCE_CACHE_DIR: &str = "cache-dir";
/// `source` of a `sql_spill_max_bytes` line whose ceiling is
/// `RAVEL_SQL_SPILL_MAX_BYTES` set alone, replacing the `--cache-dir`-derived
/// ceiling.
pub const SQL_SPILL_SOURCE_ENV_OVERRIDE: &str = "env-override";
/// `source` of a `sql_spill_max_bytes` line whose ceiling is
/// [`ravel_sql::derive_spill_max_bytes`]'s.
pub const SQL_SPILL_SOURCE_DERIVED: &str = "derived";
/// `source` of both spill lines when spill would resolve under `--cache-dir`
/// with a derived ceiling, but half the volume's free space is below the
/// 1 GiB floor ([`ravel_sql::derive_spill_max_bytes`] returns `None`).
pub const SQL_SPILL_SOURCE_INSUFFICIENT_SPACE: &str = "cache-dir-insufficient-space";
/// `source` of both spill lines under `--sql-spill off`.
pub const SQL_SPILL_SOURCE_FLAG_OFF: &str = "flag-off";
/// `source` of both spill lines when no source configures spill. Startup no
/// longer reports it: with no other source, the working-directory default
/// applies or names why it is off.
pub const SQL_SPILL_SOURCE_UNSET: &str = "unset";
/// `source` of both spill lines when spill resolved under the process working
/// directory, `<cwd>/sql-spill/<instance-id>`, because no other source
/// configures it (issue #3899).
pub const SQL_SPILL_SOURCE_CWD_DEFAULT: &str = "cwd-default";
/// `source` of both spill lines when the working-directory default is off
/// because `<cwd>/sql-spill` when it exists, or else the working directory,
/// is on tmpfs or ramfs, where spill would write to memory.
pub const SQL_SPILL_SOURCE_CWD_TMPFS: &str = "cwd-tmpfs";
/// `source` of both spill lines when the working-directory default is off
/// because the working directory, `<cwd>/sql-spill`, its free space or its
/// owner lock could not be read, created or taken.
pub const SQL_SPILL_SOURCE_CWD_UNAVAILABLE: &str = "cwd-unavailable";
/// `source` of both spill lines when the working-directory default is off
/// because half the volume's free space is below the 1 GiB floor
/// ([`ravel_sql::derive_spill_max_bytes`] returns `None`).
pub const SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE: &str = "cwd-insufficient-space";
/// `source` of the `sql_spill_compression` line when spill is on: the codec
/// is [`ravel_sql::SpillDecision::SPILL_COMPRESSION`], which no flag sets.
pub const SQL_SPILL_SOURCE_BUILT_IN: &str = "built-in";
/// `source` of a `sql_spill_merge_fan_in` line whose value equals
/// [`crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN`]; any other value reads
/// [`crate::config::PERF_SOURCE_FLAG`]. With spill off, that line reads
/// `value="none"` under the `sql_spill_dir` line's source instead.
pub const SQL_SPILL_SOURCE_DEFAULT: &str = "default";

/// The owned form of [`ravel_sql::CacheDirSpill`]: what a `--cache-dir`
/// deployment resolves its SQL spill root and ceiling from (ADR-0954, amended
/// by issue #2416).
#[cfg(feature = "sql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDirSpillInputs {
    /// The configured `--cache-dir`.
    pub cache_dir: PathBuf,
    /// This process's instance identity: the `process_id` of its
    /// `ravel_maintain::WorkerSet`, the identity its
    /// `sys/maintain/workers/<process_id>` heartbeat and live-set entry carry.
    /// `start` builds that `WorkerSet` in every mode, so this is never a
    /// second identity minted for spill.
    pub instance_id: String,
    /// Free bytes on the volume backing `<cache_dir>/sql-spill`, measured once
    /// at startup with [`ravel_sql::measure_free_bytes`].
    pub free_bytes: u64,
    /// [`crate::config::SqlSpillSettings::read_cache_bytes`].
    pub read_cache_bytes: u64,
    /// [`crate::config::SqlSpillSettings::memory_budget_bytes`].
    pub memory_budget_bytes: u64,
}

#[cfg(feature = "sql")]
impl CacheDirSpillInputs {
    fn as_cache_dir_spill(&self) -> ravel_sql::CacheDirSpill<'_> {
        ravel_sql::CacheDirSpill {
            cache_dir: &self.cache_dir,
            instance_id: &self.instance_id,
            free_bytes: self.free_bytes,
            read_cache_bytes: self.read_cache_bytes,
            memory_budget_bytes: self.memory_budget_bytes,
        }
    }
}

/// The space figures a derived spill ceiling, or its refusal, was computed
/// from: the measured free bytes and the read cache's disk-tier bound
/// subtracted from them.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillSpace {
    /// [`CacheDirSpillInputs::free_bytes`].
    pub free_bytes: u64,
    /// [`CacheDirSpillInputs::read_cache_bytes`].
    pub read_cache_bytes: u64,
}

/// The two arguments [`build_sql_state_with_parquet`] hands
/// `SqlConfig::with_spill_resolved`, and the merge fan-in it sets on the
/// result.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSpillInputs {
    /// `--sql-spill off`.
    pub off: bool,
    /// `Some` when `--cache-dir` is set, `--sql-spill` is `auto`, and
    /// `RAVEL_SQL_SPILL_DIR` is unset: the only case in which
    /// [`ravel_sql::SpillConfig::resolve`] reads a cache directory at all.
    /// Also `Some` under the working-directory default, with the working
    /// directory in `cache_dir` and `read_cache_bytes` 0.
    pub cache_dir: Option<CacheDirSpillInputs>,
    /// `cache_dir` holds the working-directory default, not `--cache-dir`.
    pub cwd_default: bool,
    /// `--sql-spill-merge-fan-in`, set on the resolved
    /// [`ravel_sql::SpillConfig::merge_fan_in`] whatever source the spill
    /// directory came from.
    pub merge_fan_in: usize,
}

#[cfg(feature = "sql")]
impl Default for SqlSpillInputs {
    fn default() -> Self {
        SqlSpillInputs {
            off: false,
            cache_dir: None,
            cwd_default: false,
            merge_fan_in: crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN,
        }
    }
}

/// The spill configuration startup resolved, and where each half came from.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSqlSpill {
    /// `None` is spill disabled.
    pub config: Option<ravel_sql::SpillConfig>,
    /// One of the `SQL_SPILL_SOURCE_*` constants.
    pub dir_source: &'static str,
    /// One of the `SQL_SPILL_SOURCE_*` constants.
    pub max_bytes_source: &'static str,
    /// `Some` when the ceiling was derived from `--cache-dir`'s free space,
    /// or spill is off because that space was too little.
    pub space: Option<SpillSpace>,
}

#[cfg(feature = "sql")]
impl ResolvedSqlSpill {
    /// The `sql_spill_dir`, `sql_spill_max_bytes`, `sql_spill_compression`
    /// and `sql_spill_merge_fan_in` startup lines, in the `performance default
    /// resolved` layout [`crate::config::ResolvedPerformanceDefaults::emit`]
    /// uses. All four lines are written whether or not spill is enabled; a
    /// disabled spill reads `value="none"` with the reason as its source. The
    /// fan-in line's source is `default` when the value equals
    /// [`crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN`] and `flag` otherwise.
    /// When the ceiling was
    /// derived from `--cache-dir`, or refused for lack of space there, the
    /// `sql_spill_max_bytes` line also carries `free_bytes` and
    /// `read_cache_bytes`, the two figures it was derived from.
    pub fn emit(&self) {
        let free_bytes = self.space.map(|space| space.free_bytes);
        let read_cache_bytes = self.space.map(|space| space.read_cache_bytes);
        match &self.config {
            Some(config) => {
                let dir = config.dir.display().to_string();
                tracing::info!(
                    setting = "sql_spill_dir",
                    value = dir.as_str(),
                    source = self.dir_source,
                    "performance default resolved"
                );
                tracing::info!(
                    setting = "sql_spill_max_bytes",
                    value = config.max_bytes,
                    source = self.max_bytes_source,
                    free_bytes,
                    read_cache_bytes,
                    "performance default resolved"
                );
                let compression = ravel_sql::SpillDecision::SPILL_COMPRESSION.to_string();
                tracing::info!(
                    setting = "sql_spill_compression",
                    value = compression.as_str(),
                    source = SQL_SPILL_SOURCE_BUILT_IN,
                    "performance default resolved"
                );
                tracing::info!(
                    setting = "sql_spill_merge_fan_in",
                    value = config.merge_fan_in,
                    source = if config.merge_fan_in == crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN
                    {
                        SQL_SPILL_SOURCE_DEFAULT
                    } else {
                        crate::config::PERF_SOURCE_FLAG
                    },
                    "performance default resolved"
                );
            }
            None => {
                tracing::info!(
                    setting = "sql_spill_dir",
                    value = "none",
                    source = self.dir_source,
                    "performance default resolved"
                );
                tracing::info!(
                    setting = "sql_spill_max_bytes",
                    value = "none",
                    source = self.max_bytes_source,
                    free_bytes,
                    read_cache_bytes,
                    "performance default resolved"
                );
                tracing::info!(
                    setting = "sql_spill_compression",
                    value = "none",
                    source = self.dir_source,
                    "performance default resolved"
                );
                tracing::info!(
                    setting = "sql_spill_merge_fan_in",
                    value = "none",
                    source = self.dir_source,
                    "performance default resolved"
                );
                if self.max_bytes_source == SQL_SPILL_SOURCE_INSUFFICIENT_SPACE {
                    tracing::warn!(
                        free_bytes,
                        read_cache_bytes,
                        "SQL spill is off: the free space under --cache-dir minus the read \
                         cache's disk-tier bound, halved, is below the 1 GiB floor of a derived \
                         spill ceiling; free space on that volume, or set \
                         RAVEL_SQL_SPILL_MAX_BYTES to choose the ceiling"
                    );
                }
                if self.max_bytes_source == SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE {
                    tracing::warn!(
                        source = self.max_bytes_source,
                        free_bytes,
                        "SQL spill is off: half the free space under the working directory is \
                         below the 1 GiB floor of a derived spill ceiling; free space on that \
                         volume, or set --cache-dir or the \
                         RAVEL_SQL_SPILL_DIR/RAVEL_SQL_SPILL_MAX_BYTES pair"
                    );
                }
            }
        }
    }
}

/// Resolve the SQL spill configuration from `--sql-spill`, the
/// `RAVEL_SQL_SPILL_DIR`/`RAVEL_SQL_SPILL_MAX_BYTES` values passed in, and
/// `--cache-dir` (ADR-0954, amended by issue #2416), with the same
/// [`ravel_sql::SpillConfig::resolve`] precedence
/// `SqlConfig::with_spill_resolved` applies: `--sql-spill off` first, then the
/// full env pair, then the cache directory (with `RAVEL_SQL_SPILL_MAX_BYTES`
/// alone replacing the derived ceiling, and disabled when the derivation finds
/// too little free space), else disabled. The working-directory default
/// arrives as `inputs.cache_dir` with `inputs.cwd_default` set and resolves
/// like `--cache-dir`, under its own source names.
///
/// A half-set env pair that no cache directory completes is an error naming
/// the variable that is missing.
#[cfg(feature = "sql")]
pub fn resolve_sql_spill(
    inputs: &SqlSpillInputs,
    env_dir: Option<&std::ffi::OsStr>,
    env_quota: Option<&std::ffi::OsStr>,
) -> anyhow::Result<ResolvedSqlSpill> {
    use ravel_sql::{ENV_SPILL_DIR, ENV_SPILL_MAX_BYTES, SpillConfig, SpillConfigError};

    if inputs.off {
        return Ok(ResolvedSqlSpill {
            config: None,
            dir_source: SQL_SPILL_SOURCE_FLAG_OFF,
            max_bytes_source: SQL_SPILL_SOURCE_FLAG_OFF,
            space: None,
        });
    }
    let cache_dir = inputs
        .cache_dir
        .as_ref()
        .map(CacheDirSpillInputs::as_cache_dir_spill);
    let mut config =
        SpillConfig::resolve(env_dir, env_quota, cache_dir).map_err(|err| match err {
            SpillConfigError::Incomplete if env_dir.is_some() => anyhow::anyhow!(
                "{ENV_SPILL_MAX_BYTES} is not set but {ENV_SPILL_DIR} is: set both to spill to \
             that directory, or unset {ENV_SPILL_DIR} (with --cache-dir set, spill then goes \
             under it)"
            ),
            SpillConfigError::Incomplete => anyhow::anyhow!(
                "{ENV_SPILL_DIR} is not set but {ENV_SPILL_MAX_BYTES} is, and there is no \
             --cache-dir to place the spill directory under: set {ENV_SPILL_DIR}, set \
             --cache-dir, or unset {ENV_SPILL_MAX_BYTES}"
            ),
            other => anyhow::Error::new(other),
        })?;
    if let Some(config) = config.as_mut() {
        config.merge_fan_in = inputs.merge_fan_in;
    }
    // With a cache directory and no env directory, `resolve` returns `None`
    // only when the derived ceiling finds too little free space: a bare env
    // quota there always enables spill.
    let (dir_source, max_bytes_source) = match (&config, env_dir.is_some(), env_quota.is_some()) {
        (None, false, _) if inputs.cache_dir.is_some() && inputs.cwd_default => (
            SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE,
            SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE,
        ),
        (None, false, _) if inputs.cache_dir.is_some() => (
            SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
            SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
        ),
        (None, _, _) => (SQL_SPILL_SOURCE_UNSET, SQL_SPILL_SOURCE_UNSET),
        (Some(_), true, _) => (SQL_SPILL_SOURCE_ENV, SQL_SPILL_SOURCE_ENV),
        (Some(_), false, true) => (SQL_SPILL_SOURCE_CACHE_DIR, SQL_SPILL_SOURCE_ENV_OVERRIDE),
        (Some(_), false, false) if inputs.cwd_default => {
            (SQL_SPILL_SOURCE_CWD_DEFAULT, SQL_SPILL_SOURCE_CWD_DEFAULT)
        }
        (Some(_), false, false) => (SQL_SPILL_SOURCE_CACHE_DIR, SQL_SPILL_SOURCE_DERIVED),
    };
    let space = match max_bytes_source {
        SQL_SPILL_SOURCE_DERIVED
        | SQL_SPILL_SOURCE_INSUFFICIENT_SPACE
        | SQL_SPILL_SOURCE_CWD_DEFAULT
        | SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE => {
            inputs.cache_dir.as_ref().map(|cache| SpillSpace {
                free_bytes: cache.free_bytes,
                read_cache_bytes: cache.read_cache_bytes,
            })
        }
        _ => None,
    };
    Ok(ResolvedSqlSpill {
        config,
        dir_source,
        max_bytes_source,
        space,
    })
}

/// What [`prepare_sql_spill`] settled before the SQL surface is built.
#[cfg(feature = "sql")]
pub struct SqlSpillStartup {
    /// For [`build_sql_state_with_parquet`].
    pub inputs: SqlSpillInputs,
    /// What the startup lines reported.
    pub resolved: ResolvedSqlSpill,
    /// This process's hold on `<cache-dir>/sql-spill/<instance-id>`, when
    /// spill resolved there. Its lock is the proof of liveness every other
    /// process's sweep tests, so the caller keeps it for the process lifetime.
    pub owner: Option<ravel_sql::spill::SpillRootOwner>,
    /// Roots under `<cache-dir>/sql-spill` the startup sweep left in place,
    /// each logged at INFO with its reason.
    pub left_in_place: Vec<LeftSpillRoot>,
}

/// A root under `<cache-dir>/sql-spill` the startup sweep left in place.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftSpillRoot {
    pub dir: PathBuf,
    /// [`SPILL_ROOT_LEFT_OWNED`] or [`SPILL_ROOT_LEFT_SWEPT_ASIDE`].
    pub reason: &'static str,
}

/// Why the sweep left a root that still carries its instance name.
#[cfg(feature = "sql")]
pub const SPILL_ROOT_LEFT_OWNED: &str =
    "a live process holds its owner lock, or its ownership could not be settled";
/// Why the sweep left a tree a sweep already moved aside for removal.
#[cfg(feature = "sql")]
pub const SPILL_ROOT_LEFT_SWEPT_ASIDE: &str = "a sweep moved it aside for removal and its removal \
     failed or is still in progress in another process";

/// Resolve SQL spill at startup from the process environment (ADR-0954,
/// amended by issue #2416), log the `sql_spill_dir` and `sql_spill_max_bytes`
/// lines, and, when spill resolved under `--cache-dir`, take ownership of
/// `<cache-dir>/sql-spill/<instance_id>` (requirement 7). Call before serving
/// any query.
///
/// Whenever `--cache-dir` is set, `--sql-spill` is not off, and
/// `<cache-dir>/sql-spill` already exists, roots there whose owner is gone are
/// swept first, before the free space is measured and whatever spill then
/// resolves to, so the measurement sees the bytes the sweep reclaimed. Only a
/// process that serves SQL calls this, so a `maintain` or `gateway` process,
/// or a build without the `sql` feature, sweeps nothing.
///
/// Fails, refusing startup, on a spill configuration error, when
/// `<cache-dir>/sql-spill` cannot be created or its free space cannot be
/// measured, or when this process cannot take its own root's lock; every
/// message names the path involved.
///
/// With no `--sql-spill off`, neither half of the env pair and no
/// `--cache-dir`, spill defaults to the process working directory (issue
/// #3899): see [`prepare_cwd_spill`]. That default never refuses startup.
#[cfg(feature = "sql")]
pub fn prepare_sql_spill(
    cache_dir: Option<&std::path::Path>,
    settings: crate::config::SqlSpillSettings,
    instance_id: &str,
) -> anyhow::Result<SqlSpillStartup> {
    let env_dir = std::env::var_os(ravel_sql::ENV_SPILL_DIR);
    let env_quota = std::env::var_os(ravel_sql::ENV_SPILL_MAX_BYTES);
    prepare_sql_spill_with(
        cache_dir,
        settings,
        instance_id,
        env_dir.as_deref(),
        env_quota.as_deref(),
        std::env::current_dir,
        spill_fs_kind,
        ravel_sql::measure_free_bytes,
    )
}

/// What the working-directory spill default needs to know about the
/// filesystem under the working directory.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillFsKind {
    /// tmpfs or ramfs: spill files there are held in memory.
    Memory,
    /// Any other type, or a type that could not be read.
    Other,
}

/// tmpfs's `statfs(2)` `f_type`, `TMPFS_MAGIC` in `linux/magic.h`.
#[cfg(all(feature = "sql", target_os = "linux"))]
const TMPFS_MAGIC: u32 = 0x0102_1994;
/// ramfs's `statfs(2)` `f_type`, `RAMFS_MAGIC` in `linux/magic.h`.
#[cfg(all(feature = "sql", target_os = "linux"))]
const RAMFS_MAGIC: u32 = 0x8584_58f6;

/// The filesystem type under `path`, from `statfs(2)`'s `f_type` through
/// rustix. A failed `statfs` reads as [`SpillFsKind::Other`]: creating and
/// measuring `<path>/sql-spill` then decides.
#[cfg(all(feature = "sql", target_os = "linux"))]
fn spill_fs_kind(path: &std::path::Path) -> SpillFsKind {
    let Ok(stat) = rustix::fs::statfs(path) else {
        return SpillFsKind::Other;
    };
    // `f_type` is a signed word of platform width; the magic numbers are its
    // low 32 bits.
    match u32::try_from(i128::from(stat.f_type) & 0xffff_ffff) {
        Ok(TMPFS_MAGIC | RAMFS_MAGIC) => SpillFsKind::Memory,
        _ => SpillFsKind::Other,
    }
}

/// Off Linux the filesystem type is not read: spill proceeds.
#[cfg(all(feature = "sql", not(target_os = "linux")))]
fn spill_fs_kind(_path: &std::path::Path) -> SpillFsKind {
    SpillFsKind::Other
}

#[cfg(feature = "sql")]
#[allow(clippy::too_many_arguments)]
fn prepare_sql_spill_with(
    cache_dir: Option<&std::path::Path>,
    settings: crate::config::SqlSpillSettings,
    instance_id: &str,
    env_dir: Option<&std::ffi::OsStr>,
    env_quota: Option<&std::ffi::OsStr>,
    working_dir: impl FnOnce() -> std::io::Result<PathBuf>,
    fs_kind: impl FnOnce(&std::path::Path) -> SpillFsKind,
    measure_free_bytes: impl FnOnce(&std::path::Path) -> std::io::Result<u64>,
) -> anyhow::Result<SqlSpillStartup> {
    use anyhow::Context as _;
    use ravel_sql::spill::SpillRootOwner;

    if !settings.off && env_dir.is_none() && env_quota.is_none() && cache_dir.is_none() {
        return Ok(prepare_cwd_spill(
            settings,
            instance_id,
            working_dir,
            fs_kind,
            measure_free_bytes,
            SpillRootOwner::acquire,
        ));
    }

    let left_in_place = match cache_dir {
        Some(cache_dir) if !settings.off => sweep_spill_roots(cache_dir),
        _ => Vec::new(),
    };
    let cache_dir_inputs = match cache_dir {
        Some(cache_dir) if !settings.off && env_dir.is_none() => {
            let spill_root = cache_dir.join(ravel_sql::SQL_SPILL_SUBDIR);
            std::fs::create_dir_all(&spill_root).with_context(|| {
                format!(
                    "could not create the SQL spill root {}",
                    spill_root.display()
                )
            })?;
            let free_bytes = measure_free_bytes(&spill_root).with_context(|| {
                format!(
                    "could not measure the free space under the SQL spill root {}",
                    spill_root.display()
                )
            })?;
            Some(CacheDirSpillInputs {
                cache_dir: cache_dir.to_path_buf(),
                instance_id: instance_id.to_string(),
                free_bytes,
                read_cache_bytes: settings.read_cache_bytes,
                memory_budget_bytes: settings.memory_budget_bytes,
            })
        }
        _ => None,
    };
    let inputs = SqlSpillInputs {
        off: settings.off,
        cache_dir: cache_dir_inputs,
        cwd_default: false,
        merge_fan_in: settings.merge_fan_in,
    };
    let resolved = resolve_sql_spill(&inputs, env_dir, env_quota)?;
    resolved.emit();

    let mut owner = None;
    if let (Some(cache), Some(config)) = (&inputs.cache_dir, &resolved.config)
        && config.dir == ravel_sql::cache_spill_dir(&cache.cache_dir, &cache.instance_id)
    {
        owner = Some(
            SpillRootOwner::acquire(&cache.cache_dir, &cache.instance_id).with_context(|| {
                format!(
                    "could not take ownership of the SQL spill root {} (another live process \
                     holds its owner lock, or the directory is not writable)",
                    config.dir.display()
                )
            })?,
        );
    }
    Ok(SqlSpillStartup {
        inputs,
        resolved,
        owner,
        left_in_place,
    })
}

/// The working-directory spill default (issue #3899): with no other source
/// configured, spill resolves to `<cwd>/sql-spill/<instance_id>` exactly as
/// `--cache-dir` would with the working directory in its place, with the
/// same orphan sweep, owner lock and derived ceiling, and `read_cache_bytes`
/// 0 because without `--cache-dir` no disk cache tier shares the volume.
///
/// Never refuses startup. Spill is off, with a WARN naming the source, when
/// `<cwd>/sql-spill` when it exists, or else the working directory, is on
/// tmpfs or ramfs (`cwd-tmpfs`), when the working directory cannot
/// be read, `<cwd>/sql-spill` cannot be created, its free space cannot be
/// measured or this process's root cannot be locked (`cwd-unavailable`), and
/// when half the free space is below the derived ceiling's 1 GiB floor
/// (`cwd-insufficient-space`).
///
/// `<cwd>/sql-spill` is created only once spill resolves on, and whatever
/// this call created there is removed again when `acquire` then fails, so a
/// default that resolves off leaves the working directory as it found it.
#[cfg(feature = "sql")]
fn prepare_cwd_spill(
    settings: crate::config::SqlSpillSettings,
    instance_id: &str,
    working_dir: impl FnOnce() -> std::io::Result<PathBuf>,
    fs_kind: impl FnOnce(&std::path::Path) -> SpillFsKind,
    measure_free_bytes: impl FnOnce(&std::path::Path) -> std::io::Result<u64>,
    acquire: impl FnOnce(&std::path::Path, &str) -> std::io::Result<ravel_sql::spill::SpillRootOwner>,
) -> SqlSpillStartup {
    let off = |source: &'static str,
               dir: &std::path::Path,
               reason: &str,
               left_in_place: Vec<LeftSpillRoot>| {
        tracing::warn!(
            source,
            dir = %dir.display(),
            reason,
            "SQL spill is off: the working-directory default cannot be used; to \
             configure spill explicitly, point --cache-dir or the \
             RAVEL_SQL_SPILL_DIR/RAVEL_SQL_SPILL_MAX_BYTES pair at a writable volume \
             that is not memory-backed, or pass --sql-spill off to run without spill \
             and silence this"
        );
        let resolved = ResolvedSqlSpill {
            config: None,
            dir_source: source,
            max_bytes_source: source,
            space: None,
        };
        resolved.emit();
        SqlSpillStartup {
            inputs: SqlSpillInputs::default(),
            resolved,
            owner: None,
            left_in_place,
        }
    };

    let cwd = match working_dir() {
        Ok(cwd) => cwd,
        Err(err) => {
            return off(
                SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
                std::path::Path::new("."),
                &format!("could not read the working directory: {err}"),
                Vec::new(),
            );
        }
    };
    let spill_root = cwd.join(ravel_sql::SQL_SPILL_SUBDIR);
    let root_existed = spill_root.is_dir();
    // An existing `<cwd>/sql-spill` may be a mount of its own, so probe and
    // measure it; otherwise the root would be created on the working
    // directory's volume.
    let measured = if root_existed { &spill_root } else { &cwd };
    if fs_kind(measured) == SpillFsKind::Memory {
        return off(
            SQL_SPILL_SOURCE_CWD_TMPFS,
            measured,
            "the SQL spill root's volume is tmpfs or ramfs, so spill would write to memory",
            Vec::new(),
        );
    }
    let left_in_place = sweep_spill_roots(&cwd);
    let free_bytes = match measure_free_bytes(measured) {
        Ok(free_bytes) => free_bytes,
        Err(err) => {
            return off(
                SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
                &spill_root,
                &format!("could not measure the free space under the SQL spill root: {err}"),
                left_in_place,
            );
        }
    };
    let inputs = SqlSpillInputs {
        off: false,
        cache_dir: Some(CacheDirSpillInputs {
            cache_dir: cwd.clone(),
            instance_id: instance_id.to_string(),
            free_bytes,
            read_cache_bytes: 0,
            memory_budget_bytes: settings.memory_budget_bytes,
        }),
        cwd_default: true,
        merge_fan_in: settings.merge_fan_in,
    };
    let resolved = match resolve_sql_spill(&inputs, None, None) {
        Ok(resolved) => resolved,
        Err(err) => {
            return off(
                SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
                &spill_root,
                &format!("{err:#}"),
                left_in_place,
            );
        }
    };
    let Some(config) = &resolved.config else {
        resolved.emit();
        return SqlSpillStartup {
            inputs,
            resolved,
            owner: None,
            left_in_place,
        };
    };
    let instance_existed = config.dir.exists();
    if let Err(err) = std::fs::create_dir_all(&spill_root) {
        return off(
            SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
            &spill_root,
            &format!("could not create the SQL spill root: {err}"),
            left_in_place,
        );
    }
    let owner = match acquire(&cwd, instance_id) {
        Ok(owner) => owner,
        Err(err) => {
            // Best effort: spill is off either way, and `remove_dir` leaves
            // a root another process has started using in the meantime.
            if !instance_existed {
                let _ = std::fs::remove_dir_all(&config.dir);
            }
            if !root_existed {
                let _ = std::fs::remove_dir(&spill_root);
            }
            return off(
                SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
                &config.dir,
                &format!(
                    "could not take ownership of the SQL spill root (another live process \
                     holds its owner lock, or the directory is not writable): {err}"
                ),
                left_in_place,
            );
        }
    };
    resolved.emit();
    SqlSpillStartup {
        inputs,
        resolved,
        owner: Some(owner),
        left_in_place,
    }
}

/// Sweep the roots under `<cache_dir>/sql-spill` whose owner is gone, then log
/// at INFO, and return, every root it left with its reason. A missing
/// `<cache_dir>/sql-spill` is neither swept nor created. Reads nothing outside
/// `<cache-dir>/sql-spill`.
#[cfg(feature = "sql")]
fn sweep_spill_roots(cache_dir: &std::path::Path) -> Vec<LeftSpillRoot> {
    let spill_root = cache_dir.join(ravel_sql::SQL_SPILL_SUBDIR);
    if !spill_root.is_dir() {
        return Vec::new();
    }
    ravel_sql::spill::sweep_orphaned_spill_roots_under(cache_dir);
    let entries = match std::fs::read_dir(&spill_root) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(
                dir = %spill_root.display(),
                error = %err,
                "could not list the SQL spill roots the startup sweep left"
            );
            return Vec::new();
        }
    };
    let mut left: Vec<LeftSpillRoot> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| {
            let swept_aside = entry
                .file_name()
                .to_string_lossy()
                .starts_with(ravel_sql::spill::SWEPT_NAME_PREFIX);
            LeftSpillRoot {
                dir: entry.path(),
                reason: if swept_aside {
                    SPILL_ROOT_LEFT_SWEPT_ASIDE
                } else {
                    SPILL_ROOT_LEFT_OWNED
                },
            }
        })
        .collect();
    left.sort_by(|a, b| a.dir.cmp(&b.dir));
    for root in &left {
        tracing::info!(
            dir = %root.dir.display(),
            reason = root.reason,
            "SQL spill root left in place by the startup sweep"
        );
    }
    left
}

/// [`build_sql_state`] with Parquet tables queryable (ADR-2040): the executor
/// resolves a tenant's Parquet manifests and grants from `store`, and reads
/// their files through one read-only external store per (credential profile,
/// bucket), opened from `parquet_profiles` the first time a query reads that
/// bucket and kept for the process. The reads share `get_limiter` and `cache`
/// with the signal-table fetchers. A (profile, bucket) that is Ravel's own
/// data bucket, by `parquet_profiles.ravel_bucket`, is refused before it is
/// opened (ADR-2040 D4).
///
/// `parquet_profiles` is `None` when no `--parquet-profiles` file is
/// configured: no Parquet table is then queryable, and a query naming one
/// fails with `ParquetQueryError::NotConfigured`.
///
/// `ddl_min_grace_ms` is installed on the executor via
/// `SqlExecutor::with_ddl_min_grace_ms` (ADR-2040): the deployment's
/// `sys/gc` `max_query_duration_ns`, in milliseconds, passed down to
/// `ravel_pqtable::writer::apply` for every `CREATE`/`CREATE OR REPLACE` this
/// executor runs. It bounds the writer's own resolve-to-put window, not a
/// running query: DDL never deletes a manifest version, only `ravel-cli
/// parquet sweep` does. The caller derives it from the already-bootstrapped
/// `GcConfigValues` rather than this function re-reading `sys/gc`.
///
/// `spill` is what [`prepare_sql_spill`] settled: the executor's
/// `SqlConfig::spill` comes from `SqlConfig::with_spill_resolved(spill.off,
/// spill.cache_dir)`, with `spill.merge_fan_in` set on it.
///
/// `read_gate` is the process's ADR-1702 read CPU gate, attached to the logs
/// and spans fetchers so the `logs` and `spans` scans decode their blocks on
/// it (decision 7). [`build_sql_state`] attaches none and decodes inline.
/// `scan_decode_inline` (`--sql-scan-decode-inline`, issue #2795) keeps the
/// gate off the logs and spans fetchers only, so those two scans decode
/// inline while the `samples` scan's catalog decodes stay on the gate.
///
/// `bucketed_final_aggregation` (`--sql-bucketed-final-aggregation`, ADR-3565
/// decision 1) sets `SqlConfig::bucketed_final_aggregation` to the default
/// `ravel_sql::BucketedFinalAggregation` when `true` and to `None` when
/// `false`. [`build_sql_state`] passes `false`.
///
/// `ipcrypt` is the deployment key and audit posture the `ipcrypt_encrypt`
/// and `ipcrypt_prefix_encrypt` SQL functions read (ADR-2993 decision 6),
/// installed via `SqlExecutor::with_ipcrypt_deployment`. [`build_sql_state`]
/// installs the default, which holds no key and so registers neither.
///
/// `cache_bulk_scan_min_bytes` is the read-bytes threshold of ADR-3072's size
/// trigger amendments, as [`resolve_cache_bulk_scan_min_bytes`] resolved it
/// from `--cache-bulk-scan-min-bytes` and the read cache's size, installed
/// via `SqlExecutor::with_cache_bulk_scan_min_bytes`. [`build_sql_state`]
/// installs `None`, the predicate-free bulk-scan rule alone.
#[cfg(feature = "sql")]
#[allow(clippy::too_many_arguments)]
pub fn build_sql_state_with_parquet(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    get_limiter: Arc<GetLimiter>,
    max_query_bytes: usize,
    max_tenant_bytes: usize,
    parallel_final_aggregation: bool,
    bucketed_final_aggregation: bool,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    declared_columns: Option<Arc<dyn ravel_sql::DeclaredColumnSource>>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    parquet_profiles: Option<crate::config::ParquetProfiles>,
    ddl_min_grace_ms: u64,
    spill: &SqlSpillInputs,
    read_gate: Arc<ravel_cpu_gate::ReadGate>,
    scan_decode_inline: bool,
    ipcrypt: ravel_sql::IpcryptDeployment,
    cache_bulk_scan_min_bytes: Option<u64>,
) -> anyhow::Result<crate::sql::SqlState> {
    let external = parquet_profiles.map(|config| {
        let stores = ravel_sql::ProfileStores::new(config.profiles);
        let stores = match config.ravel_bucket {
            Some(crate::config::RavelBucket::S3 {
                bucket,
                endpoint,
                region,
            }) => stores.refusing(ravel_sql::RavelBucket::S3 {
                endpoint,
                region,
                bucket,
            }),
            Some(crate::config::RavelBucket::Gcs { bucket, endpoint }) => {
                stores.refusing(ravel_sql::RavelBucket::Gcs { endpoint, bucket })
            }
            None => stores,
        };
        Arc::new(stores) as Arc<dyn ravel_sql::ExternalStores>
    });
    let sources = ravel_sql::ParquetSources::new(
        store.clone(),
        external,
        get_limiter.clone(),
        cache.clone(),
        ravel_sql::DEFAULT_PARQUET_METADATA_CACHE_BYTES,
    );
    build_sql_state_inner(
        catalog,
        store,
        tenant_resolver,
        cache,
        engine_config,
        get_limiter,
        max_query_bytes,
        max_tenant_bytes,
        parallel_final_aggregation,
        bucketed_final_aggregation,
        query_accounting,
        query_admission,
        declared_columns,
        process_memory_budget,
        Some(sources),
        Some(read_gate),
        scan_decode_inline,
        ddl_min_grace_ms,
        spill,
        ipcrypt,
        cache_bulk_scan_min_bytes,
    )
}

#[cfg(feature = "sql")]
#[allow(clippy::too_many_arguments)]
fn build_sql_state_inner(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    get_limiter: Arc<GetLimiter>,
    max_query_bytes: usize,
    max_tenant_bytes: usize,
    parallel_final_aggregation: bool,
    bucketed_final_aggregation: bool,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    declared_columns: Option<Arc<dyn ravel_sql::DeclaredColumnSource>>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    parquet: Option<ravel_sql::ParquetSources>,
    read_gate: Option<Arc<ravel_cpu_gate::ReadGate>>,
    scan_decode_inline: bool,
    ddl_min_grace_ms: u64,
    spill: &SqlSpillInputs,
    ipcrypt: ravel_sql::IpcryptDeployment,
    cache_bulk_scan_min_bytes: Option<u64>,
) -> anyhow::Result<crate::sql::SqlState> {
    use ravel_query::{LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor};

    let mut config = SqlConfig {
        engine: engine_config,
        max_query_bytes,
        // ADR-0094 (amended by #741): the process-wide exact-typed repartition
        // switch, from `--sql-parallel-final-aggregation`. Default-on; the
        // `=false` opt-out leaves every query single-partitioned.
        parallel_final_aggregation,
        // Issue #680 / ADR-0102 decision 2's amendment: the shipped default,
        // with no flag. Nothing an operator sets should be able to reintroduce
        // an aggregation whose memory scales with the partition count; the
        // `SqlConfig` field exists as an in-process escape hatch and for the
        // regression test's red side, not as a server-level knob.
        skip_partial_aggregation: SqlConfig::default().skip_partial_aggregation,
        // ADR-0774: the shipped default, with no flag, for the same reason as
        // the line above. The rewrite is invisible to results (the same rows,
        // in the same order, under the same schema), so the `SqlConfig` field
        // is an in-process escape hatch and the regression fixture's red side,
        // not a server-level knob.
        late_materialization_extra_columns: SqlConfig::default().late_materialization_extra_columns,
        // The bounded top-k grouped aggregate's limit ceiling: the shipped
        // default, with no flag, for the same reason as the two lines above.
        // The rewrite is exact for the shape it admits, so the `SqlConfig`
        // field is an in-process escape hatch and the tests' rule-off side,
        // not a server-level knob.
        bounded_topk_max_limit: SqlConfig::default().bounded_topk_max_limit,
        // The first-groups bound under an unordered `GROUP BY ... LIMIT`
        // (ADR-3985): the shipped default, with no flag, for the same reason.
        bounded_groups_max_limit: SqlConfig::default().bounded_groups_max_limit,
        // The per-partition TopK's limit ceiling: the shipped default, with
        // no flag, for the same reason as the line above.
        partition_topk_max_limit: SqlConfig::default().partition_topk_max_limit,
        // ADR-3565 decision 1 (issue #3608): the bucketed final aggregation,
        // from `--sql-bucketed-final-aggregation`, at its default tuning.
        // Off by default, which installs no rule.
        bucketed_final_aggregation: bucketed_final_aggregation
            .then(ravel_sql::BucketedFinalAggregation::default),
        // ADR-0954, amended by issue #2416: `with_spill_resolved` below fills
        // this from `--sql-spill`, the `RAVEL_SQL_SPILL_DIR`/
        // `RAVEL_SQL_SPILL_MAX_BYTES` pair, and `--cache-dir`, in that order;
        // none of them leaves it `None` and the memory budget refuses as before.
        spill: None,
        // Issue #913: the per-segment scan timeline is a bench-reporter-only
        // knob, with no flag, for the same reason as the lines above. Off by
        // default so a production logs query never pays its per-segment
        // metric registrations.
        segment_timing: SqlConfig::default().segment_timing,
        aggregate_reserve_ahead: SqlConfig::default().aggregate_reserve_ahead,
    }
    .with_spill_resolved(
        spill.off,
        spill
            .cache_dir
            .as_ref()
            .map(CacheDirSpillInputs::as_cache_dir_spill),
    )?;
    if let Some(resolved) = config.spill.as_mut() {
        resolved.merge_fan_in = spill.merge_fan_in;
    }
    let max_deadline = config.engine.deadline;
    let mut metrics_fetcher = SegmentFetcher::new(store.clone())
        .with_get_limiter(get_limiter.clone())
        .with_memory_budget(process_memory_budget.clone());
    // ADR-0107's read-shape crossover, from `--logs-block-range-threshold` via
    // `QueryBudgets::apply_to_engine`. This is the single wiring point for it, so
    // an operator who sets the flag to `u64::MAX` gets whole-object logs reads
    // (the pre-ADR-0107 shape) on every SQL logs scan this process serves.
    // ADR-1195: GET concurrency is now the single process-wide `GetLimiter`
    // (`--store-get-concurrency`, legacy `--fetch-concurrency`), shared with
    // the metrics and spans fetchers via `with_get_limiter` below, not a
    // private pool sized from the flag.
    // `--logs-request-cost-bytes` (ADR-0904) reaches the same fetcher the same
    // way: the request cost lives on the block-range fetcher this builder owns,
    // so the flag has to be handed over here or the fetcher keeps its
    // compiled-in `DEFAULT_LOG_REQUEST_COST_BYTES` whatever the flag says, and
    // with it the coalescing gap, the whole-object crossover, and the fast
    // path's projection routing that are all derived from it.
    // ADR-0996 decision 2: the two quantities above are the RESOLVED ones.
    // `QueryBudgets::apply_to_engine` runs `--logs-fetch-policy` through
    // `ravel_query::resolve_logs_fetch` before this config exists, so
    // `request-minimal` (and `cost-based` at a profile with neither byte
    // prices nor timings) arrives here as a saturated request cost AND a
    // saturated routing threshold, which is what makes the policy select the
    // read shape instead of being inert. `cost-based` with a finite rate also
    // resolves the projection break-even (ADR-2414 decision A3), which has to
    // reach the fetcher the same way.
    // `--logs-max-fetch-run-bytes` is the fetch bound: it caps one covering
    // GET's length on every policy, so it has to be handed to the fetcher here
    // like the other three or the fetcher keeps its compiled-in 64 MiB.
    let mut logs_fetcher = LogSegmentFetcher::new(store.clone())
        .with_block_range_threshold(config.engine.logs_block_range_threshold)
        .with_get_limiter(get_limiter.clone())
        .with_request_cost_bytes(config.engine.logs_request_cost_bytes)
        .with_projection_break_even_bytes(config.engine.logs_projection_break_even_bytes)
        .with_max_fetch_run_bytes(config.engine.logs_max_fetch_run_bytes)
        .map_err(|err| anyhow::anyhow!("invalid logs fetch bound: {err}"))?
        .with_memory_budget(process_memory_budget.clone());
    // The spans fetcher (RSPAN) reads the same object store, with the default
    // RspanConfig (ADR-0045 decision 5), and shares the process read cache
    // below (ADR-2707 decision 10): a trace lookup's ranged reads are keyed
    // `(tenant_hash, content_hash, offset, len)` like the logs fetcher's, so a
    // repeated lookup is served without a GET. Its `fetch_accounted` path is
    // tenant-checked and accounted (ADR-0045 via #1080), so a `spans` query is
    // isolated and metered like any other.
    // ADR-1195: shares the same process-wide `GetLimiter` as the metrics and
    // logs fetchers above, not a private pool.
    let mut span_fetcher = SpanSegmentFetcher::new(store.clone())
        .with_get_limiter(get_limiter)
        .with_memory_budget(process_memory_budget.clone());
    // ADR-1702 decision 7: the `logs` and `spans` scans decode each block on
    // the read gate the fetcher carries, and the logs fetcher's opens run on
    // it too. The `samples` scan's RSEG catalog decodes run on it as well.
    // `--sql-scan-decode-inline` leaves it off the logs and spans fetchers
    // only, so those scans decode inline (issue #2795).
    if let Some(gate) = read_gate {
        metrics_fetcher = metrics_fetcher.with_read_gate(gate.clone());
        if !scan_decode_inline {
            logs_fetcher = logs_fetcher.with_read_gate(gate.clone());
            span_fetcher = span_fetcher.with_read_gate(gate);
        }
    }
    if let Some(cache) = cache {
        metrics_fetcher = metrics_fetcher.with_cache(cache.clone());
        logs_fetcher = logs_fetcher.with_cache(cache.clone());
        span_fetcher = span_fetcher.with_cache(cache);
    }
    // The metrics fetcher (RSEG), the logs fetcher (RLOG), and the spans
    // fetcher (RSPAN) all read the same object store; the executor uses
    // whichever the query's target table needs (ADR-0033, extended to `spans`
    // by ADR-0045 decision 5). The `alerts` and `audit` tables (ADR-1101
    // decision 1) need no fetcher of their own: their records ride RLOG, so
    // they read through the logs fetcher above, cache included.
    let executor = SqlExecutor::new(
        catalog,
        metrics_fetcher,
        logs_fetcher,
        span_fetcher,
        config,
        max_tenant_bytes,
    );
    // ADR-0090 decision 2: one source, installed on the one shared executor, so
    // the HTTP endpoint and Flight SQL resolve a tenant's declared columns
    // through the same cache. Flight needs no second wiring point: its
    // `get_flight_info` resolves through this executor's
    // `resolve_declared_columns` and pins the result into the ticket, so `DoGet`
    // plans against the declaration `get_flight_info` saw.
    let executor = match declared_columns {
        Some(source) => executor.with_declared_column_source(source),
        None => executor,
    };
    let executor = executor.with_process_memory_budget(process_memory_budget);
    let executor = match parquet {
        Some(sources) => executor.with_parquet_sources(sources),
        None => executor,
    };
    // The deployment's `sys/gc`-derived minimum grace, which the manifest
    // writer spends as its resolve-to-put budget; see
    // `WRITER_MIN_USABLE_GRACE_MS` for why it is not query protection.
    let executor = executor
        .with_ddl_min_grace_ms(ddl_min_grace_ms)
        .with_ipcrypt_deployment(ipcrypt)
        .with_cache_bulk_scan_min_bytes(cache_bulk_scan_min_bytes);
    Ok(crate::sql::SqlState {
        executor: Arc::new(executor),
        tenant_resolver,
        // The audit writer (ADR-0042 decision 4) writes to the same store the
        // executor reads from.
        store,
        clock: Arc::new(ravel_ingest::SystemClock),
        max_deadline,
        query_accounting,
        query_admission,
        // The SQL HTTP audit routes through the QueryAuditSink seam. This
        // function builds no pipeline of its own, so it defaults to the no-op
        // sink; `start` (lib.rs) overrides this field with the process-wide
        // AuditPipeline's sink once it builds one (ADR-0062 decision 2b), in
        // every mode that installs a pipeline.
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod catalog_cache_tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::ByteLimit;
    use ravel_query::http::StaticBearerTokenResolver;
    use std::collections::HashMap;

    /// ADR-0061 decision 1: the bytes-scanned budget resolved from
    /// `--limits-file` must reach the PromQL/HTTP engine `build_app_state`
    /// builds, not be dropped to the `EngineConfig::default()` `Unlimited`.
    /// Asserts on the engine's own `config()`, the value the fetch fan-outs
    /// actually check.
    #[test]
    fn build_app_state_threads_the_byte_budget_into_the_engine() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let engine_config = EngineConfig {
            max_bytes_scanned: ByteLimit::Bounded(4096),
            ..EngineConfig::default()
        };
        let state = build_app_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            engine_config,
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            None,
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
        );
        assert_eq!(
            state.engine.config().max_bytes_scanned,
            ByteLimit::Bounded(4096),
            "the PromQL engine must enforce the configured byte budget, not the default Unlimited"
        );
    }

    /// `--disable-cache` (passed as `disable_cache: true`) must
    /// build a catalog with no byte cache constructed, the byte-cache analogue
    /// of the `None` fetcher cache `build_cache` returns. Asserts on the
    /// absence of the counters handle, not a zero hit count.
    #[test]
    fn build_catalog_disable_cache_constructs_no_byte_cache() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store,
            1,
            true,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert!(
            catalog.byte_cache_metrics().is_none(),
            "--disable-cache must leave the catalog with no byte cache constructed"
        );
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            0,
            "the disabled catalog config carries the byte-cache disable sentinel"
        );
    }

    /// ADR-0071 (finding 4): `fragments_json` renders one camelCase object per
    /// slice with the deliverable's fields plus `wireBytesConsumed` (issue
    /// #1687 part B), and an empty input renders an empty array (the query
    /// handler then omits the field entirely).
    #[test]
    fn fragments_json_renders_camelcase_per_slice_shape() {
        let entries = vec![
            crate::distrib::FragmentStatEntry {
                worker_endpoint: "10.0.0.1:7000".to_string(),
                segment_count: 3,
                bytes_reported: 4096,
                wire_bytes_consumed: 512,
                status: "ok",
            },
            crate::distrib::FragmentStatEntry {
                worker_endpoint: "192.0.2.1:9".to_string(),
                segment_count: 1,
                bytes_reported: 0,
                wire_bytes_consumed: 0,
                status: "fallback",
            },
        ];
        let json = fragments_json(&entries);
        let array = json.as_array().expect("fragments is a JSON array");
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["workerEndpoint"], "10.0.0.1:7000");
        assert_eq!(array[0]["segmentCount"], 3);
        assert_eq!(array[0]["bytesReported"], 4096);
        assert_eq!(array[0]["wireBytesConsumed"], 512);
        assert_eq!(array[0]["status"], "ok");
        assert_eq!(array[1]["workerEndpoint"], "192.0.2.1:9");
        assert_eq!(array[1]["wireBytesConsumed"], 0);
        assert_eq!(array[1]["status"], "fallback");
        assert!(
            fragments_json(&[])
                .as_array()
                .expect("empty renders an array")
                .is_empty(),
            "no entries render an empty array, which the handler omits"
        );
    }

    /// with caching on, `build_catalog`'s `cache_max_bytes` argument (the
    /// resolved `--catalog-cache-max-bytes`, ADR-2023) must bound the catalog
    /// byte cache. The value reaches `CatalogConfig::byte_cache_max_bytes`,
    /// and the byte cache (with its counters handle) is constructed.
    #[test]
    fn build_catalog_wires_cache_max_bytes_through_to_the_byte_cache() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let budget = 7 * 1024 * 1024;
        let catalog = build_catalog(
            store,
            1,
            false,
            budget,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            budget,
            "the catalog cache argument must bound the catalog byte cache"
        );
        assert!(
            catalog.byte_cache_metrics().is_some(),
            "an enabled catalog byte cache must expose its counters handle for /metrics"
        );
    }

    /// Issue #1141 reachability for the derived catalog cache ceiling: the
    /// number the resolution produced on an injected host is the number the
    /// catalog byte cache is bounded by, through the same `build_catalog`
    /// argument `crate::start` passes (`ServerConfig::catalog_cache_max_bytes`,
    /// which `main` fills from
    /// `ResolvedPerformanceDefaults::catalog_cache_max_bytes`). The catalog
    /// cache is a SEPARATE ceiling from the fetcher cache: both carve from
    /// `memory_budget_bytes` (`MemTotal` minus
    /// [`crate::config::effective_memory_overhead_reserve_bytes`], ADR-1170
    /// decision 3, scaled below 8 GiB of memory rather than the fixed
    /// [`crate::config::MEMORY_OVERHEAD_RESERVE_BYTES`] -- ADR-1170's small-host
    /// reserve amendment), not from raw `MemTotal`. On the reference profile the 30,064,771,072
    /// budget resolves to 5% for the catalog cache (1,503,238,553) while the
    /// fetcher cache `store::build_cache` bounds stays at 25%
    /// (7,516,192,768), so the two independent LRU caches do not each claim
    /// the full share. Since ADR-2023 an explicit `--cache-max-bytes` bounds
    /// the fetcher cache only: the catalog cache keeps deriving its own 5%
    /// share unless `--catalog-cache-max-bytes` is set.
    ///
    /// Prove-the-test: pass `resolved.cache_max_bytes` (the fetcher 25% number)
    /// to `build_catalog` here and the first assertion reads 7,516,192,768
    /// against the expected 1,503,238,553.
    #[test]
    fn the_derived_cache_max_bytes_reaches_the_catalog_byte_cache() {
        use clap::Parser;

        use crate::config::HostProfile;

        let cli = crate::Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let resolved = cli
            .resolve_performance(HostProfile::new(
                16,
                Some(32_212_254_720),
                Some(32_212_254_720),
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        // The two caches derive to different ceilings on the same host.
        assert_eq!(resolved.cache_max_bytes, 7_516_192_768);
        assert_eq!(resolved.catalog_cache_max_bytes, 1_503_238_553);

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store,
            1,
            cli.disable_cache,
            resolved.catalog_cache_max_bytes,
            cli.cache_dir.clone(),
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            1_503_238_553,
            "the catalog byte cache must be bounded by the derived catalog ceiling (5% of \
             memory_budget_bytes), not the fetcher cache's 25% and not the compiled-in 256 MiB"
        );

        // ADR-2023: an explicit --cache-max-bytes no longer reaches the
        // catalog cache; only --catalog-cache-max-bytes does.
        let flagged = crate::Cli::try_parse_from(["ravel-server", "--cache-max-bytes", "4096"])
            .expect("flag parses");
        let resolved = flagged
            .resolve_performance(HostProfile::new(
                16,
                Some(32_212_254_720),
                Some(32_212_254_720),
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        assert_eq!(resolved.cache_max_bytes, 4096);
        assert_eq!(
            resolved.catalog_cache_max_bytes, 1_503_238_553,
            "--cache-max-bytes must not disturb the catalog cache's own derived share"
        );

        let catalog_flagged =
            crate::Cli::try_parse_from(["ravel-server", "--catalog-cache-max-bytes", "4096"])
                .expect("flag parses");
        let resolved = catalog_flagged
            .resolve_performance(HostProfile::new(
                16,
                Some(32_212_254_720),
                Some(32_212_254_720),
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        assert_eq!(resolved.catalog_cache_max_bytes, 4096);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store,
            1,
            catalog_flagged.disable_cache,
            resolved.catalog_cache_max_bytes,
            catalog_flagged.cache_dir.clone(),
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            4096,
            "an explicit --catalog-cache-max-bytes bounds the catalog byte cache at the flag value"
        );
    }

    /// Issue #1735 reachability: `build_catalog` must set
    /// `cache_capacity_per_tenant` from `derive_cache_capacity_per_tenant`
    /// applied to the `shard_count`/`max_flush_delay` it was actually passed,
    /// never leave the flat `DEFAULT_CACHE_CAPACITY_PER_TENANT` in place.
    #[test]
    fn build_catalog_passes_the_derived_cache_capacity_per_tenant() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let shard_count = 4;
        let max_flush_delay = std::time::Duration::from_secs(2);
        let expected =
            ravel_catalog::derive_cache_capacity_per_tenant(shard_count, max_flush_delay);
        assert_eq!(
            expected, 25_000,
            "sanity: the shipped ingest defaults derive 4 * 6 * 1800 * 3 = 129,600, capped at \
             the 45 MB per-tenant budget's 25,000 entries"
        );
        assert_ne!(
            expected,
            ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT,
            "sanity: the derived value must actually differ from the flat default"
        );

        let catalog = build_catalog(
            store,
            shard_count,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            max_flush_delay,
        )
        .expect("catalog builds");

        assert_eq!(
            catalog.config().cache_capacity_per_tenant,
            expected,
            "build_catalog must pass the derived capacity through, not the flat constant"
        );
    }

    /// ADR-1733 decision 2: `build_catalog` takes the derived per-process
    /// ceiling as its input rather than the constant, so the ceiling the
    /// catalog enforces is the number the caller resolved. `None` still means
    /// "no ceiling was resolved" and leaves `CatalogConfig`'s own default,
    /// which is the per-prefix bound.
    #[test]
    fn build_catalog_applies_the_passed_resolve_ceiling() {
        let derived = crate::config::derive_catalog_resolve_concurrency(4);
        assert_eq!(
            derived, 512,
            "sanity: four concurrent queries derive 4 * 128 = 512"
        );
        assert_ne!(
            derived,
            ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY,
            "sanity: the derived ceiling must differ from the per-prefix default, or this test \
             passes on a call site that ignores its argument"
        );

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store,
            4,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            Some(derived),
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert_eq!(
            catalog.config().resolve_get_concurrency,
            derived,
            "build_catalog must apply the ceiling it was passed"
        );

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let unset = build_catalog(
            store,
            4,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog builds");
        assert_eq!(
            unset.config().resolve_get_concurrency,
            ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY,
            "an unresolved ceiling leaves CatalogConfig's own default in place"
        );
    }

    #[test]
    fn disable_cache_holds_the_record_cache_capacity_at_the_flat_floor() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let shard_count = 4;
        let max_flush_delay = std::time::Duration::from_secs(2);
        assert_eq!(
            ravel_catalog::derive_cache_capacity_per_tenant(shard_count, max_flush_delay),
            25_000,
            "sanity: without the flag these arguments derive 25,000"
        );

        let catalog = build_catalog(
            store,
            shard_count,
            true,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            max_flush_delay,
        )
        .expect("catalog builds");

        assert_eq!(
            catalog.config().cache_capacity_per_tenant,
            ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT,
            "--disable-cache is the memory-constrained-container flag, so it must hold the \
             capacity at the floor rather than the derived value"
        );
        assert_eq!(
            catalog.config().commit_cache_max_bytes_per_tenant(),
            9_000_000,
            "both byte budgets follow the floor, so the flag costs 9 MB per cache and 18 MB \
             per tenant, not the 22.5 MB per cache the derived capacity would budget"
        );
        assert_eq!(
            catalog.config().compaction_cache_max_bytes_per_tenant(),
            9_000_000
        );
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            0,
            "--disable-cache still turns the byte cache off"
        );
    }

    /// A `ServerConfig` carrying whatever `start` would have resolved for a
    /// plain `--mode all` process. Only the catalog inputs matter to the
    /// tests below; the rest is here because `ServerConfig` has no `Default`.
    fn server_config() -> crate::ServerConfig {
        crate::ServerConfig {
            mode: crate::Mode::All,
            listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
            listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
            shard_count: 1,
            max_inflight_flushes: 1,
            max_inflight_flushes_per_tenant: None,
            flush_object_backstop_multiple: 4,
            max_queued_flushes: 8,
            adaptive_flush_delay: false,
            max_flush_delay: Duration::from_secs(2),
            max_flush_delay_idle: Duration::from_secs(40),
            min_flush_bytes: 256 * 1024,
            idle_flush_byte_floor: 0,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            mtls_listener: None,
            fold_tenants: Vec::new(),
            fold: crate::FoldTaskConfig::default(),
            maintain: crate::MaintenanceTaskConfig::default(),
            alerting: crate::AlertEvalConfig::default(),
            oidc_refresh: None,
            otap: false,
            otap_max_decoded_batch_bytes: crate::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
            limits: crate::LimitsConfig::default(),
            metrics_tenant_labels: false,
            deployment_key: None,
            gc: ravel_maintain::GcConfigValues::maintain_defaults(),
            query_deadline: EngineConfig::default().deadline,
            store_probe_interval: crate::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
            admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
            query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
            max_s3_requests: EngineConfig::default().max_s3_requests,
            query_budgets: crate::config::QueryBudgets::default(),
            scrub_period: Duration::from_secs(7 * 86_400),
            indexed_fields: crate::postings_config::IndexedFieldConfig::default(),
            typed_attr_columns: crate::typed_attr_config::TypedAttrColumnConfig::default(),
            parquet_profiles: None,
            disable_cache: false,
            cache_max_bytes: 256 * 1024 * 1024,
            catalog_cache_max_bytes: 256 * 1024 * 1024,
            process_memory_budget_bytes: u64::MAX,
            process_memory_budget_is_fallback: false,
            cache_dir: None,
            catalog_resolve_concurrency: None,
            cpu_gate_permits: Default::default(),
            ingest_hold: Default::default(),
            ingest_concurrency_limit: crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(
                1024,
            ),
            max_pending_strict_acks: 8_192,
            ingest_buffer_budget_limit: ravel_ingest::IngestByteBudgetLimit::Unlimited,
            idle_tenant_state_ttl: Duration::from_secs(3600),
            distrib: None,
            remote_clusters: Vec::new(),
            audit_pipeline: None,
            audit_text: ravel_maintain::AuditTextPolicy::default(),
            shutdown_timeout: crate::DEFAULT_SHUTDOWN_TIMEOUT,
            drain_settle_interval: Duration::ZERO,
            max_ingest_lag: crate::DEFAULT_MAX_INGEST_LAG,
            strict_flush_policy: crate::config::StrictFlushPolicy::Aged,
        }
    }

    /// ADR-1733 decision 2, the last hop: the ceiling `start` resolved onto
    /// `ServerConfig::catalog_resolve_concurrency` must reach the `Catalog`
    /// the process serves queries from. `start` itself cannot be called from
    /// a test, which is why the field mapping lives in
    /// `build_catalog_for_server` and this asserts on the catalog that
    /// function returns.
    ///
    /// RED: in `build_catalog_for_server`, pass `None` for the ceiling, or
    /// the `DEFAULT_RESOLVE_PREFIX_CONCURRENCY` constant, instead of
    /// `config.catalog_resolve_concurrency`. Both type-check, and the
    /// catalog's ceiling is then 128 rather than the 512 asserted here.
    #[test]
    fn build_catalog_for_server_applies_the_configured_resolve_ceiling() {
        let derived = crate::config::derive_catalog_resolve_concurrency(4);
        assert_ne!(
            derived,
            ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY,
            "sanity: the derived ceiling must differ from the per-prefix default, or this \
             test could not tell a dropped ceiling from an applied one"
        );
        let config = crate::ServerConfig {
            catalog_resolve_concurrency: Some(derived),
            cpu_gate_permits: Default::default(),
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 7_200_000_000_000).expect("catalog builds");
        assert_eq!(
            catalog.config().resolve_get_concurrency,
            derived,
            "the resolved per-process ceiling must reach the catalog the server queries"
        );
    }

    /// `--fold-key-index-leaves` reaches the catalog both the scheduled and
    /// the on-demand fold run with, and the leaf build stays off without it.
    #[test]
    fn build_catalog_for_server_maps_the_key_index_leaf_switch() {
        for enabled in [false, true] {
            let config = crate::ServerConfig {
                fold: crate::FoldTaskConfig {
                    build_key_index_leaves: enabled,
                    ..crate::FoldTaskConfig::default()
                },
                cpu_gate_permits: Default::default(),
                ..server_config()
            };
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let catalog = build_catalog_for_server(store, &config, 7_200_000_000_000)
                .expect("catalog builds");
            assert_eq!(
                catalog.config().build_key_index_leaves,
                enabled,
                "the fold's key-index leaf switch must reach the server catalog"
            );
        }
        assert!(
            !crate::FoldTaskConfig::default().build_key_index_leaves,
            "the leaf build is off by default"
        );
    }

    /// `--fold-metrics-label-index` reaches the catalog both the scheduled
    /// and the on-demand fold run with, and the metrics leaf stays off
    /// without it.
    #[test]
    fn build_catalog_for_server_maps_the_metrics_label_index_switch() {
        for enabled in [false, true] {
            let config = crate::ServerConfig {
                fold: crate::FoldTaskConfig {
                    build_key_index_leaves: true,
                    metrics_label_index: enabled,
                    ..crate::FoldTaskConfig::default()
                },
                cpu_gate_permits: Default::default(),
                ..server_config()
            };
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let catalog = build_catalog_for_server(store, &config, 7_200_000_000_000)
                .expect("catalog builds");
            assert_eq!(
                catalog.config().metrics_label_index,
                enabled,
                "the fold's metrics label-index switch must reach the server catalog"
            );
        }
        assert!(
            !crate::FoldTaskConfig::default().metrics_label_index,
            "the metrics leaf build is off by default"
        );
    }

    /// `--fold-key-index-build-max-pairs` reaches the catalog the folds run
    /// with, and its default is the catalog's.
    #[test]
    fn build_catalog_for_server_maps_the_key_index_pair_budget() {
        let config = crate::ServerConfig {
            fold: crate::FoldTaskConfig {
                key_index_build_max_pairs: 1_234,
                ..crate::FoldTaskConfig::default()
            },
            cpu_gate_permits: Default::default(),
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 7_200_000_000_000).expect("catalog builds");
        assert_eq!(catalog.config().key_index_build_max_pairs, 1_234);
        assert_eq!(
            crate::FoldTaskConfig::default().key_index_build_max_pairs,
            ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS
        );
    }

    /// ADR-1306 follow-up task 5: the per-query S3 request budget a running
    /// server enforces is derived with the seal margin of the catalog that
    /// server folds and resolves with, not with `ravel_query`'s
    /// `SealMargin::REFERENCE` constants.
    ///
    /// Both halves come from the production paths. The budget comes from the
    /// real `Cli::resolve_max_s3_requests`, which is
    /// `resolve_max_s3_requests_with(server_seal_margin())`, the exact call
    /// `main.rs` makes to fill `ServerConfig::max_s3_requests`; the margin
    /// comes from the
    /// `CatalogConfig` `build_catalog_for_server` returns, which is the config
    /// `start` hands to both resolve and `fold::spawn`. The three durations
    /// are restated nowhere here, so a flag that later moves one of them on
    /// the catalog and not on the budget path fails this test rather than
    /// leaving the budget sized for a span the fold no longer seals against.
    ///
    /// Every server path pins the catalog's compiled-in margin today, and
    /// `SealMargin::REFERENCE` holds those same three durations, so at the
    /// production inputs the resolution returns the same number whether it
    /// reads the margin off the catalog or falls back to the reference
    /// constants. The equality between the resolved budget and
    /// `derive_max_s3_requests_for` at the running margin therefore cannot,
    /// on its own, tell those two apart. The assertion that can is the one
    /// over the seam: `Cli::resolve_max_s3_requests_with` is called with a
    /// margin an hour longer than the reference, and the budget it returns is
    /// pinned to `derive_max_s3_requests_for` at THAT margin and asserted
    /// different from `derive_max_s3_requests`, the reference-margin wrapper.
    /// A resolution that ignores the margin it is handed fails the first of
    /// those two; the second is not a restatement of it, because it is what
    /// fails if `derive_max_s3_requests_for` itself stops reading its margin,
    /// which would make both derivations agree and leave the first passing.
    ///
    /// RED: replace `seal_margin` in the body of
    /// `Cli::resolve_max_s3_requests_with` (`src/config.rs`) with
    /// `crate::query::server_seal_margin()`, which is exactly the revert to
    /// the reference derivation that changes no production number. The
    /// `resolved_at_longer` equality below then reports
    /// `left: Bounded(385700)` against `right: Bounded(580100)`, the budget
    /// the hour-longer margin was required to produce.
    ///
    /// The `server_seal_margin() == running` assertion is the other direction:
    /// it fails if the catalog `build_catalog_for_server` returns ever seals
    /// on a margin the budget path does not read.
    #[test]
    fn derived_request_budget_uses_the_catalogs_seal_margin() {
        use clap::Parser;

        let cli = crate::config::Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.shards, 4,
            "guards the shard default this test is pinned to"
        );
        let cadence = cli
            .resolve_flush_cadence()
            .expect("server defaults resolve a flush cadence");

        // The catalog the server really queries and folds with, built through
        // the one `ServerConfig` -> `Catalog` mapping `start` uses.
        let config = crate::ServerConfig {
            shard_count: cli.shards,
            max_flush_delay: cadence.max_flush_delay,
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 7_200_000_000_000).expect("catalog builds");
        let running = ravel_query::SealMargin::from_catalog_config(catalog.config());

        assert_eq!(
            server_seal_margin(),
            running,
            "the seal margin the derived budget is sized from must be the seal margin of \
             the catalog `build_catalog_for_server` returns"
        );

        let expected =
            ravel_query::derive_max_s3_requests_for(cli.shards, cadence.max_flush_delay, running);
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("server defaults resolve a bounded budget"),
            ravel_query::RequestLimit::Bounded(expected),
            "the budget a running server enforces must be the derivation at the running \
             catalog's own seal margin"
        );
        assert_eq!(
            expected, 385_700,
            "sanity: at 4 shards, the 2s cadence and the catalog's 4,800s seal margin the \
             derivation is the figure ADR-1306's dictionary GET amendment states, and the figure \
             the flag help text and docs/query-engine.md give an operator"
        );

        // The equality above holds just as well if the resolution ignores the
        // margin and falls back to `SealMargin::REFERENCE`, because the
        // running margin IS the reference triple today. The seam is what
        // separates them: resolve at a margin no production input produces,
        // one hour of extra `fold_safety_margin`, and require the budget to
        // follow it.
        let longer = ravel_query::SealMargin {
            fold_safety_margin: running.fold_safety_margin + Duration::from_secs(3_600),
            ..running
        };
        let at_longer =
            ravel_query::derive_max_s3_requests_for(cli.shards, cadence.max_flush_delay, longer);
        let at_reference = ravel_query::derive_max_s3_requests(cli.shards, cadence.max_flush_delay);
        let resolved_at_longer = cli
            .resolve_max_s3_requests_with(longer)
            .expect("an hour-longer seal margin resolves a bounded budget");
        assert_eq!(
            resolved_at_longer,
            ravel_query::RequestLimit::Bounded(at_longer),
            "`resolve_max_s3_requests_with` must derive at the seal margin it is handed, \
             not at one it reads back from the catalog or from SealMargin::REFERENCE"
        );
        assert_ne!(
            resolved_at_longer,
            ravel_query::RequestLimit::Bounded(at_reference),
            "a budget resolved at an hour-longer seal margin must differ from \
             `derive_max_s3_requests`, the reference-margin wrapper: if these agree, the \
             resolution ignored its margin argument and the equality above is checking \
             nothing"
        );

        // What that hour is worth, so the assertions above are pinned to a
        // figure and not just to each other. A `fold_safety_margin` one hour
        // longer adds an hour to `healthy_tail_max` and an hour to
        // `lag_allowance`, so `covered_span` grows by 7,200s: 3,600 more
        // flushes per shard at the 2s cadence, each budgeted at 9 requests,
        // with the 3/2 headroom over 4 shards.
        assert_eq!(
            at_longer - expected,
            3_600 * ravel_query::BUDGETED_REQUESTS_PER_UNSEALED_FLUSH * 3 / 2
                * u64::from(cli.shards),
            "an hour of extra seal margin must widen the derived budget by an hour of tail \
             on every shard"
        );

        // ADR-1306 decision 5: an explicit flag is still used verbatim, seal
        // margin or not. Checked on both the no-argument path and the seam,
        // since the seam is the one a margin could leak into.
        let cli = crate::config::Cli::try_parse_from(["ravel-server", "--max-s3-requests", "999"])
            .expect("explicit flag parses");
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("explicit override resolves"),
            ravel_query::RequestLimit::Bounded(999),
            "an explicit --max-s3-requests is used verbatim, not re-derived"
        );
        assert_eq!(
            cli.resolve_max_s3_requests_with(longer)
                .expect("explicit override resolves at any seal margin"),
            ravel_query::RequestLimit::Bounded(999),
            "an explicit --max-s3-requests stays verbatim whatever seal margin the \
             resolution is handed"
        );
    }

    /// ADR-1306 follow-up task 5 and the 2026-09-27 refusal-threshold
    /// amendment's open half: the `EngineConfig` the server's query surfaces
    /// enforce must carry the seal margin and HEAD cache TTL of the catalog
    /// this process resolves through, and the fold interval the fold loop this
    /// process spawns really sleeps. Those three are the terms of
    /// `EngineConfig::fold_lag_threshold`, which decides whether a request
    /// budget refusal names fold lag.
    ///
    /// Both halves go through the production path.
    /// [`build_engine_config`] is the exact function `start` calls, and the
    /// `CatalogConfig` handed to it here is the one
    /// [`build_catalog_for_server`] returned, which `start` hands to resolve
    /// and to `fold::spawn` alike.
    ///
    /// The production inputs cannot tell a wired value from the reference
    /// constant on their own: `server_catalog_config_base` is
    /// `CatalogConfig::default`, whose three seal durations are exactly
    /// `SealMargin::REFERENCE` and whose `head_cache_ttl_ns` is exactly
    /// `REFERENCE_HEAD_CACHE_TTL`, and `FoldTaskConfig::default`'s interval is
    /// exactly `REFERENCE_FOLD_INTERVAL`. So each of the three is also driven
    /// across a seam no production input produces, and required to follow it.
    ///
    /// RED, one per field, each the exact revert this test exists to catch:
    /// in [`build_engine_config`], replace `seal_margin` with
    /// `ravel_query::SealMargin::REFERENCE`, `fold_interval` with
    /// `ravel_query::REFERENCE_FOLD_INTERVAL`, or `head_cache_ttl` with
    /// `ravel_query::REFERENCE_HEAD_CACHE_TTL`. Each type-checks, each changes
    /// no production number, and each fails exactly one of the three seam
    /// assertions below.
    #[test]
    fn derived_engine_config_uses_the_running_fold_and_catalog_values() {
        // The production path first: the running catalog's own config, and
        // the fold config `start` hands `fold::spawn`.
        let config = server_config();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 7_200_000_000_000).expect("catalog builds");
        assert_eq!(
            catalog.config().head_cache_ttl_ns,
            server_catalog_config_base().head_cache_ttl_ns,
            "the startup sys/gc HEAD cache TTL check reads `server_catalog_config_base`, so the \
             catalog `build_catalog_for_server` returns must run on that same TTL"
        );
        let engine =
            build_engine_config(&config, catalog.config()).expect("server defaults are valid");
        assert_eq!(
            engine.seal_margin,
            ravel_query::SealMargin::from_catalog_config(catalog.config()),
            "the engine must seal-margin off the catalog `build_catalog_for_server` returns"
        );
        assert_eq!(
            engine.fold_interval, config.fold.fold_interval,
            "the engine must take the interval of the `FoldTaskConfig` `start` spawns the \
             fold with"
        );
        assert_eq!(
            engine.head_cache_ttl,
            Duration::from_nanos(catalog.config().head_cache_ttl_ns.unsigned_abs()),
            "the engine must take the HEAD cache TTL the resolve path really reads through"
        );

        // The seam. Every value below differs from the reference constant the
        // reverts above would substitute, so each assertion fails on exactly
        // one of them.
        let odd_catalog = CatalogConfig {
            max_flush_lifetime_ns: 1_111_000_000_000,
            clock_skew_allowance_ns: 222_000_000_000,
            fold_safety_margin_ns: 333_000_000_000,
            head_cache_ttl_ns: 77_000_000_000,
            ..server_catalog_config_base()
        };
        let odd_fold_interval = Duration::from_secs(137);
        assert_ne!(
            odd_fold_interval,
            ravel_query::REFERENCE_FOLD_INTERVAL,
            "sanity: the seam's fold interval must differ from the reference constant, or the \
             assertion below could not tell a wired value from a hardcoded one"
        );
        let odd_seal_margin = ravel_query::SealMargin::from_catalog_config(&odd_catalog);
        assert_ne!(odd_seal_margin, ravel_query::SealMargin::REFERENCE);
        let odd_head_cache_ttl = Duration::from_nanos(odd_catalog.head_cache_ttl_ns.unsigned_abs());
        assert_ne!(odd_head_cache_ttl, ravel_query::REFERENCE_HEAD_CACHE_TTL);

        let odd_config = crate::ServerConfig {
            fold: crate::FoldTaskConfig {
                fold_interval: odd_fold_interval,
                ..config.fold
            },
            ..server_config()
        };
        let engine = build_engine_config(&odd_config, &odd_catalog).expect("still valid");
        assert_eq!(
            engine.seal_margin, odd_seal_margin,
            "the seal margin must come from the `CatalogConfig` argument, not from \
             SealMargin::REFERENCE"
        );
        assert_eq!(
            engine.fold_interval, odd_fold_interval,
            "the fold interval must come from the `ServerConfig`'s `FoldTaskConfig`, not from \
             REFERENCE_FOLD_INTERVAL"
        );
        assert_eq!(
            engine.head_cache_ttl, odd_head_cache_ttl,
            "the HEAD cache TTL must come from the `CatalogConfig` argument, not from \
             REFERENCE_HEAD_CACHE_TTL"
        );

        // The threshold those three feed, so the wiring is pinned to the
        // quantity it exists for rather than only to three fields.
        assert_eq!(
            engine.fold_lag_threshold(),
            ravel_query::fold_lag_tail_threshold(
                odd_seal_margin,
                odd_fold_interval,
                odd_head_cache_ttl
            ),
            "the fold-lag refusal threshold must be the one this deployment's fold and \
             catalog imply"
        );
    }

    /// The `ServerConfig` a `--mode query` process builds from `args`, with
    /// its query budgets resolved through the same `Cli` methods `main` calls.
    fn query_mode_config(args: &[&str]) -> crate::ServerConfig {
        let mut argv = vec!["ravel-server", "--mode", "query"];
        argv.extend_from_slice(args);
        let cli = crate::Cli::parse_validated_from(argv).expect("flags parse");
        cli.validate().expect("flags validate");
        let resolved = cli
            .resolve_performance(crate::config::HostProfile::new(
                16,
                Some(32_212_254_720),
                Some(32_212_254_720),
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        crate::ServerConfig {
            mode: crate::Mode::Query,
            fold: crate::FoldTaskConfig {
                fold_interval: Duration::from_secs(cli.fold_interval_secs),
                ..crate::FoldTaskConfig::default()
            },
            query_budgets: cli.query_budgets(&resolved).expect("budgets resolve"),
            ..server_config()
        }
    }

    /// ADR-1306 decision 6, amendment of 2026-10-01: a `--mode query` process
    /// runs no scheduled fold, so the fold interval its refusals classify
    /// against is `--fold-lag-interval-secs`, the maintain tier's interval,
    /// and not the 300 s default of a `--fold-interval-secs` it may not set.
    ///
    /// RED: in [`build_engine_config`], take `fold_interval` from
    /// `config.fold.fold_interval` alone, as before the amendment.
    #[test]
    fn query_mode_engine_classifies_against_the_fold_lag_interval() {
        let config = query_mode_config(&["--fold-lag-interval-secs", "900"]);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 7_200_000_000_000).expect("catalog builds");
        let engine = build_engine_config(&config, catalog.config()).expect("valid");
        assert_eq!(
            engine.fold_interval,
            Duration::from_secs(900),
            "--fold-lag-interval-secs must reach the engine's fold interval"
        );

        // Unset, the classification keeps the default interval.
        let unset = query_mode_config(&[]);
        let engine_unset = build_engine_config(&unset, catalog.config()).expect("valid");
        assert_eq!(
            engine_unset.fold_interval,
            crate::fold::DEFAULT_FOLD_INTERVAL
        );
    }

    /// Only zero is refused: the smallest non-zero interval passes, since the
    /// command line accepts sub-millisecond values such as `500us`.
    ///
    /// Flip to watch it fail: change `i.is_zero()` in [`check_fold_lag_interval`] to
    /// `i < Duration::from_millis(1)`.
    #[test]
    fn check_fold_lag_interval_accepts_the_smallest_nonzero_interval() {
        let budgets = crate::config::QueryBudgets {
            fold_lag_interval: Some(Duration::from_nanos(1)),
            ..crate::config::QueryBudgets::default()
        };
        check_fold_lag_interval(&budgets).expect("a 1 ns interval passes");
    }

    /// A zero fold-lag interval is refused with the typed error naming the
    /// flag; a non-zero one and an unset one pass.
    ///
    /// Flip to watch it fail: delete the `return
    /// Err(FoldLagIntervalError::ZeroFoldLagInterval);` line in
    /// [`check_fold_lag_interval`].
    #[test]
    fn a_zero_fold_lag_interval_is_refused() {
        let mut budgets = crate::config::QueryBudgets {
            fold_lag_interval: Some(Duration::ZERO),
            ..crate::config::QueryBudgets::default()
        };
        let err = check_fold_lag_interval(&budgets)
            .expect_err("a zero fold-lag interval must be refused");
        assert!(
            matches!(err, FoldLagIntervalError::ZeroFoldLagInterval),
            "expected ZeroFoldLagInterval, got: {err}"
        );
        assert!(
            err.to_string().contains("--fold-lag-interval-secs"),
            "the refusal must name the flag, got: {err}"
        );

        budgets.fold_lag_interval = Some(Duration::from_secs(900));
        check_fold_lag_interval(&budgets).expect("a non-zero interval passes");
        budgets.fold_lag_interval = None;
        check_fold_lag_interval(&budgets).expect("an unset interval passes");
    }

    /// The issue #2074 case end to end through the refusal text: a maintain
    /// tier folding every 900 s can leave a 9,000 s tail while keeping up
    /// (threshold 8,400 + 900 + 30 = 9,330 s), so a query tier told that
    /// interval must not blame the fold for it. The same tail against the
    /// 300 s default (threshold 8,730 s) does name fold lag, which is what
    /// keeps the first half from passing vacuously.
    #[test]
    fn a_tail_a_900_s_fold_can_leave_is_not_refused_as_fold_lag() {
        let tail = Duration::from_secs(9_000);
        let refusal = |config: &crate::ServerConfig| {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let catalog =
                build_catalog_for_server(store, config, 7_200_000_000_000).expect("catalog builds");
            let engine = build_engine_config(config, catalog.config()).expect("valid");
            let budget = ravel_query::RequestBudget::new(
                ravel_query::RequestLimit::Bounded(10),
                ravel_query::FoldLag::from_resolved_tail(Some(tail), engine.fold_lag_threshold()),
            );
            ravel_query::request_budget_exceeded(11, budget)
                .expect("11 requests exceed a budget of 10")
                .to_string()
        };

        let with_flag = refusal(&query_mode_config(&["--fold-lag-interval-secs", "900"]));
        assert!(
            !with_flag.contains(ravel_query::FOLD_LAST_SUCCESS_GAUGE)
                && !with_flag.contains("fold is behind"),
            "a 9,000 s tail is inside what a 900 s fold keeping up can leave, so the refusal \
             must not name fold lag, got: {with_flag}"
        );
        assert_eq!(
            with_flag,
            "query issued 11 S3 requests, exceeding the budget of 10"
        );

        let without_flag = refusal(&query_mode_config(&[]));
        assert!(
            without_flag.contains("the catalog's unsealed tail is 9000 s, longer than the 8730 s"),
            "against the 300 s default the same tail names fold lag, got: {without_flag}"
        );
    }

    /// ADR-1306 follow-up task 3's drift guard. `ravel-query` cannot import
    /// `ravel-server` (the dependency runs the other way), so
    /// [`ravel_query::REFERENCE_FOLD_INTERVAL`] is a HAND COPY of this
    /// crate's [`crate::fold::DEFAULT_FOLD_INTERVAL`], and
    /// [`ravel_query::REFERENCE_HEAD_CACHE_TTL`] is the catalog default the
    /// server's own catalog runs on. Both are what an `EngineConfig` built
    /// with no deployment context falls back to, and a deployment that never
    /// moves either flag is classified against exactly them.
    ///
    /// Moving `DEFAULT_FOLD_INTERVAL` here, or the catalog's
    /// `head_cache_ttl_ns` under `server_catalog_config_base`, without moving
    /// the `ravel-query` copy would leave every no-context `EngineConfig`
    /// classifying refusals against a threshold this server does not run on,
    /// and nothing in `ravel-query`'s own tests can see it. This is the test
    /// that can.
    ///
    /// RED: change either constant on one side only.
    #[test]
    fn ravel_query_reference_durations_match_the_servers_running_ones() {
        assert_eq!(
            ravel_query::REFERENCE_FOLD_INTERVAL,
            crate::fold::DEFAULT_FOLD_INTERVAL,
            "ravel-query's hand copy of the fold interval must be this crate's own default"
        );
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog_for_server(store, &server_config(), 7_200_000_000_000)
            .expect("catalog builds");
        assert_eq!(
            ravel_query::REFERENCE_HEAD_CACHE_TTL,
            Duration::from_nanos(catalog.config().head_cache_ttl_ns.unsigned_abs()),
            "ravel-query's reference HEAD cache TTL must be the TTL the catalog the server \
             resolves through really runs on"
        );
    }

    /// Every other input the same mapping carries: the shard count, the
    /// catalog cache ceiling, the flush delay (through the derived record
    /// cache capacity), the cache directory, and `--disable-cache`. A field
    /// crossed with another one, or left at a constant, shows up on the
    /// catalog this returns.
    ///
    /// RED: swap any one of those fields in `build_catalog_for_server` for
    /// the constant `build_catalog` would otherwise see. Passing
    /// `Duration::ZERO` for the flush delay, for instance, changes the
    /// derived capacity and fails the `cache_capacity_per_tenant` assertion.
    ///
    /// It runs on a runtime because the byte cache's disk tier spawns onto
    /// one the moment `--cache-dir` attaches it.
    #[tokio::test]
    async fn build_catalog_for_server_applies_the_other_catalog_inputs() {
        let cache_dir = tempfile::tempdir().expect("temp cache dir");
        let config = crate::ServerConfig {
            shard_count: 3,
            catalog_cache_max_bytes: 64 * 1024 * 1024,
            max_flush_delay: Duration::from_secs(11),
            cache_dir: Some(cache_dir.path().to_path_buf()),
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 9_000_000_000_000).expect("catalog builds");
        assert_eq!(catalog.config().shard_count, 3);
        assert_eq!(catalog.config().byte_cache_max_bytes, 64 * 1024 * 1024);
        assert_eq!(
            catalog.config().cache_capacity_per_tenant,
            ravel_catalog::derive_cache_capacity_per_tenant(3, Duration::from_secs(11)),
            "the record cache capacity derives from this config's shard count and flush delay"
        );
        assert!(
            catalog.byte_cache_disk_metrics().is_some(),
            "--cache-dir reaches the byte cache's disk tier"
        );
        assert_eq!(
            catalog.config().max_ingest_lag_ns,
            9_000_000_000_000,
            "the listening window is the caller's resolved value, not the config default"
        );

        // The other side of the cache flag: with it set, no byte cache is
        // built and the disk tier has nothing to attach to.
        let disabled = crate::ServerConfig {
            disable_cache: true,
            ..config
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &disabled, 9_000_000_000_000).expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            0,
            "--disable-cache reaches the catalog's byte cache"
        );
        assert!(catalog.byte_cache_disk_metrics().is_none());
    }

    /// The resolved column-statistics cap (issue #3244) reaches
    /// `CatalogConfig::column_stats_cache_max_bytes` on the catalog the
    /// server builds, and `--disable-cache` turns it into the `0` sentinel.
    ///
    /// RED: pass `server_catalog_config_base().column_stats_cache_max_bytes`
    /// instead of the parameter in `build_catalog_from` and the first
    /// assertion reads 67,108,864 against 601,295,421; drop the
    /// `disable_cache` branch and the second reads 601,295,421 against 0.
    #[test]
    fn build_catalog_for_server_applies_the_column_stats_cache_cap() {
        let config = crate::ServerConfig {
            query_budgets: crate::config::QueryBudgets {
                column_stats_cache_max_bytes: 601_295_421,
                ..crate::config::QueryBudgets::default()
            },
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 9_000_000_000_000).expect("catalog builds");
        assert_eq!(catalog.config().column_stats_cache_max_bytes, 601_295_421);

        let disabled = crate::ServerConfig {
            disable_cache: true,
            ..config
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &disabled, 9_000_000_000_000).expect("catalog builds");
        assert_eq!(
            catalog.config().column_stats_cache_max_bytes,
            0,
            "--disable-cache reaches the column-statistics cache"
        );
    }

    /// `--catalog-record-cache-max-bytes-per-tenant` (ADR-3513) reaches the
    /// record caches' entry bound on the catalog the server builds. At 2
    /// shards and a 100 ms flush delay the derivation is 1,296,000 entries
    /// uncapped; a 400 MB budget holds it to `400_000_000 / 1_800 = 222_222`,
    /// where the default budget holds it to 25,000. Each cache's byte budget
    /// follows from the bound: `222_222 * 900 = 199_999_800` bytes.
    ///
    /// RED: pass `ravel_catalog::MAX_RECORD_CACHE_BYTES_PER_TENANT` in place of
    /// `record_cache_max_bytes_per_tenant` in `build_catalog_from` and the
    /// first assertion reads 25,000 against 222,222.
    #[test]
    fn build_catalog_for_server_applies_the_record_cache_budget() {
        let config = crate::ServerConfig {
            shard_count: 2,
            max_flush_delay: Duration::from_millis(100),
            query_budgets: crate::config::QueryBudgets {
                record_cache_max_bytes_per_tenant: 400_000_000,
                ..crate::config::QueryBudgets::default()
            },
            ..server_config()
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            build_catalog_for_server(store, &config, 9_000_000_000_000).expect("catalog builds");
        assert_eq!(catalog.config().cache_capacity_per_tenant, 222_222);
        assert_eq!(
            catalog.config().commit_cache_max_bytes_per_tenant(),
            199_999_800
        );
        assert_eq!(
            catalog.config().compaction_cache_max_bytes_per_tenant(),
            199_999_800
        );

        let default_budget = crate::ServerConfig {
            query_budgets: crate::config::QueryBudgets::default(),
            ..config
        };
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog_for_server(store, &default_budget, 9_000_000_000_000)
            .expect("catalog builds");
        assert_eq!(
            catalog.config().cache_capacity_per_tenant,
            ravel_catalog::MAX_CACHE_CAPACITY_PER_TENANT,
            "unset, the budget is 45 MB and the bound is the 25,000-entry cap"
        );
    }
}

#[cfg(all(test, feature = "sql"))]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::http::StaticBearerTokenResolver;
    use std::collections::HashMap;
    use std::time::Duration;

    /// `build_sql_state` must enforce the `EngineConfig` it was given, not an
    /// independent `SqlConfig::default()` -- the gap the ADR-0050 section 4 /
    /// EC4 fix-continuation found: the PromQL path (`build_app_state`) took a
    /// resolved deadline, but SQL/Flight SQL silently kept a hardcoded 30s.
    #[test]
    fn build_sql_state_honors_the_passed_engine_config_deadline() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let tenant_resolver: Arc<dyn TenantResolver> =
            Arc::new(StaticBearerTokenResolver::new(HashMap::new()));
        let non_default = EngineConfig {
            deadline: Duration::from_secs(10),
            ..EngineConfig::default()
        };
        assert_ne!(
            non_default.deadline,
            EngineConfig::default().deadline,
            "sanity: the test deadline must actually differ from the default"
        );

        let state = build_sql_state(
            catalog,
            store,
            tenant_resolver,
            None,
            non_default,
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
        )
        .expect("sql state builds");

        assert_eq!(
            state.max_deadline, non_default.deadline,
            "build_sql_state's max_deadline must be the resolved value passed in, \
             not an independent EngineConfig::default()"
        );
    }

    /// The grace `start` hands the builder is the one the executor's DDL path
    /// uses; a builder that dropped it would leave the 11-minute default.
    #[test]
    fn build_sql_state_with_parquet_installs_the_ddl_min_grace() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let state = build_sql_state_with_parquet(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            EngineConfig::default(),
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            None,
            7_000,
            &SqlSpillInputs::default(),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            false,
            ravel_sql::IpcryptDeployment::default(),
            None,
        )
        .expect("sql state builds");
        assert_eq!(state.executor.ddl_min_grace_ms(), 7_000);
        assert_ne!(7_000, ravel_sql::DEFAULT_MIN_GRACE_MS);
    }

    /// `main`/`lib.rs`'s startup path is `ddl_min_grace_ms(config.gc.
    /// max_query_duration_ns)` fed straight into `build_sql_state_with_parquet`.
    /// This chains the two the same way, from a `max_query_duration_ns` that
    /// converts to a value other than `DEFAULT_MIN_GRACE_MS`, so a regression
    /// at that call site back to the literal default fails here rather than
    /// only at the first `CREATE` a real deployment runs.
    #[test]
    fn the_startup_computed_grace_reaches_the_executor() {
        let max_query_duration_ns: i64 = 3 * 60 * 1_000_000_000; // 3 minutes
        let expected_ms = ddl_min_grace_ms(max_query_duration_ns).expect("grace");
        assert_ne!(
            expected_ms,
            ravel_sql::DEFAULT_MIN_GRACE_MS,
            "sanity: the test value must actually differ from the default"
        );

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let state = build_sql_state_with_parquet(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            EngineConfig::default(),
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            None,
            expected_ms,
            &SqlSpillInputs::default(),
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            false,
            ravel_sql::IpcryptDeployment::default(),
            None,
        )
        .expect("sql state builds");
        assert_eq!(state.executor.ddl_min_grace_ms(), expected_ms);
    }

    #[test]
    fn the_ddl_min_grace_is_sys_gc_max_query_duration_in_milliseconds() {
        assert_eq!(ddl_min_grace_ms(600_000_000_000).expect("grace"), 600_000);
        assert_eq!(
            ddl_min_grace_ms(2_000_000).expect("grace"),
            2,
            "the writer's usable-grace boundary itself must still be accepted"
        );
    }

    /// Any negative value is refused, including one that integer division by a
    /// million would round to zero.
    #[test]
    fn a_negative_sys_gc_max_query_duration_fails_naming_sys_gc() {
        for ns in [-1, -999_999, -1_000_000, i64::MIN] {
            let err = ddl_min_grace_ms(ns).expect_err("negative must refuse startup");
            assert!(err.to_string().contains("sys/gc"), "{ns}: {err}");
        }
    }

    /// `ravel_pqtable::writer::apply` refuses a grace whose half rounds down to
    /// zero with `NoPutBudget`: a value that would convert to such a grace,
    /// 0 included, must refuse startup instead of deferring the failure to the
    /// first `CREATE` the deployment ever runs.
    #[test]
    fn a_grace_below_the_writers_usable_minimum_fails_naming_sys_gc() {
        for ns in [0, 1, 999_999, 1_000_000, 1_999_999] {
            let err =
                ddl_min_grace_ms(ns).expect_err("below the writer's usable minimum must refuse");
            assert!(err.to_string().contains("sys/gc"), "{ns}: {err}");
        }
    }

    /// ADR-0061 decision 1: the SQL/HTTP surface must enforce the same
    /// bytes-scanned budget the PromQL surface does, so the value threaded into
    /// `build_sql_state`'s `EngineConfig` must survive into the executor's
    /// `SqlConfig.engine` (where `RsegScanExec::prepare_partition` checks it),
    /// not be dropped to `SqlConfig::default()`'s `Unlimited`.
    #[test]
    fn build_sql_state_threads_the_byte_budget_into_the_executor() {
        use ravel_query::ByteLimit;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let engine_config = EngineConfig {
            max_bytes_scanned: ByteLimit::Bounded(4096),
            ..EngineConfig::default()
        };
        let state = build_sql_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            engine_config,
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
        )
        .expect("sql state builds");
        assert_eq!(
            state.executor.config().engine.max_bytes_scanned,
            ByteLimit::Bounded(4096),
            "the SQL executor must enforce the configured byte budget, not the default Unlimited"
        );
    }

    /// ADR-2040: `start`'s state builder always installs Parquet sources on the
    /// executor, configured exactly when a profile file was loaded, while the
    /// plain builder installs none. The profiles' stores refuse Ravel's own
    /// bucket (D4): here a GCS profile and a Ravel bucket at GCS's
    /// interoperability endpoint, refused before any credential is looked up.
    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn build_sql_state_with_parquet_installs_the_profiles_it_is_given() {
        let build = |profiles: Option<crate::config::ParquetProfiles>| {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let catalog = build_catalog(
                store.clone(),
                1,
                false,
                ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
                None,
                None,
                None,
                Duration::from_secs(2),
            )
            .expect("catalog");
            build_sql_state_with_parquet(
                catalog,
                store,
                Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
                None,
                EngineConfig::default(),
                Arc::new(GetLimiter::new(1).expect("nonzero permits")),
                ravel_sql::DEFAULT_MAX_QUERY_BYTES,
                DEFAULT_MAX_TENANT_BYTES,
                false,
                false,
                Arc::new(crate::metrics::QueryAccountingMetrics::new(
                    std::collections::HashSet::new(),
                )),
                QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
                None,
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                profiles,
                ravel_sql::DEFAULT_MIN_GRACE_MS,
                &SqlSpillInputs::default(),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                false,
                ravel_sql::IpcryptDeployment::default(),
                None,
            )
            .expect("sql state builds")
        };
        let profiles = ravel_object_store::external::load_profiles(
            r#"[{"name": "lake", "kind": "gcs", "credentials": {"mode": "application_default"}}]"#,
        )
        .expect("profiles");
        // Ravel's bucket as `--store s3` and as `--store gcs` (ADR-3456 D7)
        // reach it: a GCS profile on it is refused either way.
        for ravel_bucket in [
            crate::config::RavelBucket::S3 {
                bucket: "ravel-data".to_string(),
                endpoint: Some("https://storage.googleapis.com".to_string()),
                region: "us-east-1".to_string(),
            },
            crate::config::RavelBucket::Gcs {
                bucket: "ravel-data".to_string(),
                endpoint: None,
            },
        ] {
            let configured = build(Some(crate::config::ParquetProfiles {
                profiles: profiles.clone(),
                ravel_bucket: Some(ravel_bucket.clone()),
            }));
            let sources = configured.executor.parquet_sources().expect("sources");
            assert!(sources.is_configured());
            let refused = sources
                .external_stores()
                .expect("profile stores")
                .store("lake", "ravel-data")
                .err()
                .expect("Ravel's bucket is refused");
            assert!(
                matches!(refused, ravel_sql::ExternalStoreError::RavelBucket { .. }),
                "{ravel_bucket:?}: {refused:?}"
            );
        }
        let unconfigured = build(None);
        assert!(
            !unconfigured
                .executor
                .parquet_sources()
                .expect("sources are installed without profiles too")
                .is_configured()
        );
    }

    /// Build a minimal `SqlState` the way `start` does, sourcing the two SQL
    /// budgets from a parsed CLI's `query_budgets()` so the test traces the same
    /// wiring the running server uses (`Cli::query_budgets` -> `build_sql_state`
    /// args -> `SqlConfig`/`SqlExecutor`).
    fn sql_state_from_cli(argv: &[&str]) -> crate::sql::SqlState {
        use clap::Parser;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let cli = crate::Cli::try_parse_from(argv).expect("flags parse");
        // The injected reference host of issue #1141 (16 cores, 30 GB), never
        // the real one: an unset SQL budget flag is now a share of the host's
        // memory, so a test that read the machine it runs on would assert a
        // different number on every box.
        let resolved = cli
            .resolve_performance(crate::config::HostProfile::new(
                16,
                Some(32_212_254_720),
                Some(32_212_254_720),
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        let budgets = cli.query_budgets(&resolved).expect("budgets resolve");
        build_sql_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            EngineConfig::default(),
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            budgets.sql_max_query_bytes,
            budgets.sql_tenant_max_bytes,
            budgets.sql_parallel_final_aggregation,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
        )
        .expect("sql state builds")
    }

    /// ADR-0088 reachability: `--sql-max-query-bytes` must reach the value the
    /// SQL executor's per-query DataFusion memory pool is built from
    /// (`SqlConfig::max_query_bytes`), not stop at a parsed field. Traced from
    /// the CLI through `query_budgets()` and `build_sql_state` to
    /// `executor.config()`.
    #[test]
    fn sql_max_query_bytes_is_reachable_from_cli() {
        let non_default = 7 * 1024 * 1024;
        assert_ne!(non_default, ravel_sql::DEFAULT_MAX_QUERY_BYTES);
        let state = sql_state_from_cli(&[
            "ravel-server",
            "--sql-max-query-bytes",
            &non_default.to_string(),
        ]);
        assert_eq!(
            state.executor.config().max_query_bytes,
            non_default,
            "the SQL executor's per-query pool ceiling must be the configured flag, not the default"
        );

        // Unset: the HOST-DERIVED per-query pool reaches the executor (issue
        // #1141), 50% of the injected reference host's 30 GB (ADR-2414 B1), not
        // the compiled-in 256 MiB.
        let state = sql_state_from_cli(&["ravel-server"]);
        assert_eq!(
            state.executor.config().max_query_bytes,
            16_106_127_360,
            "an unset --sql-max-query-bytes must reach the executor as the host-derived pool"
        );
        assert_ne!(16_106_127_360, ravel_sql::DEFAULT_MAX_QUERY_BYTES);
    }

    /// ADR-0094 reachability: `--sql-parallel-final-aggregation` must reach the
    /// `SqlConfig::parallel_final_aggregation` the executor's classification
    /// gate reads, not stop at a parsed field. Traced from the CLI through
    /// `query_budgets()` and `build_sql_state` to `executor.config()`. Under the
    /// #741 amendment the default is on, and the `=false` opt-out reaches the
    /// executor as `false`.
    #[test]
    fn sql_parallel_final_aggregation_is_reachable_from_cli() {
        // Bare flag: still accepted, still means on.
        let state = sql_state_from_cli(&["ravel-server", "--sql-parallel-final-aggregation"]);
        assert!(
            state.executor.config().parallel_final_aggregation,
            "the bare flag must carry the executor to on"
        );

        // The opt-out reaches the executor as false.
        let state = sql_state_from_cli(&["ravel-server", "--sql-parallel-final-aggregation=false"]);
        assert!(
            !state.executor.config().parallel_final_aggregation,
            "the =false opt-out must reach the executor as single-partition"
        );

        // Unset: the amended default-on posture (issue #741).
        let state = sql_state_from_cli(&["ravel-server"]);
        assert!(
            state.executor.config().parallel_final_aggregation,
            "default must be on (ADR-0094 amendment, issue #741)"
        );
    }

    /// ADR-3565 decision 1 reachability: `--sql-bucketed-final-aggregation`
    /// must reach the plan, not stop at a parsed field. Traced from the CLI
    /// through `query_budgets()` and `build_sql_state_with_parquet`, the
    /// builder `start` calls, to the physical plan `EXPLAIN` renders for a
    /// two-integer-key aggregate of the q33 shape.
    #[tokio::test]
    async fn sql_bucketed_final_aggregation_is_reachable_from_cli() {
        async fn plan_text(argv: &[&str]) -> String {
            use clap::Parser;
            let tenant = ravel_types::TenantId::new("bucketed-3608".to_string());

            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let catalog = build_catalog(
                store.clone(),
                1,
                false,
                ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
                None,
                None,
                None,
                Duration::from_secs(2),
            )
            .expect("catalog");
            // Data, so the scan is fanned out and the final aggregation is
            // `FinalPartitioned`; an empty tenant plans `SinglePartitioned`.
            let records: Vec<_> = (1..=64)
                .map(|ts| crate::tests::sql_log_record(ts, "row"))
                .collect();
            crate::tests::publish_small_log_segment(store.as_ref(), &tenant, &records).await;
            let cli = crate::Cli::try_parse_from(argv).expect("flags parse");
            let resolved = cli
                .resolve_performance(crate::config::HostProfile::new(
                    16,
                    Some(32_212_254_720),
                    Some(32_212_254_720),
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            let budgets = cli.query_budgets(&resolved).expect("budgets resolve");
            // Four partitions, so the final aggregation is `FinalPartitioned`.
            let engine = EngineConfig {
                sql_partition_count: Some(4),
                ..EngineConfig::default()
            };
            let state = build_sql_state_with_parquet(
                catalog,
                store,
                Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
                None,
                engine,
                Arc::new(GetLimiter::new(1).expect("nonzero permits")),
                budgets.sql_max_query_bytes,
                budgets.sql_tenant_max_bytes,
                budgets.sql_parallel_final_aggregation,
                budgets.sql_bucketed_final_aggregation,
                Arc::new(crate::metrics::QueryAccountingMetrics::new(
                    std::collections::HashSet::new(),
                )),
                QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
                None,
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                None,
                ravel_sql::DEFAULT_MIN_GRACE_MS,
                &SqlSpillInputs::default(),
                crate::cpu_gates::CpuGates::new(Default::default()).read,
                false,
                ravel_sql::IpcryptDeployment::default(),
                None,
            )
            .expect("sql state builds");
            let request = ravel_sql::SqlRequest {
                sql: "SELECT ts, flags, count(*), sum(severity_num), avg(severity_num) \
                      FROM logs GROUP BY ts, flags"
                    .to_string(),
                window: ravel_types::TimeRange {
                    start_ns: 0,
                    end_ns: i64::MAX,
                },
                min_tokens: Vec::new(),
                now_ns: 1_000_000,
                deadline: Duration::from_secs(30),
                row_window: false,
                max_rows: None,
                budgets: None,
            };
            state
                .executor
                .explain(tenant.hash(), &request)
                .await
                .expect("explain runs")
                .plan_text
        }

        let on = plan_text(&["ravel-server", "--sql-bucketed-final-aggregation"]).await;
        assert!(
            on.contains("BucketedFinalAggregateExec"),
            "the bare flag must install the rule:\n{on}"
        );
        let off = plan_text(&["ravel-server"]).await;
        assert!(
            !off.contains("BucketedFinalAggregateExec") && off.contains("FinalPartitioned"),
            "unset must leave the stock final:\n{off}"
        );
        let explicit_off =
            plan_text(&["ravel-server", "--sql-bucketed-final-aggregation=false"]).await;
        assert!(
            !explicit_off.contains("BucketedFinalAggregateExec"),
            "=false must leave the stock final:\n{explicit_off}"
        );
    }

    /// ADR-0088 reachability: `--sql-tenant-max-bytes` must reach the per-tenant
    /// ceiling the executor's per-tenant accountant enforces
    /// (`SqlExecutor::max_tenant_bytes`), not stop at a parsed field.
    #[test]
    fn sql_tenant_max_bytes_is_reachable_from_cli() {
        let non_default = 3 * 1024 * 1024 * 1024;
        assert_ne!(non_default, DEFAULT_MAX_TENANT_BYTES);
        let state = sql_state_from_cli(&[
            "ravel-server",
            "--sql-tenant-max-bytes",
            &non_default.to_string(),
        ]);
        assert_eq!(
            state.executor.max_tenant_bytes(),
            non_default,
            "the SQL executor's per-tenant ceiling must be the configured flag, not the default"
        );

        // Unset: the HOST-DERIVED per-tenant ceiling reaches the executor
        // (issue #1141), 50% of the injected reference host's 30 GB.
        let state = sql_state_from_cli(&["ravel-server"]);
        assert_eq!(
            state.executor.max_tenant_bytes(),
            16_106_127_360,
            "an unset --sql-tenant-max-bytes must reach the executor as the host-derived ceiling"
        );
        assert_ne!(16_106_127_360, DEFAULT_MAX_TENANT_BYTES);
    }

    const GIB: u64 = 1024 * 1024 * 1024;
    const INSTANCE: &str = "inst-self";

    fn settings(off: bool, memory_budget_bytes: u64) -> crate::config::SqlSpillSettings {
        crate::config::SqlSpillSettings {
            off,
            memory_budget_bytes,
            read_cache_bytes: 0,
            merge_fan_in: crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN,
        }
    }

    fn os(value: &str) -> Option<&std::ffi::OsStr> {
        Some(std::ffi::OsStr::new(value))
    }

    /// `prepare_sql_spill_with` with the env pair and the measured free bytes
    /// injected, so no test reads this process's environment or volume.
    fn prepare(
        cache_dir: Option<&std::path::Path>,
        spill: crate::config::SqlSpillSettings,
        env_dir: Option<&std::ffi::OsStr>,
        env_quota: Option<&std::ffi::OsStr>,
        free_bytes: u64,
    ) -> anyhow::Result<SqlSpillStartup> {
        prepare_sql_spill_with(
            cache_dir,
            spill,
            INSTANCE,
            env_dir,
            env_quota,
            no_working_dir,
            |_| SpillFsKind::Other,
            |_| Ok(free_bytes),
        )
    }

    /// The working directory the helpers above inject: unreadable, so a test
    /// that reaches the working-directory default never touches this
    /// process's own working directory.
    fn no_working_dir() -> std::io::Result<PathBuf> {
        Err(std::io::Error::other("no working directory in this test"))
    }

    /// [`prepare`] with a working directory `cwd` and its filesystem type
    /// injected as well.
    fn prepare_in(
        cwd: &std::path::Path,
        fs_kind: SpillFsKind,
        cache_dir: Option<&std::path::Path>,
        spill: crate::config::SqlSpillSettings,
        env_dir: Option<&std::ffi::OsStr>,
        env_quota: Option<&std::ffi::OsStr>,
        free_bytes: u64,
    ) -> anyhow::Result<SqlSpillStartup> {
        prepare_sql_spill_with(
            cache_dir,
            spill,
            INSTANCE,
            env_dir,
            env_quota,
            || Ok(cwd.to_path_buf()),
            |_| fs_kind,
            |_| Ok(free_bytes),
        )
    }

    /// [`prepare_in`] with no `--cache-dir` and no env pair.
    fn prepare_cwd(
        cwd: &std::path::Path,
        spill: crate::config::SqlSpillSettings,
        fs_kind: SpillFsKind,
        free_bytes: u64,
    ) -> anyhow::Result<SqlSpillStartup> {
        prepare_in(cwd, fs_kind, None, spill, None, None, free_bytes)
    }

    /// A sibling spill root whose owner has exited: its lock file exists and
    /// nobody holds it.
    fn dead_sibling(cache_dir: &std::path::Path, id: &str) -> PathBuf {
        let owner = ravel_sql::spill::SpillRootOwner::acquire(cache_dir, id).expect("acquire");
        let dir = owner.dir().to_path_buf();
        drop(owner);
        dir
    }

    /// The process environment must not configure spill for the tests that
    /// build an executor: `SqlConfig::with_spill_resolved` reads it directly.
    fn assert_spill_env_unset() {
        for var in [ravel_sql::ENV_SPILL_DIR, ravel_sql::ENV_SPILL_MAX_BYTES] {
            assert!(
                std::env::var_os(var).is_none(),
                "{var} is set in the test environment; these tests pin the unset case"
            );
        }
    }

    /// Every INFO event, as `message=... name=value...`, the shape
    /// `config::tests::capture_events` records; the second vector holds every
    /// WARN event in the same shape.
    #[derive(Clone, Default)]
    struct InfoCapture(
        Arc<parking_lot::Mutex<Vec<String>>>,
        Arc<parking_lot::Mutex<Vec<String>>>,
    );

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for InfoCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let sink = match *event.metadata().level() {
                tracing::Level::INFO => &self.0,
                tracing::Level::WARN => &self.1,
                _ => return,
            };
            #[derive(Default)]
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value}", field.name());
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
            sink.lock().push(visitor.0);
        }
    }

    fn capture_info() -> (InfoCapture, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::layer::SubscriberExt as _;
        // While exactly one dispatcher is registered, tracing computes a
        // callsite's cached interest from the registering thread's own default,
        // so another test's thread with no subscriber caches "never" for the
        // lines this one captures. A second dispatcher kept alive for the whole
        // process keeps every interest computed across all live dispatchers.
        static KEEP_TWO_DISPATCHERS: std::sync::OnceLock<tracing::Dispatch> =
            std::sync::OnceLock::new();
        KEEP_TWO_DISPATCHERS
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let capture = InfoCapture::default();
        let guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        (capture, guard)
    }

    /// The one `performance default resolved` line for `setting`.
    fn resolved_line(lines: &[String], setting: &str) -> String {
        let key = format!(" setting=\"{setting}\"");
        let matching: Vec<&String> = lines
            .iter()
            .filter(|line| line.contains("performance default resolved") && line.contains(&key))
            .collect();
        assert_eq!(matching.len(), 1, "exactly one {setting} line: {lines:?}");
        matching[0].clone()
    }

    /// The size trigger threshold startup resolves is the flag raised to the
    /// read cache's `max_bytes`, and its one startup line says which applied:
    /// a flag below a 6,184,752,906-byte cache resolves to the cache size with
    /// source `flag-floored-to-cache-max-bytes`; a flag above it, or a flag
    /// with no read cache, resolves to the flag; no flag leaves the trigger
    /// off whatever the cache.
    ///
    /// FLIP: resolving to the flag as given reads the floored case as
    /// 1,000,000,000 with source `flag`.
    #[test]
    fn the_bulk_scan_threshold_is_floored_at_the_cache_size_and_logged_once() {
        const CACHE: u64 = 6_184_752_906;
        for (label, flag, cache_max_bytes, value, source, line) in [
            (
                "flag below the cache",
                Some(1_000_000_000),
                Some(CACHE),
                Some(CACHE),
                BULK_SCAN_SOURCE_CACHE_FLOOR,
                " setting=\"cache_bulk_scan_min_bytes\" value=6184752906 \
                 source=\"flag-floored-to-cache-max-bytes\" flag=1000000000",
            ),
            (
                "flag above the cache",
                Some(8_000_000_000),
                Some(CACHE),
                Some(8_000_000_000),
                BULK_SCAN_SOURCE_FLAG,
                " setting=\"cache_bulk_scan_min_bytes\" value=8000000000 \
                 source=\"flag\" flag=8000000000",
            ),
            (
                "flag with no read cache",
                Some(1_000_000_000),
                None,
                Some(1_000_000_000),
                BULK_SCAN_SOURCE_FLAG,
                " setting=\"cache_bulk_scan_min_bytes\" value=1000000000 \
                 source=\"flag\" flag=1000000000",
            ),
            (
                "no flag",
                None,
                Some(CACHE),
                None,
                BULK_SCAN_SOURCE_UNSET,
                " setting=\"cache_bulk_scan_min_bytes\" value=\"none\" source=\"unset\"",
            ),
        ] {
            let resolved = resolve_cache_bulk_scan_min_bytes(flag, cache_max_bytes);
            assert_eq!(
                resolved,
                ResolvedBulkScanMinBytes {
                    value,
                    flag,
                    source
                },
                "{label}"
            );
            let (capture, guard) = capture_info();
            resolved.emit();
            drop(guard);
            let lines = capture.0.lock().clone();
            let resolved_line = resolved_line(&lines, "cache_bulk_scan_min_bytes");
            assert!(resolved_line.contains(line), "{label}: {resolved_line}");
        }
    }

    /// `--cache-dir` alone derives the ceiling from the free bytes measured at
    /// startup and the memory budget `--sql-spill`'s settings carry: half the
    /// free space, capped at four times the budget, the cap raised to 1 GiB
    /// when it is below that. One case per clause, each pinned to the byte,
    /// through the same
    /// `prepare_sql_spill_with` `start` runs, so the measured figure and the
    /// budget are the ones that reach the derivation.
    ///
    /// Prove-the-test: pass `memory_budget_bytes: u64::MAX` instead of
    /// `settings.memory_budget_bytes` into `CacheDirSpillInputs` and the cap
    /// row reads 53,687,091,200 against 8,589,934,592; pass `free_bytes * 2`
    /// (a figure larger than what is free) and the half row reads
    /// 6,442,450,944 against 3,221,225,472.
    #[test]
    fn cache_dir_alone_derives_the_ceiling_one_case_per_clause() {
        for (label, free_bytes, memory_budget_bytes, expected) in [
            ("half of free binds", 6 * GIB, 2 * GIB, 3 * GIB),
            ("four times the budget binds", 100 * GIB, 2 * GIB, 8 * GIB),
            ("the 1 GiB floor binds", 2 * GIB, GIB / 8, GIB),
        ] {
            let cache = tempfile::tempdir().expect("cache dir");
            let startup = prepare(
                Some(cache.path()),
                settings(false, memory_budget_bytes),
                None,
                None,
                free_bytes,
            )
            .expect("cache-dir spill resolves");
            assert_eq!(
                startup.resolved.config,
                Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: expected,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                "{label}"
            );
            assert_eq!(
                (
                    startup.resolved.dir_source,
                    startup.resolved.max_bytes_source
                ),
                (SQL_SPILL_SOURCE_CACHE_DIR, SQL_SPILL_SOURCE_DERIVED),
                "{label}"
            );
        }
    }

    /// Too little free space for the derived ceiling's 1 GiB floor resolves
    /// spill off, not to a ceiling the volume cannot hold: at 800 MiB and at 0
    /// free both halves read `cache-dir-insufficient-space`, no root is
    /// taken, and the executor gets no spill; at exactly 2 GiB free (half =
    /// 1 GiB) spill is on with a 1 GiB ceiling. A bare
    /// `RAVEL_SQL_SPILL_MAX_BYTES` is the operator's own ceiling and still
    /// enables spill at 0 free.
    ///
    /// Prove-the-test: restore the floor-last clamp in
    /// `ravel_sql::derive_spill_max_bytes` and the 800 MiB row reads a 1 GiB
    /// derived ceiling; map a `None` config to `SQL_SPILL_SOURCE_UNSET` in
    /// `resolve_sql_spill` and the 800 MiB row reads `unset`.
    #[test]
    fn too_little_free_space_resolves_spill_off_with_its_own_source() {
        assert_spill_env_unset();
        for free_bytes in [800 * 1024 * 1024, 0] {
            let cache = tempfile::tempdir().expect("cache dir");
            let startup = prepare(
                Some(cache.path()),
                settings(false, 2 * GIB),
                None,
                None,
                free_bytes,
            )
            .expect("too little space is not a startup error");
            assert_eq!(
                startup.resolved,
                ResolvedSqlSpill {
                    config: None,
                    dir_source: SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
                    max_bytes_source: SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
                    space: Some(SpillSpace {
                        free_bytes,
                        read_cache_bytes: 0,
                    }),
                },
                "free_bytes={free_bytes}"
            );
            assert!(startup.owner.is_none(), "free_bytes={free_bytes}");
            assert_eq!(
                executor_spill(&startup.inputs),
                None,
                "free_bytes={free_bytes}"
            );
        }

        let cache = tempfile::tempdir().expect("cache dir");
        let at_floor = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            None,
            2 * GIB,
        )
        .expect("spill resolves");
        assert_eq!(
            at_floor.resolved,
            ResolvedSqlSpill {
                config: Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: GIB,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                dir_source: SQL_SPILL_SOURCE_CACHE_DIR,
                max_bytes_source: SQL_SPILL_SOURCE_DERIVED,
                space: Some(SpillSpace {
                    free_bytes: 2 * GIB,
                    read_cache_bytes: 0,
                }),
            }
        );
        drop(at_floor);

        let cache = tempfile::tempdir().expect("cache dir");
        let quota = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            os("777"),
            0,
        )
        .expect("spill resolves");
        assert_eq!(
            quota.resolved.config.map(|config| config.max_bytes),
            Some(777)
        );
    }

    /// The production measurement is the volume's FREE space, not its size:
    /// with no budget cap in play, the derived ceiling is at most half of
    /// what `ravel_sql::measure_free_bytes` reports for `<cache-dir>/sql-spill`
    /// afterwards, plus 4 GiB for whatever another process writes to the
    /// volume in between. The bound is one-sided, so space freed in between
    /// only widens it; it fails only if more than 4 GiB is written between two
    /// readings a few milliseconds apart. The exact arithmetic is pinned by
    /// the injected cases above.
    ///
    /// Prove-the-test: pass a measurement of the volume's size to
    /// `prepare_sql_spill_with` in `prepare_sql_spill`, and on a volume with
    /// more than 8 GiB in use the ceiling exceeds the bound.
    #[test]
    fn the_derived_ceiling_is_half_the_measured_free_space() {
        assert_spill_env_unset();
        let cache = tempfile::tempdir().expect("cache dir");
        let startup = prepare_sql_spill(Some(cache.path()), settings(false, u64::MAX), INSTANCE)
            .expect("cache-dir spill resolves");
        let free_after = ravel_sql::measure_free_bytes(&cache.path().join("sql-spill"))
            .expect("free space is measurable");
        let ceiling = startup
            .resolved
            .config
            .expect("cache-dir spill is enabled")
            .max_bytes;
        let bound = ((free_after + 4 * GIB) / 2).max(GIB);
        assert!(
            ceiling <= bound,
            "ceiling {ceiling} must be half the free bytes, at most {bound}"
        );
        assert!(ceiling >= GIB, "the 1 GiB floor holds");
    }

    /// The source precedence: the full env pair wins over `--cache-dir`;
    /// `RAVEL_SQL_SPILL_MAX_BYTES` alone keeps the cache-dir root under the
    /// env ceiling; `--sql-spill off` wins over both; with neither source
    /// spill falls to the working-directory default; a half-set pair that no cache dir completes is an error naming
    /// the missing variable. What each row sweeps is pinned by
    /// `an_env_spill_dir_start_sweeps_the_cache_dir_roots` and
    /// `sql_spill_off_touches_nothing`.
    ///
    /// Prove-the-test: move the `inputs.off` early return in
    /// `resolve_sql_spill` below the `SpillConfig::resolve` call and keep
    /// the env result, and the off row reads the env pair's directory; map
    /// `SpillConfigError::Incomplete` through unchanged and the half-set rows
    /// read "only one of the two is set", naming neither variable.
    #[test]
    fn spill_sources_resolve_in_precedence_order() {
        let cache = tempfile::tempdir().expect("cache dir");
        let explicit = cache.path().join("explicit");

        let env = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            os(explicit.to_str().expect("utf-8 temp path")),
            os("4096"),
            100 * GIB,
        )
        .expect("the env pair resolves");
        assert_eq!(
            env.resolved,
            ResolvedSqlSpill {
                config: Some(ravel_sql::SpillConfig {
                    dir: explicit.clone(),
                    max_bytes: 4096,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                dir_source: SQL_SPILL_SOURCE_ENV,
                max_bytes_source: SQL_SPILL_SOURCE_ENV,
                space: None,
            }
        );
        assert!(env.inputs.cache_dir.is_none() && env.owner.is_none());

        let off = prepare(
            Some(cache.path()),
            settings(true, 2 * GIB),
            os(explicit.to_str().expect("utf-8 temp path")),
            os("4096"),
            100 * GIB,
        )
        .expect("off never errors");
        assert_eq!(
            off.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_FLAG_OFF,
                max_bytes_source: SQL_SPILL_SOURCE_FLAG_OFF,
                space: None,
            }
        );
        assert!(off.inputs.off && off.inputs.cache_dir.is_none() && off.owner.is_none());

        let half_set_off = prepare(Some(cache.path()), settings(true, GIB), None, os("x"), GIB)
            .expect("off ignores even an unparseable env half");
        assert_eq!(half_set_off.resolved.config, None);

        let cwd = tempfile::tempdir().expect("working dir");
        let neither = prepare_cwd(
            cwd.path(),
            settings(false, 2 * GIB),
            SpillFsKind::Other,
            100 * GIB,
        )
        .expect("no source resolves");
        assert_eq!(
            neither.resolved,
            ResolvedSqlSpill {
                config: Some(ravel_sql::SpillConfig {
                    dir: cwd.path().join("sql-spill").join(INSTANCE),
                    max_bytes: 8 * GIB,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                dir_source: SQL_SPILL_SOURCE_CWD_DEFAULT,
                max_bytes_source: SQL_SPILL_SOURCE_CWD_DEFAULT,
                space: Some(SpillSpace {
                    free_bytes: 100 * GIB,
                    read_cache_bytes: 0,
                }),
            }
        );
        drop(neither);

        let quota_alone = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            os("777"),
            100 * GIB,
        )
        .expect("a dir-less quota under --cache-dir resolves");
        assert_eq!(
            quota_alone.resolved,
            ResolvedSqlSpill {
                config: Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: 777,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                dir_source: SQL_SPILL_SOURCE_CACHE_DIR,
                max_bytes_source: SQL_SPILL_SOURCE_ENV_OVERRIDE,
                space: None,
            }
        );
        drop(quota_alone);

        for cache_dir in [Some(cache.path()), None] {
            let err = prepare(
                cache_dir,
                settings(false, 2 * GIB),
                os("/explicit"),
                None,
                100 * GIB,
            )
            .err()
            .expect("a directory with no quota is refused");
            assert!(
                err.to_string()
                    .starts_with("RAVEL_SQL_SPILL_MAX_BYTES is not set but RAVEL_SQL_SPILL_DIR is"),
                "{err}"
            );
        }
        let err = prepare(None, settings(false, 2 * GIB), None, os("777"), 100 * GIB)
            .err()
            .expect("a quota with no directory and no --cache-dir is refused");
        assert!(
            err.to_string()
                .starts_with("RAVEL_SQL_SPILL_DIR is not set but RAVEL_SQL_SPILL_MAX_BYTES is"),
            "{err}"
        );
    }

    /// `--sql-spill` and `--cache-dir` reach the `SqlConfig` the server's SQL
    /// executor runs with, traced from the parsed command line through
    /// `query_budgets`, `prepare_sql_spill_with`, and
    /// `build_sql_state_with_parquet`. On the injected reference host
    /// (`memory_budget_bytes` 30,064,771,072) with 300 GiB free, the cap binds:
    /// 120,259,084,288. With 40 GiB free, the read cache's disk-tier bound
    /// (7,516,192,768 fetcher plus 1,503,238,553 catalog) comes off first:
    /// (42,949,672,960 - 9,019,431,321) / 2 = 16,965,120,819, and with 200 GiB
    /// free, the configuration guide's example, 102,864,466,739.
    ///
    /// The builder honours `off` itself, not only through `prepare` leaving
    /// the cache inputs out: inputs carrying both `off` and a cache dir build
    /// an executor with spill disabled.
    ///
    /// Prove-the-test: pass `None` instead of the cache inputs to
    /// `.with_spill_resolved(...)` in `build_sql_state_inner` and the auto row
    /// reads `None`; pass `false` instead of `spill.off` and the
    /// direct off row reads the cache-dir config; subtract only the fetcher
    /// cache's bound and the 40 GiB row reads 17,716,740,096.
    #[test]
    fn sql_spill_and_cache_dir_reach_the_executor_from_cli() {
        use clap::Parser;

        assert_spill_env_unset();
        let state_with_free = |args: &[&str], cache_dir: &std::path::Path, free_bytes: u64| {
            let mut argv = vec!["ravel-server", "--cache-dir"];
            let cache_arg = cache_dir.to_str().expect("utf-8 temp path");
            argv.push(cache_arg);
            argv.extend_from_slice(args);
            let cli = crate::Cli::try_parse_from(argv).expect("flags parse");
            let resolved = cli
                .resolve_performance(crate::config::HostProfile::new(
                    16,
                    Some(32_212_254_720),
                    Some(32_212_254_720),
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            let budgets = cli.query_budgets(&resolved).expect("budgets resolve");
            let startup = prepare(
                cli.cache_dir.as_deref(),
                budgets.sql_spill,
                None,
                None,
                free_bytes,
            )
            .expect("spill resolves");
            executor_spill(&startup.inputs)
        };
        let state_for = |args: &[&str], cache_dir: &std::path::Path| {
            state_with_free(args, cache_dir, 300 * GIB)
        };

        let cache = tempfile::tempdir().expect("cache dir");
        assert_eq!(
            state_for(&[], cache.path()),
            Some(ravel_sql::SpillConfig {
                dir: cache.path().join("sql-spill").join(INSTANCE),
                max_bytes: 120_259_084_288,
                merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
            })
        );
        for (free_bytes, expected) in [(40 * GIB, 16_965_120_819), (200 * GIB, 102_864_466_739)] {
            let cache = tempfile::tempdir().expect("cache dir");
            assert_eq!(
                state_with_free(&[], cache.path(), free_bytes),
                Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: expected,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                "free_bytes={free_bytes}"
            );
        }
        let cache = tempfile::tempdir().expect("cache dir");
        assert_eq!(state_for(&["--sql-spill", "off"], cache.path()), None);
        assert_eq!(
            executor_spill(&SqlSpillInputs {
                off: true,
                cache_dir: Some(CacheDirSpillInputs {
                    cache_dir: cache.path().to_path_buf(),
                    instance_id: INSTANCE.to_string(),
                    free_bytes: 300 * GIB,
                    read_cache_bytes: 9_019_431_321,
                    memory_budget_bytes: 30_064_771_072,
                }),
                cwd_default: false,
                merge_fan_in: crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN,
            }),
            None,
            "off in the inputs wins over a cache dir in the same inputs"
        );
    }

    /// `--sql-spill-merge-fan-in` (ADR-0954 item 2, issue #3899) reaches the
    /// disk manager of the sessions the server's SQL executor builds, traced
    /// from the parsed command line through `query_budgets`,
    /// `prepare_sql_spill_with`, `build_sql_state_with_parquet` and
    /// `ravel_sql::build_session`: unset is the named default, 4 is 4, and 0
    /// is DataFusion's unbounded fan-in.
    ///
    /// Prove-the-test: drop the `merge_fan_in` override after
    /// `.with_spill_resolved(...)` in `build_sql_state_inner` and the 4 and 0
    /// rows read the default.
    #[test]
    fn sql_spill_merge_fan_in_reaches_the_disk_manager_from_cli() {
        use clap::Parser;

        assert_spill_env_unset();
        for (args, expected) in [
            (&[][..], crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN),
            (&["--sql-spill-merge-fan-in", "4"][..], 4),
            (&["--sql-spill-merge-fan-in", "0"][..], 0),
        ] {
            let cache = tempfile::tempdir().expect("cache dir");
            let mut argv = vec!["ravel-server", "--cache-dir"];
            argv.push(cache.path().to_str().expect("utf-8 temp path"));
            argv.extend_from_slice(args);
            let cli = crate::Cli::try_parse_from(argv).expect("flags parse");
            let resolved = cli
                .resolve_performance(crate::config::HostProfile::new(
                    16,
                    Some(32_212_254_720),
                    Some(32_212_254_720),
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            let budgets = cli.query_budgets(&resolved).expect("budgets resolve");
            let startup = prepare(
                cli.cache_dir.as_deref(),
                budgets.sql_spill,
                None,
                None,
                300 * GIB,
            )
            .expect("spill resolves");
            assert_eq!(
                startup
                    .resolved
                    .config
                    .as_ref()
                    .map(|config| config.merge_fan_in),
                Some(expected),
                "{args:?}: the startup line's value"
            );
            let config = executor_config(&startup.inputs);
            assert_eq!(
                config.spill.as_ref().map(|spill| spill.merge_fan_in),
                Some(expected),
                "{args:?}"
            );
            assert_eq!(session_merge_fan_in(&config), expected, "{args:?}");
        }
    }

    /// The `SqlConfig::spill` of an executor `build_sql_state_with_parquet`
    /// builds from `inputs`.
    fn executor_spill(inputs: &SqlSpillInputs) -> Option<ravel_sql::SpillConfig> {
        executor_config(inputs).spill
    }

    /// The spill merge fan-in on the disk manager of a spill-enabled session
    /// `ravel_sql::build_session` builds from `config`, the path every SQL
    /// statement's session takes.
    fn session_merge_fan_in(config: &ravel_sql::SqlConfig) -> usize {
        let spill = config.spill.as_ref().expect("spill resolved");
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantHash([0u8; 16]);
        let table = ravel_sql::SessionTable::Metrics(Arc::new(ravel_sql::RavelTableProvider::new(
            ravel_catalog::Snapshot {
                segments: Vec::new(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            tenant,
            ravel_query::SegmentFetcher::new(store),
            config.clone(),
            ravel_query::PhaseAccounting::new(),
        )));
        let (pool, _breach) = config.query_pool(
            ravel_sql::TenantMemoryAccountant::new(1 << 30),
            ravel_types::accounting::QueryAccounting::new(),
        );
        ravel_sql::build_session(
            config,
            pool,
            table,
            false,
            ravel_sql::SpillDecision::Enabled {
                dir: &spill.dir,
                max_bytes: spill.max_bytes,
            },
            &ravel_sql::SessionPrincipal::without_capabilities(tenant),
        )
        .expect("session builds")
        .runtime_env()
        .disk_manager
        .max_spill_merge_fan_in()
    }

    /// The `SqlConfig` of an executor `build_sql_state_with_parquet` builds
    /// from `inputs`.
    fn executor_config(inputs: &SqlSpillInputs) -> ravel_sql::SqlConfig {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
            None,
            None,
            None,
            Duration::from_secs(2),
        )
        .expect("catalog");
        let state = build_sql_state_with_parquet(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            EngineConfig::default(),
            Arc::new(GetLimiter::new(1).expect("nonzero permits")),
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            None,
            ravel_sql::DEFAULT_MIN_GRACE_MS,
            inputs,
            crate::cpu_gates::CpuGates::new(Default::default()).read,
            false,
            ravel_sql::IpcryptDeployment::default(),
            None,
        )
        .expect("sql state builds");
        state.executor.config().clone()
    }

    /// The startup lines: `sql_spill_dir`, `sql_spill_max_bytes`,
    /// `sql_spill_compression` and `sql_spill_merge_fan_in` each appear
    /// exactly once, on the `performance default resolved` layout, with the
    /// value and source of the cache-dir, env, flag fan-in, off and
    /// insufficient-space cases. The compression line reads
    /// `value="lz4_frame" source="built-in"` with spill on, and the spill
    /// directory line's `value="none"` and source with it off.
    ///
    /// Prove-the-test: label the cache-dir ceiling `SQL_SPILL_SOURCE_ENV` in
    /// `resolve_sql_spill` and the cache-dir row reads `source="env"`; drop
    /// the `None` arm's lines in `ResolvedSqlSpill::emit` and the off row
    /// finds no line; drop the insufficient-space arm in `resolve_sql_spill`
    /// and that row reads `source="unset"`; drop the `merge_fan_in`
    /// assignment in `resolve_sql_spill` and the flag fan-in row reads
    /// `value=2 source="default"`.
    #[test]
    fn startup_lines_carry_the_spill_dir_and_ceiling_with_their_sources() {
        let cache = tempfile::tempdir().expect("cache dir");
        let explicit = cache.path().join("explicit");
        let root = cache.path().join("sql-spill").join(INSTANCE);
        let default_fan_in = crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN;
        let cases = [
            (
                "cache-dir",
                false,
                default_fan_in,
                None,
                None,
                100 * GIB,
                format!(
                    " value={:?} source=\"cache-dir\"",
                    root.display().to_string()
                ),
                format!(" value={} source=\"derived\"", 8 * GIB),
                " value=2 source=\"default\"".to_string(),
            ),
            (
                "env",
                false,
                default_fan_in,
                os(explicit.to_str().expect("utf-8 temp path")),
                os("4096"),
                100 * GIB,
                format!(" value={:?} source=\"env\"", explicit.display().to_string()),
                " value=4096 source=\"env\"".to_string(),
                " value=2 source=\"default\"".to_string(),
            ),
            (
                "flag fan-in",
                false,
                4,
                os(explicit.to_str().expect("utf-8 temp path")),
                os("4096"),
                100 * GIB,
                format!(" value={:?} source=\"env\"", explicit.display().to_string()),
                " value=4096 source=\"env\"".to_string(),
                " value=4 source=\"flag\"".to_string(),
            ),
            (
                "off",
                true,
                default_fan_in,
                os(explicit.to_str().expect("utf-8 temp path")),
                os("4096"),
                100 * GIB,
                " value=\"none\" source=\"flag-off\"".to_string(),
                " value=\"none\" source=\"flag-off\"".to_string(),
                " value=\"none\" source=\"flag-off\"".to_string(),
            ),
            (
                "insufficient space",
                false,
                default_fan_in,
                None,
                None,
                GIB,
                " value=\"none\" source=\"cache-dir-insufficient-space\"".to_string(),
                " value=\"none\" source=\"cache-dir-insufficient-space\"".to_string(),
                " value=\"none\" source=\"cache-dir-insufficient-space\"".to_string(),
            ),
        ];
        for (
            label,
            off,
            merge_fan_in,
            env_dir,
            env_quota,
            free_bytes,
            dir_fields,
            max_fields,
            fan_in_fields,
        ) in cases
        {
            let (capture, guard) = capture_info();
            let startup = prepare(
                Some(cache.path()),
                crate::config::SqlSpillSettings {
                    merge_fan_in,
                    ..settings(off, 2 * GIB)
                },
                env_dir,
                env_quota,
                free_bytes,
            )
            .expect("spill resolves");
            drop(guard);
            drop(startup);
            let lines = capture.0.lock().clone();
            let dir_line = resolved_line(&lines, "sql_spill_dir");
            assert!(dir_line.contains(&dir_fields), "{label}: {dir_line}");
            let max_line = resolved_line(&lines, "sql_spill_max_bytes");
            assert!(max_line.contains(&max_fields), "{label}: {max_line}");
            let compression_fields = if dir_fields.starts_with(" value=\"none\"") {
                dir_fields
            } else {
                " value=\"lz4_frame\" source=\"built-in\"".to_string()
            };
            let compression_line = resolved_line(&lines, "sql_spill_compression");
            assert!(
                compression_line.contains(&compression_fields),
                "{label}: {compression_line}"
            );
            let fan_in_line = resolved_line(&lines, "sql_spill_merge_fan_in");
            assert!(
                fan_in_line.contains(&fan_in_fields),
                "{label}: {fan_in_line}"
            );
        }
    }

    /// The ceiling is derived from the measured free bytes less the read
    /// cache's disk-tier bound, and the `sql_spill_max_bytes` line records
    /// both: 10 GiB free under a 9 GiB bound leaves 1 GiB, half of which is
    /// below the floor, so spill is off; 40 GiB free under the same bound
    /// derives half of 31 GiB, 16,642,998,272. A ceiling the env sets records
    /// neither figure.
    ///
    /// Prove-the-test: pass `cache_dir.free_bytes` instead of
    /// `cache_dir.spillable_free_bytes()` in `ravel_sql::SpillConfig::resolve`
    /// and the 10 GiB row reads `value=5368709120 source="derived"`; leave
    /// `read_cache_bytes` off the `CacheDirSpillInputs` `prepare_sql_spill_with`
    /// builds (0) and the 10 GiB row reads the same value with
    /// `read_cache_bytes=0`.
    #[test]
    fn the_derived_ceiling_subtracts_the_read_cache_bound_and_logs_both() {
        const BOUND: u64 = 9 * GIB;
        let cache = tempfile::tempdir().expect("cache dir");
        let explicit = cache.path().join("explicit");
        let cases = [
            (
                "10 GiB free",
                None,
                10 * GIB,
                " value=\"none\" source=\"cache-dir-insufficient-space\"".to_string(),
                Some(10 * GIB),
            ),
            (
                "40 GiB free",
                None,
                40 * GIB,
                " value=16642998272 source=\"derived\"".to_string(),
                Some(40 * GIB),
            ),
            (
                "env pair",
                os(explicit.to_str().expect("utf-8 temp path")),
                40 * GIB,
                " value=4096 source=\"env\"".to_string(),
                None,
            ),
        ];
        for (label, env_dir, free_bytes, max_fields, recorded_free) in cases {
            let (capture, guard) = capture_info();
            let startup = prepare(
                Some(cache.path()),
                crate::config::SqlSpillSettings {
                    off: false,
                    memory_budget_bytes: u64::MAX,
                    read_cache_bytes: BOUND,
                    merge_fan_in: crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN,
                },
                env_dir,
                env_dir.map(|_| std::ffi::OsStr::new("4096")),
                free_bytes,
            )
            .expect("spill resolves");
            drop(guard);
            drop(startup);
            let lines = capture.0.lock().clone();
            let max_line = resolved_line(&lines, "sql_spill_max_bytes");
            assert!(max_line.contains(&max_fields), "{label}: {max_line}");
            match recorded_free {
                Some(free) => {
                    assert!(
                        max_line.contains(&format!(" free_bytes={free}"))
                            && max_line.contains(&format!(" read_cache_bytes={BOUND}")),
                        "{label}: {max_line}"
                    );
                }
                None => assert!(
                    !max_line.contains("free_bytes") && !max_line.contains("read_cache_bytes"),
                    "{label}: {max_line}"
                ),
            }
        }
    }

    /// Requirement 7's startup sweep, as `start` runs it: of two sibling roots
    /// under one cache dir, the one whose owner lock a live holder has (taken
    /// here, and made a day old) survives and is logged at INFO with its path,
    /// and the one whose lock nobody holds (made just now) is removed. This
    /// process's own root survives and stays locked.
    ///
    /// Prove-the-test: replace `sweep_orphaned_spill_roots_under(cache_dir)`
    /// in `sweep_spill_roots` with a removal of every sibling older than an
    /// hour and the live root is gone; with a removal of every root the live
    /// root is gone.
    #[test]
    fn startup_sweep_removes_only_roots_whose_owner_lock_is_free() {
        let cache = tempfile::tempdir().expect("cache dir");
        let live =
            ravel_sql::spill::SpillRootOwner::acquire(cache.path(), "inst-live").expect("acquire");
        let day_ago = std::time::SystemTime::now() - Duration::from_secs(24 * 60 * 60);
        std::fs::File::open(live.dir())
            .expect("open the live root")
            .set_modified(day_ago)
            .expect("age the live root");
        let dead = dead_sibling(cache.path(), "inst-dead");

        let (capture, guard) = capture_info();
        let startup = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            None,
            100 * GIB,
        )
        .expect("spill resolves");
        drop(guard);

        assert!(live.dir().is_dir(), "a root with a live owner must survive");
        assert!(!dead.exists(), "a root whose owner is gone must be removed");
        assert_eq!(
            startup.left_in_place,
            vec![LeftSpillRoot {
                dir: live.dir().to_path_buf(),
                reason: SPILL_ROOT_LEFT_OWNED,
            }]
        );
        let own = startup.owner.as_ref().expect("this process owns its root");
        assert_eq!(own.dir(), cache.path().join("sql-spill").join(INSTANCE));
        assert!(own.dir().is_dir());
        assert!(
            ravel_sql::spill::SpillRootOwner::acquire(cache.path(), INSTANCE).is_err(),
            "the startup owner must still hold its own root's lock"
        );
        let lines = capture.0.lock().clone();
        let path = format!(" dir={}", live.dir().display());
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.contains("left in place") && line.contains(&path))
                .count(),
            1,
            "the live root is logged once with its path: {lines:?}"
        );
    }

    /// Startup refuses, naming the path, when its own root's lock is already
    /// held. The sweep that precedes the refusal leaves that held root in
    /// place and removes only the root whose lock it took.
    ///
    /// Prove-the-test: make the `SpillRootOwner::acquire` failure in
    /// `prepare_sql_spill_with` non-fatal and startup returns `Ok`; drop the
    /// path from its context and the refusal no longer names the root.
    #[test]
    fn startup_refuses_when_its_own_spill_root_is_locked() {
        let cache = tempfile::tempdir().expect("cache dir");
        let _holder =
            ravel_sql::spill::SpillRootOwner::acquire(cache.path(), INSTANCE).expect("acquire");
        let dead = dead_sibling(cache.path(), "inst-dead");

        let err = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            None,
            100 * GIB,
        )
        .err()
        .expect("a held own root refuses startup");
        let own = cache.path().join("sql-spill").join(INSTANCE);
        assert!(
            err.to_string().contains(&own.display().to_string()),
            "the refusal names the root: {err}"
        );
        assert!(own.is_dir(), "the sweep leaves a root whose lock is held");
        assert!(
            !dead.exists(),
            "the sweep removes a root whose lock it took"
        );
    }

    /// Startup with `--cache-dir` set, `--sql-spill` not off, and
    /// `<cache-dir>/sql-spill` present, injecting a measurement that reads
    /// `free_bytes(orphan exists)` and records each call's view of `orphan`.
    fn prepare_measuring(
        cache_dir: &std::path::Path,
        env_dir: Option<&std::ffi::OsStr>,
        env_quota: Option<&std::ffi::OsStr>,
        orphan: &std::path::Path,
        free_bytes: impl Fn(bool) -> u64,
        seen: &mut Vec<bool>,
    ) -> anyhow::Result<SqlSpillStartup> {
        prepare_sql_spill_with(
            Some(cache_dir),
            settings(false, u64::MAX),
            INSTANCE,
            env_dir,
            env_quota,
            no_working_dir,
            |_| SpillFsKind::Other,
            |_| {
                let present = orphan.exists();
                seen.push(present);
                Ok(free_bytes(present))
            },
        )
    }

    /// A volume under the derived ceiling's floor only because of an orphan
    /// root resolves spill on at this start: the sweep removes the orphan
    /// before the free space is measured. The injected measurement reads
    /// 1 GiB free while the orphan exists (half is below the floor) and
    /// 10 GiB once it is gone, whose half is the 5 GiB ceiling asserted.
    ///
    /// Prove-the-test: move the `sweep_spill_roots` call in
    /// `prepare_sql_spill_with` below the measurement and the resolution
    /// reads `cache-dir-insufficient-space` with the measurement seeing the
    /// orphan; sweep only once a config resolved under the cache dir and the
    /// orphan survives.
    #[test]
    fn an_orphan_root_is_swept_before_free_space_is_measured() {
        let cache = tempfile::tempdir().expect("cache dir");
        let orphan = dead_sibling(cache.path(), "inst-dead");
        let mut seen = Vec::new();
        let startup = prepare_measuring(
            cache.path(),
            None,
            None,
            &orphan,
            |present| if present { GIB } else { 10 * GIB },
            &mut seen,
        )
        .expect("spill resolves");

        assert!(!orphan.exists(), "the orphan root must be swept");
        assert_eq!(
            seen,
            vec![false],
            "measured once, after the orphan was removed"
        );
        assert_eq!(
            startup.resolved,
            ResolvedSqlSpill {
                config: Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: 5 * GIB,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                dir_source: SQL_SPILL_SOURCE_CACHE_DIR,
                max_bytes_source: SQL_SPILL_SOURCE_DERIVED,
                space: Some(SpillSpace {
                    free_bytes: 10 * GIB,
                    read_cache_bytes: 0,
                }),
            }
        );
        assert!(startup.owner.is_some(), "this process owns its root");
    }

    /// A start that resolves spill off for lack of space still sweeps: the
    /// orphan goes, a root whose owner lock this test holds stays and is
    /// reported, and no root of this process's own is taken.
    ///
    /// Prove-the-test: return early from `prepare_sql_spill_with`'s sweep
    /// when the volume is under the floor and the orphan survives; replace
    /// `sweep_orphaned_spill_roots_under` with a removal of every root and
    /// the held root is gone.
    #[test]
    fn an_insufficient_space_start_still_sweeps_orphans() {
        let cache = tempfile::tempdir().expect("cache dir");
        let orphan = dead_sibling(cache.path(), "inst-dead");
        let held =
            ravel_sql::spill::SpillRootOwner::acquire(cache.path(), "inst-held").expect("acquire");
        let mut seen = Vec::new();
        let startup = prepare_measuring(cache.path(), None, None, &orphan, |_| GIB, &mut seen)
            .expect("too little space is not a startup error");

        assert!(!orphan.exists(), "the orphan root must be swept");
        assert!(held.dir().is_dir(), "a held root must survive the sweep");
        assert_eq!(
            startup.left_in_place,
            vec![LeftSpillRoot {
                dir: held.dir().to_path_buf(),
                reason: SPILL_ROOT_LEFT_OWNED,
            }]
        );
        assert_eq!(
            startup.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
                max_bytes_source: SQL_SPILL_SOURCE_INSUFFICIENT_SPACE,
                space: Some(SpillSpace {
                    free_bytes: GIB,
                    read_cache_bytes: 0,
                }),
            }
        );
        assert!(startup.owner.is_none());
        let mut left: Vec<_> = std::fs::read_dir(cache.path().join("sql-spill"))
            .expect("list the spill parent")
            .map(|entry| entry.expect("spill parent entry").file_name())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec![std::ffi::OsString::from("inst-held")],
            "no root of this process's own is created"
        );
        assert_eq!(seen, vec![false]);
    }

    /// An env pair resolves spill outside `--cache-dir`, and the roots an
    /// earlier run left under `--cache-dir` are still swept; a cache dir with
    /// no `sql-spill` gains none.
    ///
    /// Prove-the-test: sweep only when the resolved directory is the
    /// cache-dir root and the orphan survives; create `<cache-dir>/sql-spill`
    /// in `sweep_spill_roots` before its existence check and the empty cache
    /// dir gains it.
    #[test]
    fn an_env_spill_dir_start_sweeps_the_cache_dir_roots() {
        let cache = tempfile::tempdir().expect("cache dir");
        let elsewhere = tempfile::tempdir().expect("env spill dir");
        let orphan = dead_sibling(cache.path(), "inst-dead");
        let mut seen = Vec::new();
        let startup = prepare_measuring(
            cache.path(),
            os(elsewhere.path().to_str().expect("utf-8 temp path")),
            os("4096"),
            &orphan,
            |_| 100 * GIB,
            &mut seen,
        )
        .expect("the env pair resolves");

        assert!(
            !orphan.exists(),
            "the cache dir's orphan root must be swept"
        );
        assert!(
            cache.path().join("sql-spill").is_dir(),
            "the sweep removes roots, not the directory that holds them"
        );
        assert_eq!(
            (
                startup.resolved.dir_source,
                startup.resolved.max_bytes_source
            ),
            (SQL_SPILL_SOURCE_ENV, SQL_SPILL_SOURCE_ENV)
        );
        assert!(startup.owner.is_none());
        assert!(seen.is_empty(), "an env-rooted spill measures nothing");

        let empty = tempfile::tempdir().expect("cache dir");
        prepare(
            Some(empty.path()),
            settings(false, 2 * GIB),
            os(elsewhere.path().to_str().expect("utf-8 temp path")),
            os("4096"),
            100 * GIB,
        )
        .expect("the env pair resolves");
        assert!(
            !empty.path().join("sql-spill").exists(),
            "the sweep creates no spill root under --cache-dir"
        );
    }

    /// `--sql-spill off` leaves an orphan root under `--cache-dir` in place,
    /// and on a cache dir without `sql-spill` creates none.
    ///
    /// Prove-the-test: drop the `!settings.off` guard on the sweep in
    /// `prepare_sql_spill_with` and the orphan is gone; drop the same guard on
    /// the creation of `<cache-dir>/sql-spill` and the empty cache dir gains
    /// it.
    #[test]
    fn sql_spill_off_touches_nothing() {
        let cache = tempfile::tempdir().expect("cache dir");
        let orphan = dead_sibling(cache.path(), "inst-dead");
        let off = prepare(Some(cache.path()), settings(true, 2 * GIB), None, None, 0)
            .expect("off never errors");
        assert!(off.resolved.config.is_none());
        assert!(orphan.is_dir(), "a disabled spill sweeps nothing");
        assert!(off.left_in_place.is_empty());

        let empty = tempfile::tempdir().expect("cache dir");
        prepare(Some(empty.path()), settings(true, 2 * GIB), None, None, 0)
            .expect("off never errors");
        assert!(
            !empty.path().join("sql-spill").exists(),
            "a disabled spill creates no spill root"
        );
    }

    fn settings_with_read_cache(
        memory_budget_bytes: u64,
        read_cache_bytes: u64,
    ) -> crate::config::SqlSpillSettings {
        crate::config::SqlSpillSettings {
            off: false,
            memory_budget_bytes,
            read_cache_bytes,
            merge_fan_in: crate::config::DEFAULT_SQL_SPILL_MERGE_FAN_IN,
        }
    }

    /// With no `--sql-spill off`, no env pair and no `--cache-dir`, spill
    /// resolves to `<cwd>/sql-spill/<instance-id>` under
    /// `derive_spill_max_bytes(free, budget)`, both startup lines read
    /// `source="cwd-default"`, the orphan sweep and the owner lock run as
    /// they do under `--cache-dir`, and the read cache's bound is not
    /// subtracted: without `--cache-dir` there is no disk tier. The settings
    /// carry a 9,019,431,321-byte read cache bound that would leave the 6 GiB
    /// row nothing to spill to if it were subtracted.
    #[test]
    fn with_no_source_spill_defaults_under_the_working_directory() {
        assert_spill_env_unset();
        for (label, free_bytes, memory_budget_bytes, expected) in [
            ("half of free binds", 6 * GIB, 2 * GIB, 3 * GIB),
            ("four times the budget binds", 100 * GIB, 2 * GIB, 8 * GIB),
        ] {
            assert_eq!(
                ravel_sql::derive_spill_max_bytes(free_bytes, memory_budget_bytes),
                Some(expected),
                "{label}"
            );
            let cwd = tempfile::tempdir().expect("working dir");
            let orphan = dead_sibling(cwd.path(), "inst-dead");
            let (capture, guard) = capture_info();
            let startup = prepare_cwd(
                cwd.path(),
                settings_with_read_cache(memory_budget_bytes, 9_019_431_321),
                SpillFsKind::Other,
                free_bytes,
            )
            .expect("the working-directory default resolves");
            drop(guard);
            let dir = cwd.path().join("sql-spill").join(INSTANCE);
            let config = ravel_sql::SpillConfig {
                dir: dir.clone(),
                max_bytes: expected,
                merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
            };
            assert_eq!(
                startup.resolved,
                ResolvedSqlSpill {
                    config: Some(config.clone()),
                    dir_source: SQL_SPILL_SOURCE_CWD_DEFAULT,
                    max_bytes_source: SQL_SPILL_SOURCE_CWD_DEFAULT,
                    space: Some(SpillSpace {
                        free_bytes,
                        read_cache_bytes: 0,
                    }),
                },
                "{label}"
            );
            assert!(startup.inputs.cwd_default, "{label}");
            assert_eq!(
                startup
                    .owner
                    .as_ref()
                    .map(|owner| owner.dir().to_path_buf()),
                Some(dir.clone()),
                "{label}: this process holds its root's owner lock"
            );
            assert!(!orphan.exists(), "{label}: the dead sibling is swept");
            assert_eq!(
                executor_spill(&startup.inputs),
                Some(config),
                "{label}: the executor spills where the lines say"
            );
            let lines = capture.0.lock().clone();
            let dir_line = resolved_line(&lines, "sql_spill_dir");
            assert!(
                dir_line.contains(&format!(
                    " value={:?} source=\"cwd-default\"",
                    dir.display().to_string()
                )),
                "{label}: {dir_line}"
            );
            let max_line = resolved_line(&lines, "sql_spill_max_bytes");
            assert!(
                max_line.contains(&format!(
                    " value={expected} source=\"cwd-default\" free_bytes={free_bytes} \
                     read_cache_bytes=0"
                )),
                "{label}: {max_line}"
            );
            assert!(capture.1.lock().is_empty(), "{label}: no WARN");
        }
    }

    /// A working directory on tmpfs or ramfs resolves spill off with
    /// `source="cwd-tmpfs"` on both lines and a WARN naming it, startup
    /// succeeds, and nothing is created there.
    #[test]
    fn a_tmpfs_working_directory_resolves_spill_off_and_warns() {
        assert_spill_env_unset();
        let cwd = tempfile::tempdir().expect("working dir");
        let (capture, guard) = capture_info();
        let startup = prepare_cwd(
            cwd.path(),
            settings(false, 2 * GIB),
            SpillFsKind::Memory,
            100 * GIB,
        )
        .expect("a tmpfs working directory does not refuse startup");
        drop(guard);
        assert_eq!(
            startup.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_CWD_TMPFS,
                max_bytes_source: SQL_SPILL_SOURCE_CWD_TMPFS,
                space: None,
            }
        );
        assert!(startup.owner.is_none());
        assert_eq!(startup.inputs, SqlSpillInputs::default());
        assert_eq!(executor_spill(&startup.inputs), None);
        assert!(!cwd.path().join("sql-spill").exists());
        let lines = capture.0.lock().clone();
        assert!(
            resolved_line(&lines, "sql_spill_dir").contains(" value=\"none\" source=\"cwd-tmpfs\"")
        );
        let warns = capture.1.lock().clone();
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].contains(" source=\"cwd-tmpfs\"") && warns[0].contains("tmpfs or ramfs"),
            "{warns:?}"
        );
    }

    /// An existing `<cwd>/sql-spill` that is a tmpfs or ramfs mount of its
    /// own, under a disk-backed working directory, resolves spill off with
    /// `source="cwd-tmpfs"` and a WARN, and creates and locks nothing.
    #[test]
    fn a_tmpfs_sql_spill_mount_under_the_working_directory_resolves_spill_off() {
        assert_spill_env_unset();
        let cwd = tempfile::tempdir().expect("working dir");
        let spill_root = cwd.path().join("sql-spill");
        std::fs::create_dir(&spill_root).expect("sql-spill");
        let (capture, guard) = capture_info();
        let startup = prepare_sql_spill_with(
            None,
            settings(false, 2 * GIB),
            INSTANCE,
            None,
            None,
            || Ok(cwd.path().to_path_buf()),
            |path| {
                if path == spill_root {
                    SpillFsKind::Memory
                } else {
                    SpillFsKind::Other
                }
            },
            |_| Ok(100 * GIB),
        )
        .expect("a tmpfs sql-spill mount does not refuse startup");
        drop(guard);
        assert_eq!(
            startup.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_CWD_TMPFS,
                max_bytes_source: SQL_SPILL_SOURCE_CWD_TMPFS,
                space: None,
            }
        );
        assert!(startup.owner.is_none());
        assert_eq!(startup.inputs, SqlSpillInputs::default());
        assert_eq!(executor_spill(&startup.inputs), None);
        let entries: Vec<_> = std::fs::read_dir(&spill_root)
            .expect("list sql-spill")
            .collect();
        assert!(entries.is_empty(), "{entries:?}");
        let warns = capture.1.lock().clone();
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].contains(" source=\"cwd-tmpfs\"") && warns[0].contains("tmpfs or ramfs"),
            "{warns:?}"
        );
    }

    /// The production filesystem-type probe reads a real tmpfs mount as
    /// memory. The mount is taken from `/proc/self/mounts`; a host with no
    /// tmpfs mount says so and returns.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_filesystem_probe_reads_a_tmpfs_mount_as_memory() {
        let mounts = std::fs::read_to_string("/proc/self/mounts").expect("mount table");
        let Some(mount_point) = mounts.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            let point = fields.nth(1)?;
            (fields.next()? == "tmpfs" && !point.contains('\\')).then(|| point.to_string())
        }) else {
            eprintln!("no tmpfs mount on this host; nothing to probe");
            return;
        };
        assert_eq!(
            spill_fs_kind(std::path::Path::new(&mount_point)),
            SpillFsKind::Memory,
            "{mount_point}"
        );
    }

    /// Set on the child [`prepare_sql_spill_reads_the_process_working_directory`]
    /// starts; without it the child test does nothing.
    const CWD_CHILD_ENV: &str = "RAVEL_TEST_SQL_SPILL_CWD_CHILD";

    /// `prepare_sql_spill` itself, not the `_with` form, resolves the default
    /// under the process working directory: the probes it passes are the real
    /// ones. Changing this process's working directory would race the other
    /// tests, so the test binary re-runs itself with the working directory set
    /// to a temporary directory under the binary's own (disk-backed) target
    /// directory, and that child reports what it resolved and which working
    /// directory its probes read. `cwd-insufficient-space` passes as well as
    /// `cwd-default`: the free-space probe is real, and a nearly full build
    /// volume is not a regression in which directory was probed.
    #[test]
    fn prepare_sql_spill_reads_the_process_working_directory() {
        assert_spill_env_unset();
        let exe = std::env::current_exe().expect("test binary");
        let parent = exe.parent().expect("test binary directory");
        let cwd = tempfile::tempdir_in(parent).expect("working dir");
        let cwd_path = cwd.path().canonicalize().expect("canonical working dir");
        let output = std::process::Command::new(&exe)
            .args([
                "query::tests::prepare_sql_spill_cwd_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .current_dir(&cwd_path)
            .env(CWD_CHILD_ENV, "1")
            .env_remove(ravel_sql::ENV_SPILL_DIR)
            .env_remove(ravel_sql::ENV_SPILL_MAX_BYTES)
            .output()
            .expect("run the child test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "child failed: {stdout}{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let field = |name: &str| {
            let prefix = format!("SPILL-CHILD {name}=");
            let mut values = stdout
                .lines()
                .filter_map(|line| line.split_once(prefix.as_str()).map(|(_, value)| value));
            let value = values
                .next()
                .unwrap_or_else(|| panic!("the child reported no {name}: {stdout}"));
            assert!(
                values.next().is_none(),
                "the child reported {name} twice: {stdout}"
            );
            value
        };
        let (source, cwd, dir) = (field("source"), field("cwd"), field("dir"));
        // The real free-space probe runs, so a nearly full build volume
        // resolves off for lack of space; either way the probes must have
        // read the child's working directory.
        assert_eq!(
            cwd,
            cwd_path.display().to_string(),
            "source={source} dir={dir}"
        );
        match source {
            SQL_SPILL_SOURCE_CWD_DEFAULT => assert_eq!(
                dir,
                cwd_path
                    .join("sql-spill")
                    .join(INSTANCE)
                    .display()
                    .to_string()
            ),
            SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE => assert_eq!(dir, "none"),
            other => panic!("unexpected source={other} cwd={cwd} dir={dir}"),
        }
    }

    /// The child half of [`prepare_sql_spill_reads_the_process_working_directory`].
    #[test]
    #[ignore = "run only as the child of prepare_sql_spill_reads_the_process_working_directory"]
    fn prepare_sql_spill_cwd_child() {
        if std::env::var_os(CWD_CHILD_ENV).is_none() {
            eprintln!("{CWD_CHILD_ENV} is unset; this test runs only as a child process");
            return;
        }
        let startup = prepare_sql_spill(None, settings(false, 2 * GIB), INSTANCE).expect("startup");
        let dir = startup.resolved.config.as_ref().map_or_else(
            || "none".to_string(),
            |config| config.dir.display().to_string(),
        );
        // The working directory the probes read, carried in the inputs both
        // when spill resolves on and when the free space is too little.
        let cwd = startup.inputs.cache_dir.as_ref().map_or_else(
            || "none".to_string(),
            |cache| cache.cache_dir.display().to_string(),
        );
        println!("SPILL-CHILD source={}", startup.resolved.dir_source);
        println!("SPILL-CHILD cwd={cwd}");
        println!("SPILL-CHILD dir={dir}");
    }

    /// The working-directory default resolves off with
    /// `source="cwd-unavailable"`, never an error, when the working directory
    /// cannot be read, when `<cwd>/sql-spill` cannot be created (the working
    /// directory is a file, or a directory without write permission), when
    /// its free space cannot be measured, and when another holder has this
    /// process's root locked; and with `source="cwd-insufficient-space"` when
    /// half the free space is below 1 GiB. None of those leaves a
    /// `<cwd>/sql-spill` behind that was not there before, including a lock
    /// failure after the root was created.
    #[test]
    fn an_unusable_working_directory_resolves_spill_off() {
        assert_spill_env_unset();
        let unavailable = ResolvedSqlSpill {
            config: None,
            dir_source: SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
            max_bytes_source: SQL_SPILL_SOURCE_CWD_UNAVAILABLE,
            space: None,
        };
        let check = |label: &str, startup: anyhow::Result<SqlSpillStartup>| {
            let startup = startup.unwrap_or_else(|err| panic!("{label}: startup refused: {err}"));
            assert_eq!(startup.resolved, unavailable, "{label}");
            assert!(startup.owner.is_none(), "{label}");
            assert_eq!(startup.inputs, SqlSpillInputs::default(), "{label}");
        };

        let scratch = tempfile::tempdir().expect("scratch");
        let file = scratch.path().join("not-a-dir");
        std::fs::write(&file, b"x").expect("file");
        let (capture, guard) = capture_info();
        check(
            "a file",
            prepare_cwd(
                &file,
                settings(false, 2 * GIB),
                SpillFsKind::Other,
                100 * GIB,
            ),
        );
        drop(guard);
        let warns = capture.1.lock().clone();
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].contains(" source=\"cwd-unavailable\""),
            "{warns:?}"
        );

        check(
            "no working directory",
            prepare(None, settings(false, 2 * GIB), None, None, 100 * GIB),
        );

        let cwd = tempfile::tempdir().expect("working dir");
        check(
            "unmeasurable",
            prepare_sql_spill_with(
                None,
                settings(false, 2 * GIB),
                INSTANCE,
                None,
                None,
                || Ok(cwd.path().to_path_buf()),
                |_| SpillFsKind::Other,
                |_| Err(std::io::Error::other("statvfs failed")),
            ),
        );
        assert!(
            !cwd.path().join("sql-spill").exists(),
            "unmeasurable: nothing is created"
        );

        let fresh = tempfile::tempdir().expect("working dir");
        check(
            "lock failed after creating the root",
            Ok(prepare_cwd_spill(
                settings(false, 2 * GIB),
                INSTANCE,
                || Ok(fresh.path().to_path_buf()),
                |_| SpillFsKind::Other,
                |_| Ok(100 * GIB),
                |cwd, instance_id| {
                    std::fs::create_dir(ravel_sql::cache_spill_dir(cwd, instance_id))?;
                    Err(std::io::Error::other("lock held"))
                },
            )),
        );
        assert!(
            !fresh.path().join("sql-spill").exists(),
            "a failed lock removes the root this call created"
        );

        let held = ravel_sql::spill::SpillRootOwner::acquire(cwd.path(), INSTANCE)
            .expect("another holder");
        check(
            "locked",
            prepare_cwd(
                cwd.path(),
                settings(false, 2 * GIB),
                SpillFsKind::Other,
                100 * GIB,
            ),
        );
        assert!(
            held.dir().is_dir(),
            "locked: the holder's root is left in place"
        );
        drop(held);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let sealed = tempfile::tempdir().expect("working dir");
            std::fs::set_permissions(sealed.path(), std::fs::Permissions::from_mode(0o500))
                .expect("remove write permission");
            if std::fs::create_dir(sealed.path().join("probe")).is_ok() {
                eprintln!("this process writes through a read-only directory; skipping that row");
            } else {
                check(
                    "read-only",
                    prepare_cwd(
                        sealed.path(),
                        settings(false, 2 * GIB),
                        SpillFsKind::Other,
                        100 * GIB,
                    ),
                );
            }
            std::fs::set_permissions(sealed.path(), std::fs::Permissions::from_mode(0o700))
                .expect("restore write permission");
        }

        let small = tempfile::tempdir().expect("working dir");
        let (capture, guard) = capture_info();
        let startup = prepare_cwd(
            small.path(),
            settings(false, 2 * GIB),
            SpillFsKind::Other,
            800 * 1024 * 1024,
        )
        .expect("too little space does not refuse startup");
        drop(guard);
        assert_eq!(
            startup.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE,
                max_bytes_source: SQL_SPILL_SOURCE_CWD_INSUFFICIENT_SPACE,
                space: Some(SpillSpace {
                    free_bytes: 800 * 1024 * 1024,
                    read_cache_bytes: 0,
                }),
            }
        );
        assert!(startup.owner.is_none());
        assert_eq!(executor_spill(&startup.inputs), None);
        assert!(
            !small.path().join("sql-spill").exists(),
            "insufficient space: nothing is created"
        );
        let warns = capture.1.lock().clone();
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].contains(" source=\"cwd-insufficient-space\""),
            "{warns:?}"
        );
    }

    /// Every configured source ranks above the working-directory default:
    /// `--sql-spill off`, the env pair and `--cache-dir` each resolve as they
    /// did before it existed, and neither sweeps nor writes the working
    /// directory (a dead sibling root there survives),
    /// and a bare `RAVEL_SQL_SPILL_MAX_BYTES` with no `--cache-dir` is still
    /// the error naming the missing variable.
    #[test]
    fn configured_sources_win_over_the_working_directory_default() {
        let cwd = tempfile::tempdir().expect("working dir");
        let cache = tempfile::tempdir().expect("cache dir");
        let explicit = cache.path().join("explicit");
        let orphan = dead_sibling(cwd.path(), "inst-dead");
        let untouched = |label: &str| {
            let entries: Vec<PathBuf> = std::fs::read_dir(cwd.path().join("sql-spill"))
                .expect("the working directory's spill root")
                .map(|entry| entry.expect("entry").path())
                .collect();
            assert_eq!(
                entries,
                vec![orphan.clone()],
                "{label}: the working directory is neither swept nor written"
            );
        };

        let off = prepare_in(
            cwd.path(),
            SpillFsKind::Other,
            None,
            settings(true, 2 * GIB),
            None,
            None,
            100 * GIB,
        )
        .expect("off never errors");
        assert_eq!(
            off.resolved,
            ResolvedSqlSpill {
                config: None,
                dir_source: SQL_SPILL_SOURCE_FLAG_OFF,
                max_bytes_source: SQL_SPILL_SOURCE_FLAG_OFF,
                space: None,
            }
        );
        untouched("off");

        let env = prepare_in(
            cwd.path(),
            SpillFsKind::Other,
            None,
            settings(false, 2 * GIB),
            os(explicit.to_str().expect("utf-8 temp path")),
            os("4096"),
            100 * GIB,
        )
        .expect("the env pair resolves");
        assert_eq!(
            (env.resolved.config, env.resolved.dir_source),
            (
                Some(ravel_sql::SpillConfig {
                    dir: explicit.clone(),
                    max_bytes: 4096,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                SQL_SPILL_SOURCE_ENV
            )
        );
        untouched("env pair");

        let cache_dir = prepare_in(
            cwd.path(),
            SpillFsKind::Other,
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            None,
            100 * GIB,
        )
        .expect("--cache-dir resolves");
        assert_eq!(
            (
                cache_dir.resolved.config.clone(),
                cache_dir.resolved.dir_source,
                cache_dir.resolved.max_bytes_source
            ),
            (
                Some(ravel_sql::SpillConfig {
                    dir: cache.path().join("sql-spill").join(INSTANCE),
                    max_bytes: 8 * GIB,
                    merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
                }),
                SQL_SPILL_SOURCE_CACHE_DIR,
                SQL_SPILL_SOURCE_DERIVED
            )
        );
        assert!(!cache_dir.inputs.cwd_default);
        untouched("--cache-dir");
        drop(cache_dir);

        let quota_alone = prepare_in(
            cwd.path(),
            SpillFsKind::Other,
            None,
            settings(false, 2 * GIB),
            None,
            os("777"),
            100 * GIB,
        );
        let err = match quota_alone {
            Ok(_) => panic!("a bare quota with no --cache-dir must not resolve"),
            Err(err) => err.to_string(),
        };
        assert!(err.starts_with("RAVEL_SQL_SPILL_DIR is not set"), "{err}");
        untouched("bare quota");
    }

    /// A tree a sweep already moved aside but could not remove is reported
    /// with its own reason, not as a root a live process holds. The tree is
    /// made unremovable with a read-only subdirectory; a process that can
    /// remove it anyway (one with `CAP_DAC_OVERRIDE`) cannot set this up, and
    /// the test says so and returns.
    ///
    /// Prove-the-test: report every left root with `SPILL_ROOT_LEFT_OWNED`
    /// and the reason reads the live-owner text.
    #[cfg(unix)]
    #[test]
    fn a_swept_aside_tree_left_in_place_has_its_own_reason() {
        use std::os::unix::fs::PermissionsExt as _;

        let cache = tempfile::tempdir().expect("cache dir");
        let swept = cache
            .path()
            .join("sql-spill")
            .join(format!("{}1-0-0", ravel_sql::spill::SWEPT_NAME_PREFIX));
        let sealed = swept.join("sealed");
        std::fs::create_dir_all(&sealed).expect("swept tree");
        std::fs::write(sealed.join("spill"), b"x").expect("scratch file");
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o500))
            .expect("seal the subdirectory");
        let unremovable = std::fs::remove_file(sealed.join("spill")).is_err();

        let (capture, guard) = capture_info();
        let startup = prepare(
            Some(cache.path()),
            settings(false, 2 * GIB),
            None,
            None,
            100 * GIB,
        );
        drop(guard);
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o700))
            .expect("unseal for cleanup");
        let startup = startup.expect("spill resolves");
        if !unremovable {
            eprintln!("this process can remove a read-only directory's entries; skipped");
            return;
        }

        assert_eq!(
            startup.left_in_place,
            vec![LeftSpillRoot {
                dir: swept.clone(),
                reason: SPILL_ROOT_LEFT_SWEPT_ASIDE,
            }]
        );
        let lines = capture.0.lock().clone();
        let path = format!(" dir={}", swept.display());
        let reason = format!(" reason={SPILL_ROOT_LEFT_SWEPT_ASIDE:?}");
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.contains("left in place")
                    && line.contains(&path)
                    && line.contains(&reason))
                .count(),
            1,
            "the swept-aside tree is logged once with its reason: {lines:?}"
        );
    }
}
