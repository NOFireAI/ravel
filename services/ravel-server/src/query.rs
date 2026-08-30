//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`,
//! and, behind the `sql` feature, the `SqlState` for `/api/v1/sql`.

use std::path::PathBuf;
use std::sync::Arc;

use ravel_cache::CacheLimits;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{EngineConfig, QueryAdmissionController, QueryEngine, ReadCache};
use ravel_types::accounting::{AccountedOp, CostEstimate, QueryAccountingSnapshot};

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
            "decompressedBytes": accounting.decompressed_bytes,
            "segmentsOpened": accounting.segments_opened,
            "seriesMatched": accounting.series_matched,
            "bytesReused": accounting.bytes_reused,
            "peakIntermediateBytes": accounting.peak_intermediate_bytes,
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
/// segment count, the store bytes the worker reported, and the routing outcome
/// (`ok` / `fallback` / `error`). No per-shard cardinality beyond the entries
/// themselves: this is the response body, not the metric allowlist.
pub fn fragments_json(entries: &[crate::distrib::FragmentStatEntry]) -> serde_json::Value {
    serde_json::Value::Array(
        entries
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "workerEndpoint": entry.worker_endpoint,
                    "segmentCount": entry.segment_count,
                    "bytesReported": entry.bytes_reported,
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
/// `disable_cache` and `cache_max_bytes` are the CLI's `--disable-cache` and
/// `--cache-max-bytes`, the same flags that govern the fetcher cache in
/// [`crate::store::build_cache`]. They reach the catalog's ADR-0046 byte cache
/// too: `--disable-cache` builds a catalog with no byte cache at
/// all (the `byte_cache_max_bytes: 0` sentinel), so a memory-constrained
/// `--disable-cache` deployment no longer silently keeps a 512 MiB catalog
/// byte cache; otherwise `cache_max_bytes` is the catalog byte cache's total
/// budget, sharing one number with the fetcher cache. The other two byte-cache
/// bounds keep their catalog defaults (the CLI has no flag for them).
///
/// `cache_dir` is the CLI's `--cache-dir` (#97). When present and the byte cache
/// is not disabled, the catalog byte cache gains an ADR-0046 local-disk tier at
/// that path. Its [`CacheLimits`] reuse the SAME `cache_max_bytes` number for
/// both the RAM and disk tiers (there is no separate disk-tier capacity flag),
/// with this crate's own byte-cache entry-count/max-entry-bytes defaults, so the
/// disk tier shares the one `--cache-max-bytes` number the fetcher cache and the
/// catalog RAM tier already share. `--disable-cache` (the `0` sentinel) wins
/// over a configured `cache_dir`: no cache of either tier is built.
pub fn build_catalog(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    disable_cache: bool,
    cache_max_bytes: u64,
    cache_dir: Option<PathBuf>,
) -> anyhow::Result<Arc<Catalog>> {
    // `0` is the byte cache's disabled sentinel (ravel_catalog::CatalogConfig):
    // Catalog::new then constructs no byte cache. Mirrors how build_cache turns
    // --disable-cache into a `None` fetcher cache.
    let byte_cache_max_bytes = if disable_cache { 0 } else { cache_max_bytes };
    let catalog_config = CatalogConfig {
        shard_count,
        byte_cache_max_bytes,
        ..CatalogConfig::default()
    };
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

/// Build the query `AppState`. `engine_config` carries the resolved query
/// deadline (ADR-0050 section 4, EC4): the caller passes the SAME
/// `EngineConfig` whose `deadline` was validated against `sys/gc` in `main`, so
/// the engine that actually enforces the deadline uses the validated value
/// rather than an independent `EngineConfig::default()`.
#[allow(clippy::too_many_arguments)]
pub fn build_app_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    distributed: Option<Arc<ravel_query::distrib::Distributed>>,
    federation: Option<Arc<ravel_query::distrib::Federation>>,
    metadata_cache: Option<Arc<ravel_query::http::MetadataCache>>,
) -> AppState {
    let mut engine = QueryEngine::new(catalog, store, engine_config);
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
    let state = AppState::new(Arc::new(engine), tenant_resolver)
        .with_cost_recorder(query_accounting)
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
#[cfg(feature = "sql")]
#[allow(clippy::too_many_arguments)]
pub fn build_sql_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<ReadCache>,
    engine_config: EngineConfig,
    max_query_bytes: usize,
    max_tenant_bytes: usize,
    parallel_final_aggregation: bool,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    query_admission: Arc<QueryAdmissionController>,
    declared_columns: Option<Arc<dyn ravel_sql::DeclaredColumnSource>>,
) -> anyhow::Result<crate::sql::SqlState> {
    use ravel_query::{LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor};

    let config = SqlConfig {
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
        // ADR-0954: bounded ephemeral spill. Left at the default (`None`, spill
        // off) with no server-level flag or env plumbing yet: this line only
        // keeps the exhaustive literal compiling now that `SqlConfig` carries
        // the field. Wiring `--sql-spill-dir` / `RAVEL_SQL_SPILL_*` into the
        // server is the follow-up (issue #954), not this change.
        spill: SqlConfig::default().spill,
    };
    let max_deadline = config.engine.deadline;
    let mut metrics_fetcher = SegmentFetcher::new(store.clone());
    // ADR-0107's read-shape crossover, from `--logs-block-range-threshold` via
    // `QueryBudgets::apply_to_engine`. This is the single wiring point for it, so
    // an operator who sets the flag to `u64::MAX` gets whole-object logs reads
    // (the pre-ADR-0107 shape) on every SQL logs scan this process serves.
    // `--fetch-concurrency` is documented (ADR-0088) as the knob that bounds
    // S3 GET concurrency; the logs fetcher keeps its own permit pool (ADR-0107
    // decision 1, separate from RSEG's), so the bound has to be handed to it
    // here or the pool stays at its compiled-in 16 whatever the flag says
    // (issue #700).
    // `--logs-request-cost-bytes` (ADR-0904) reaches the same fetcher the same
    // way: the request cost lives on the block-range fetcher this builder owns,
    // so the flag has to be handed over here or the fetcher keeps its
    // compiled-in `DEFAULT_LOG_REQUEST_COST_BYTES` whatever the flag says, and
    // with it the coalescing gap, the whole-object crossover, and the fast
    // path's projection routing that are all derived from it.
    let mut logs_fetcher = LogSegmentFetcher::new(store.clone())
        .with_block_range_threshold(config.engine.logs_block_range_threshold)
        .with_max_concurrent_gets(config.engine.fetch_concurrency)
        .with_request_cost_bytes(config.engine.logs_request_cost_bytes);
    // The spans fetcher (RSPAN) reads the same object store, with the default
    // RspanConfig (ADR-0045 decision 5). It attaches no fetcher cache: unlike
    // the RSEG/RLOG fetchers it has no `with_cache` seam, and none is wired
    // here. Its `fetch_accounted` path is tenant-checked and accounted (ADR-0045
    // via #1080), so a `spans` query is isolated and metered like any other.
    let span_fetcher = SpanSegmentFetcher::new(store.clone());
    if let Some(cache) = cache {
        metrics_fetcher = metrics_fetcher.with_cache(cache.clone());
        logs_fetcher = logs_fetcher.with_cache(cache);
    }
    // The metrics fetcher (RSEG), the logs fetcher (RLOG), and the spans
    // fetcher (RSPAN) all read the same object store; the executor uses
    // whichever the query's target table needs (ADR-0033, extended to `spans`
    // by ADR-0045 decision 5).
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
        // The SQL HTTP audit routes through the QueryAuditSink seam;
        // installing the process-wide AuditPipeline is a separate server-wiring
        // step, so this defaults to the no-op today. The endpoint already
        // submits and awaits through the trait, so enabling it is a one-line swap.
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
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
            None,
            None,
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
    /// slice with the deliverable's four fields, and an empty input renders an
    /// empty array (the query handler then omits the field entirely).
    #[test]
    fn fragments_json_renders_camelcase_per_slice_shape() {
        let entries = vec![
            crate::distrib::FragmentStatEntry {
                worker_endpoint: "10.0.0.1:7000".to_string(),
                segment_count: 3,
                bytes_reported: 4096,
                status: "ok",
            },
            crate::distrib::FragmentStatEntry {
                worker_endpoint: "192.0.2.1:9".to_string(),
                segment_count: 1,
                bytes_reported: 0,
                status: "fallback",
            },
        ];
        let json = fragments_json(&entries);
        let array = json.as_array().expect("fragments is a JSON array");
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["workerEndpoint"], "10.0.0.1:7000");
        assert_eq!(array[0]["segmentCount"], 3);
        assert_eq!(array[0]["bytesReported"], 4096);
        assert_eq!(array[0]["status"], "ok");
        assert_eq!(array[1]["workerEndpoint"], "192.0.2.1:9");
        assert_eq!(array[1]["status"], "fallback");
        assert!(
            fragments_json(&[])
                .as_array()
                .expect("empty renders an array")
                .is_empty(),
            "no entries render an empty array, which the handler omits"
        );
    }

    /// with caching on, `--cache-max-bytes` must bound the catalog
    /// byte cache, not just the fetcher cache. The value reaches
    /// `CatalogConfig::byte_cache_max_bytes`, and the byte cache (with its
    /// counters handle) is constructed.
    #[test]
    fn build_catalog_wires_cache_max_bytes_through_to_the_byte_cache() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let budget = 7 * 1024 * 1024;
        let catalog = build_catalog(store, 1, false, budget, None).expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            budget,
            "--cache-max-bytes must bound the catalog byte cache, not only the fetcher cache"
        );
        assert!(
            catalog.byte_cache_metrics().is_some(),
            "an enabled catalog byte cache must expose its counters handle for /metrics"
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
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
        )
        .expect("sql state builds");

        assert_eq!(
            state.max_deadline, non_default.deadline,
            "build_sql_state's max_deadline must be the resolved value passed in, \
             not an independent EngineConfig::default()"
        );
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
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            DEFAULT_MAX_TENANT_BYTES,
            false,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
        )
        .expect("sql state builds");
        assert_eq!(
            state.executor.config().engine.max_bytes_scanned,
            ByteLimit::Bounded(4096),
            "the SQL executor must enforce the configured byte budget, not the default Unlimited"
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
        )
        .expect("catalog");
        let budgets = crate::Cli::try_parse_from(argv)
            .expect("flags parse")
            .query_budgets();
        build_sql_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            EngineConfig::default(),
            budgets.sql_max_query_bytes,
            budgets.sql_tenant_max_bytes,
            budgets.sql_parallel_final_aggregation,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
            None,
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

        // Unset: byte-identical to the compiled-in per-query default.
        let state = sql_state_from_cli(&["ravel-server"]);
        assert_eq!(
            state.executor.config().max_query_bytes,
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
        );
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

        // Unset: byte-identical to the compiled-in per-tenant default.
        let state = sql_state_from_cli(&["ravel-server"]);
        assert_eq!(state.executor.max_tenant_bytes(), DEFAULT_MAX_TENANT_BYTES);
    }
}
