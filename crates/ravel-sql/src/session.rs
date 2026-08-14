//! Security invariant 2: per-query, single-tenant `SessionContext`.
//!
//! [`build_session`] constructs a complete, throwaway DataFusion session for
//! exactly one query: its own `SessionConfig`, its own `RuntimeEnv`, its own
//! memory pool, and exactly one registered table -- either the requesting
//! tenant's `samples` provider over a `Signal::Metrics` snapshot or its `logs`
//! provider over a `Signal::Logs` snapshot (ADR-0033 decision C: no v1 query
//! spans both signals, so the session registers one, chosen by
//! [`SessionTable`]). Nothing is cached, pooled, or reused across queries or
//! tenants.
//!
//! The session widens no further than the one table the query needs (ADR-0033,
//! "one SQL endpoint, two tables"): a `samples` query registers `samples` and
//! the metric scalar UDFs; a `logs` query registers `logs` and `has_word`. The
//! aggregate allowlist below is enforced for both, because `count(*)` and the
//! numeric aggregates are reachable from either table.
//!
//! This is the tenant-isolation mechanism, not a performance shortcut
//! (framing it as "context construction is cheap enough" invites a later
//! caching optimization that silently converts it into a cross-tenant leak).
//! If context construction ever proves too expensive, the fix is cheaper
//! construction; sharing state requires an ADR.
//!
//! Defense in depth behind the parse gate (crate::validate):
//!
//! - `information_schema` is disabled, so there is no metadata surface to
//!   enumerate even within a single-tenant session.
//! - The `RuntimeEnv` carries [`EmptyObjectStoreRegistry`], which holds no
//!   stores and refuses every lookup. DataFusion's default registry
//!   auto-registers a local filesystem store for `file://`; that default is
//!   replaced here, so a `CREATE EXTERNAL TABLE`/`COPY` that somehow slipped
//!   the parse gate has nothing to bind to. `RsegScanExec` does its own I/O
//!   through `SegmentFetcher` and never consults this registry.
//! - Aggregate registration is an allowlist (ADR-0022 decision 2): every
//!   aggregate UDAF the default session registers is enumerated and every name
//!   outside the admitted set ([`ADMITTED_AGGREGATES`]: `count`, `sum`, `min`,
//!   `max`, `avg`, `mean`) is deregistered. This backstops the subset check in
//!   crate::validate and fails closed under DataFusion upgrades -- a newly added
//!   default aggregate is excluded by default rather than silently reachable.
//!   `avg`/`mean` are admitted (ADR-0022 decisions 3, 4): they stay
//!   in the admitted set so the deregistration loop keeps them, and their
//!   built-in accumulator is then replaced by the sequential-fold UDAF
//!   (crate::avg), the same registry-replacement pattern min/max use.
//! - The `range`/`generate_series` table functions are deregistered
//!   (checkpoint review finding, not in the original design):
//!   `SessionContext::new_with_config_rt` calls DataFusion's
//!   `with_default_features()`, which registers these unconditionally --
//!   `EmptyObjectStoreRegistry` blocks reading anything, but a table
//!   function generates rows in memory and needs no store, so
//!   `SELECT count(*) FROM range(0, 1e18)` would otherwise reach the
//!   planner as a second, ungoverned data source alongside `samples`.
//!
//! Determinism:
//! every `repartition_*` knob is turned off. Float aggregation is
//! order-dependent, so v1 requires aggregations to execute
//! single-partitioned above the merged, deduplicated stream. Left on,
//! `EnforceDistribution` would insert a `RepartitionExec` above
//! `RsegDedupExec` and fan an aggregate out across partitions, reordering
//! summation and breaking bit-exactness against the differential gate's
//! reference. Parallel aggregation is banned until an ADR defines a
//! tolerance policy or a compensated-summation scheme.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::{SessionConfig, SessionContext};
use url::Url;

use crate::avg::sequential_avg_udaf;
use crate::config::SqlConfig;
use crate::logs_provider::LogsTableProvider;
use crate::logs_udf::has_word_udf;
use crate::map_field_planner::map_field_access_planner;
use crate::minmax::{total_order_max_udaf, total_order_min_udaf};
use crate::provider::RavelTableProvider;
use crate::spans_provider::SpansTableProvider;
use crate::udf::{label_match_udf, label_udf};

/// The metrics table name (`Signal::Metrics`).
pub const SAMPLES_TABLE: &str = "samples";

/// The logs table name (`Signal::Logs`, ADR-0033).
pub const LOGS_TABLE: &str = "logs";

/// The spans table name (`Signal::Spans`, ADR-0045 decision 5).
pub const SPANS_TABLE: &str = "spans";

/// The single table a query's session registers. ADR-0033 decision C admits
/// exactly one signal per query in v1, so the executor resolves one snapshot
/// and hands [`build_session`] one provider; the enum keeps the provider
/// types (metrics vs logs vs spans) apart without a `dyn TableProvider`
/// erasure.
pub enum SessionTable {
    /// The `samples` table over a resolved `Signal::Metrics` snapshot.
    Metrics(Arc<RavelTableProvider>),
    /// The `logs` table over a resolved `Signal::Logs` snapshot.
    Logs(Arc<LogsTableProvider>),
    /// The `spans` table over a resolved `Signal::Spans` snapshot (ADR-0045
    /// decision 5). Needs no table-specific scalar UDF: unlike `logs`'
    /// `has_word` and metrics' `label`/`label_match`, every `spans` predicate
    /// this ships (ts, trace_id, duration, status_code, service_name) is a
    /// plain column comparison.
    Spans(Arc<SpansTableProvider>),
}

/// The v1 SQL aggregate allowlist (ADR-0022 decision 2). [`build_session`]
/// enumerates the aggregate UDAFs the default session registers and
/// deregisters every name outside this set, so exclusion is the default state
/// and a DataFusion upgrade that adds a default aggregate fails closed.
/// `avg`/`mean` are admitted (ADR-0022 decision 7) and stay excluded here until their custom UDAF lands.
///
/// The complement within the default registrations lives in
/// [`crate::validate::EXCLUDED_AGGREGATES`]; the two are kept exhaustive by
/// `admitted_and_excluded_aggregates_cover_the_default_registrations` below.
///
/// `avg`/`mean` are admitted (ADR-0022 decisions 3, 4): kept here so
/// the deregistration loop does not drop them, then their built-in accumulator
/// is displaced by the sequential-fold UDAF (crate::avg).
pub const ADMITTED_AGGREGATES: [&str; 6] = ["count", "sum", "min", "max", "avg", "mean"];

/// An `ObjectStoreRegistry` that holds nothing and registers nothing.
///
/// `register_store` silently declines (returning `None`, the trait's "no
/// previous store" answer) and `get_store` always errors. Both halves
/// matter: declining registration is what keeps a store from being
/// installed at runtime, and erroring on lookup is what makes an attempted
/// use fail loudly instead of falling back to a default local filesystem.
#[derive(Debug, Default)]
pub struct EmptyObjectStoreRegistry;

impl ObjectStoreRegistry for EmptyObjectStoreRegistry {
    fn register_store(
        &self,
        _url: &Url,
        _store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        None
    }

    fn get_store(&self, _url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        Err(DataFusionError::Execution(
            "the SQL session registers no object store; \
             external table and file access are not available"
                .to_string(),
        ))
    }
}

/// Build the per-query `SessionConfig`. Separated from [`build_session`] so
/// the invariants above can be asserted without constructing a provider.
pub fn session_config(config: &SqlConfig) -> SessionConfig {
    SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(config.engine.fetch_concurrency.max(1))
        // See the module docs: v1 aggregates must stay single-partitioned.
        .with_repartition_aggregations(false)
        .with_repartition_joins(false)
        .with_repartition_sorts(false)
        .with_repartition_windows(false)
        .with_repartition_file_scans(false)
}

/// Build a fresh, single-tenant `SessionContext` around `table` with `pool`
/// installed as the query's memory pool.
///
/// `table` selects which single table the session exposes (ADR-0033 decision
/// C: one signal per query). The aggregate allowlist, the total-order/
/// sequential-fold UDAF replacements, and the `range`/`generate_series`
/// deregistration run for both, because those are the hard registration
/// boundaries behind the parse gate and must not depend on which table is
/// registered. The table-specific scalar UDFs are registered per table: the
/// metric `label`/`label_match` UDFs only alongside `samples`, and `has_word`
/// only alongside `logs`, so the session exposes exactly the surface the one
/// query needs and no more.
///
/// The caller owns `pool` (typically a `TenantDelegatingPool` from
/// [`SqlConfig::query_pool`](crate::SqlConfig::query_pool)) so it can read
/// the reserved-byte counters after the query finishes.
pub fn build_session(
    config: &SqlConfig,
    pool: Arc<dyn MemoryPool>,
    table: SessionTable,
) -> DFResult<SessionContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_object_store_registry(Arc::new(EmptyObjectStoreRegistry))
        .build_arc()?;

    let mut ctx = SessionContext::new_with_config_rt(session_config(config), runtime);

    // Register a hand-written `ExprPlanner` so `col['key']`
    // (`logs.attrs`, `samples.labels`) plans instead of failing with
    // `GetFieldAccess not supported`. See `crate::map_field_planner` for why
    // this small planner is used instead of the `nested_expressions`
    // feature. Registered for both tables, since `samples.labels` is a `Map`
    // column too and this planner is table-agnostic.
    ctx.register_expr_planner(map_field_access_planner())?;

    // Allowlist enforcement (ADR-0022 decision 2), the hard registration
    // boundary behind the parse gate. Enumerate every aggregate UDAF the
    // default session registered and deregister every name outside the admitted
    // set, so a DataFusion upgrade that registers a new default aggregate fails
    // closed instead of silently widening the SQL surface. Names are collected
    // first because deregistration mutates the same map the accessor borrows.
    let excluded: Vec<String> = ctx
        .state()
        .aggregate_functions()
        .keys()
        .filter(|name| !ADMITTED_AGGREGATES.contains(&name.to_ascii_lowercase().as_str()))
        .cloned()
        .collect();
    for name in &excluded {
        ctx.deregister_udaf(name);
    }
    // Replace the built-in min/max with the total-order UDAF (ADR-0023):
    // `register_udaf` inserts by name and displaces the built-in entry, so
    // grouped and ungrouped MIN/MAX over floating point both use the
    // `f64::total_cmp` order. Non-float input delegates to the wrapped
    // built-in, so `min(ts)` and friends are unchanged. This is the
    // structural guard that replaced the old validation-time rejection of
    // grouped min/max.
    ctx.register_udaf(total_order_min_udaf());
    ctx.register_udaf(total_order_max_udaf());
    // Replace the built-in avg/mean with the sequential-fold UDAF (ADR-0022
    // decisions 3, 4): `register_udaf` inserts by name and by the `mean` alias,
    // displacing both built-in entries so avg's numerator is a naive f64 fold
    // in deterministic order rather than arrow's lane-parallel batch sum. avg
    // stays in `ADMITTED_AGGREGATES` so the loop above keeps the built-in until
    // this line replaces it. This is independent of the public `sum` UDAF,
    // which is untouched (ADR-0024).
    ctx.register_udaf(sequential_avg_udaf());
    // `with_default_features()` (inside `new_with_config_rt` above)
    // registers these unconditionally; they generate rows in memory and so
    // are not blocked by `EmptyObjectStoreRegistry`. `samples` must be the
    // only table this session can query.
    ctx.deregister_udtf("range");
    ctx.deregister_udtf("generate_series");

    // Register exactly the one table this query needs, plus that table's own
    // scalar UDFs. A metrics query never sees `has_word` and a logs query
    // never sees the `label`/`label_match` UDFs, matching the security
    // invariant's stance that the session exposes only what one query needs.
    match table {
        SessionTable::Metrics(provider) => {
            ctx.register_udf(label_udf());
            ctx.register_udf(label_match_udf());
            ctx.register_table(SAMPLES_TABLE, provider)?;
        }
        SessionTable::Logs(provider) => {
            ctx.register_udf(has_word_udf());
            ctx.register_table(LOGS_TABLE, provider)?;
        }
        SessionTable::Spans(provider) => {
            ctx.register_table(SPANS_TABLE, provider)?;
        }
    }
    Ok(ctx)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::Snapshot;
    use ravel_object_store::memory::MemoryStore;

    use ravel_types::accounting::QueryAccounting;

    use super::*;
    use crate::memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};
    use crate::spans_fetcher::SpanSegmentFetcher;

    fn test_pool() -> Arc<dyn MemoryPool> {
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        // The pool reports its reserved high-water mark into per-query
        // accounting (ADR-0044 decision 1's `peak_intermediate_bytes`), so it
        // takes a handle. These session tests do not assert on it.
        Arc::new(TenantDelegatingPool::new(
            1 << 30,
            tenant,
            breach,
            QueryAccounting::new(),
        ))
    }

    /// Collect every `WHERE`/`HAVING` predicate in `plan` as a top-level AND
    /// conjunct, mirroring `executor::collect_filter_predicates`: recurses
    /// through the plan's inputs so the filter is found regardless of which
    /// operators DataFusion places above it.
    fn collect_filter_predicates(
        plan: &datafusion::logical_expr::LogicalPlan,
        out: &mut Vec<datafusion::logical_expr::Expr>,
    ) {
        if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
            out.push(filter.predicate.clone());
        }
        for input in plan.inputs() {
            collect_filter_predicates(input, out);
        }
    }

    /// The ADR-0045 decision 5 acceptance test: `spans` plans through
    /// `build_session` (registration), and a real `WHERE` clause conjoining
    /// `duration_ns`, `status_code`, and `service_name` -- parsed and
    /// type-coerced by DataFusion itself, not hand-built through the fluent
    /// `col()`/`lit()` API `spans_pushdown`'s own unit tests use -- extracts
    /// into exactly the `SpansPushdown` bounds those predicates prove.
    #[tokio::test]
    async fn spans_table_is_registered_and_prunes_duration_status_service() {
        let store: Arc<dyn ravel_object_store::ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let fetcher = SpanSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = crate::spans_provider::SpansTableProvider::new(
            snapshot,
            ravel_types::TenantHash([0u8; 16]),
            fetcher,
            QueryAccounting::new(),
        );
        let ctx = build_session(
            &SqlConfig::default(),
            test_pool(),
            SessionTable::Spans(Arc::new(provider)),
        )
        .expect("spans session builds");

        // Registration and planning proof: a trivial SELECT * still executes
        // over the empty snapshot.
        let df = ctx
            .sql("SELECT * FROM spans")
            .await
            .expect("SELECT * FROM spans plans");
        let batches = df.collect().await.expect("empty snapshot scan executes");
        assert!(batches.iter().all(|b| b.num_rows() == 0));

        // The pruning proof: extract_spans over the real, SQL-parsed filter.
        let sql = "SELECT * FROM spans WHERE duration_ns > 500000000 \
                   AND status_code = 2 AND service_name = 'checkout'";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("filtered query plans");
        let mut predicates = Vec::new();
        collect_filter_predicates(&plan, &mut predicates);
        let pushdown = crate::spans_pushdown::extract_spans(&predicates);

        assert_eq!(pushdown.duration_lo, Some(500_000_001));
        assert_eq!(pushdown.duration_hi, None);
        assert_eq!(
            pushdown.status_mask,
            Some(ravel_rspan::skip_index::STATUS_BIT_ERROR)
        );
        assert_eq!(pushdown.service_name, Some("checkout".to_string()));
    }

    #[test]
    fn information_schema_is_disabled_and_repartitioning_is_off() {
        let config = session_config(&SqlConfig::default());
        let options = config.options();
        assert!(
            !options.catalog.information_schema,
            "information_schema must stay disabled (security invariant 1)"
        );
        assert!(!options.optimizer.repartition_aggregations);
        assert!(!options.optimizer.repartition_joins);
        assert!(!options.optimizer.repartition_sorts);
        assert!(!options.optimizer.repartition_windows);
        assert!(!options.optimizer.repartition_file_scans);
    }

    /// ADR-0022 decision 2: the validation reject-list
    /// (`crate::validate::EXCLUDED_AGGREGATES`) plus the registration allowlist
    /// ([`ADMITTED_AGGREGATES`]) must exactly cover every aggregate UDAF the
    /// default DataFusion session registers -- primary spellings and aliases.
    /// A version bump that adds a default aggregate breaks this test instead of
    /// silently widening the SQL surface, and a stale entry (a name no longer
    /// registered) is caught the same way.
    #[test]
    fn admitted_and_excluded_aggregates_cover_the_default_registrations() {
        use std::collections::BTreeSet;

        // A default context registers the same aggregate UDAFs `build_session`
        // starts from; the session_config flags do not add or remove any.
        let ctx = SessionContext::new();
        let registered: BTreeSet<String> = ctx
            .state()
            .aggregate_functions()
            .keys()
            .map(|name| name.to_ascii_lowercase())
            .collect();

        let mut classified: BTreeSet<String> = BTreeSet::new();
        for name in ADMITTED_AGGREGATES {
            assert!(
                classified.insert(name.to_string()),
                "duplicate admitted aggregate name {name}"
            );
        }
        for name in crate::validate::EXCLUDED_AGGREGATES {
            assert!(
                classified.insert(name.to_string()),
                "{name} appears in both the admitted set and the excluded list"
            );
        }

        let unclassified: Vec<&String> = registered.difference(&classified).collect();
        let stale: Vec<&String> = classified.difference(&registered).collect();
        assert!(
            unclassified.is_empty() && stale.is_empty(),
            "aggregate allowlist/reject-list drifted from the default registrations.\n  \
             registered but unclassified (surface silently widened; add to the admitted set \
             or crate::validate::EXCLUDED_AGGREGATES): {unclassified:?}\n  \
             classified but not registered (stale entry to remove): {stale:?}"
        );
    }

    #[test]
    fn the_empty_registry_refuses_lookups_and_registrations() {
        let registry = EmptyObjectStoreRegistry;
        let url = Url::parse("file:///tmp/").expect("valid url");
        assert!(
            registry.get_store(&url).is_err(),
            "an empty registry must not resolve file:// to a local store"
        );
        let s3 = Url::parse("s3://bucket/").expect("valid url");
        assert!(registry.get_store(&s3).is_err());
    }
}
