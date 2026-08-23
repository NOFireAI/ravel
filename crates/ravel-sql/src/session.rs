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
//! - Function registration is an allowlist across every registry (ADR-0022
//!   decision 2 for aggregates, ADR-0097 decisions 2/4/6 for the rest):
//!   [`build_session`] enumerates each of `aggregate_functions()`,
//!   `scalar_functions()`, `window_functions()`, and `table_functions()` and
//!   deregisters every name outside that registry's admitted set
//!   ([`ADMITTED_AGGREGATES`], [`ADMITTED_SCALARS`], [`ADMITTED_WINDOWS`],
//!   [`ADMITTED_TABLE_FUNCTIONS`]). This backstops the subset check in
//!   crate::validate and fails closed under DataFusion upgrades -- a newly added
//!   default function in any registry is excluded by default rather than
//!   silently reachable. `avg`/`mean` are admitted (ADR-0022 decisions 3, 4):
//!   they stay in the admitted set so the deregistration loop keeps them, and
//!   their built-in accumulator is then replaced by the sequential-fold UDAF
//!   (crate::avg), the same registry-replacement pattern min/max use.
//! - The table-function admitted set is empty, so `range`/`generate_series`
//!   (registered unconditionally by DataFusion's `with_default_features()`
//!   inside `SessionContext::new_with_config_rt`) are deregistered like any
//!   other non-admitted name. `EmptyObjectStoreRegistry` blocks reading
//!   anything, but a table function generates rows in memory and needs no
//!   store, so `SELECT count(*) FROM range(0, 1e18)` would otherwise reach the
//!   planner as a second, ungoverned data source alongside the one registered
//!   table.
//!
//! Determinism:
//! every `repartition_*` knob except aggregation is turned off unconditionally.
//! Float aggregation is order-dependent, so v1 requires it to execute
//! single-partitioned above the merged, deduplicated stream. Left on,
//! `EnforceDistribution` would insert a `RepartitionExec` above
//! `RsegDedupExec` and fan an aggregate out across partitions, reordering
//! summation and breaking bit-exactness against the differential gate's
//! reference.
//!
//! `repartition_aggregations` is the one exception (ADR-0094): it reads the
//! `exact_typed_aggregates` argument [`session_config`] receives, rather than a
//! hardcoded `false`. That argument is `true` only when a per-query
//! classification (`executor::SqlExecutor`) has proven every aggregate and
//! GROUP BY key in the query is order/partition-independent -- `count`, and
//! `sum`/`min`/`max` over a resolved non-float input, with no float group key;
//! `avg`/`mean` (always plain IEEE f64 addition) and any float input or key are
//! never eligible. It is `false` for every other query and behind a default-off
//! `SqlConfig` flag, so this is byte-identical to the old single-partition plan
//! unless an operator opts in. The join/sort/window/file-scan knobs stay
//! unconditionally `false`: their determinism requirements are out of ADR-0094's
//! scope.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
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
/// `avg`/`mean` are admitted (ADR-0022 decisions 3, 4): they are kept in this
/// set so the deregistration loop preserves the built-in entries, which
/// [`build_session`] then replaces with the sequential-fold UDAF (crate::avg).
///
/// The complement within the default registrations lives in
/// [`crate::validate::EXCLUDED_AGGREGATES`]; the two are kept exhaustive by
/// `admitted_and_excluded_aggregates_cover_the_default_registrations` below.
pub const ADMITTED_AGGREGATES: [&str; 6] = ["count", "sum", "min", "max", "avg", "mean"];

/// The v1 SQL scalar allowlist (ADR-0097 decisions 2, 4). [`build_session`]
/// enumerates the scalar UDFs the default session registers and deregisters
/// every name outside this set, the same fail-closed pattern
/// [`ADMITTED_AGGREGATES`] uses. Scalars are pure per-row transforms, so
/// ADR-0022's sequential-accumulation concern does not apply and the whole
/// registered surface stays admitted except the eight in [`EXCLUDED_SCALARS`].
///
/// Ravel's own per-table scalar UDFs (`label` and `label_match` for Metrics,
/// `has_word` for Logs) are part of this set: they are registered *after* the
/// deregistration loop, per table, so they are never enumerated by it, but the
/// drift test counts them here. No single session registers all three; each
/// table's session registers the upstream admitted scalars plus that table's
/// own UDFs (ADR-0097 decision 2: "one upstream set plus a per-table
/// addendum").
///
/// The complement within the upstream default registrations lives in
/// [`EXCLUDED_SCALARS`]; the two are kept exhaustive by
/// `admitted_and_excluded_cover_all_registries_for_every_table` below.
pub const ADMITTED_SCALARS: [&str; 127] = [
    // Ravel's own per-table scalar UDFs (registered after the loop, per table).
    "label",
    "label_match",
    "has_word",
    // The upstream datafusion-functions scalar packs (string, unicode,
    // datetime, math, regex, encoding), minus the eight in EXCLUDED_SCALARS.
    "abs",
    "acos",
    "acosh",
    "arrow_cast",
    "arrow_field",
    "arrow_metadata",
    "arrow_try_cast",
    "arrow_typeof",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "bit_length",
    "btrim",
    "cast_to_type",
    "cbrt",
    "ceil",
    "char_length",
    "character_length",
    "chr",
    "coalesce",
    "concat",
    "concat_ws",
    "contains",
    "cos",
    "cosh",
    "cot",
    "date_bin",
    "date_format",
    "date_part",
    "date_trunc",
    "datepart",
    "datetrunc",
    "decode",
    "degrees",
    "encode",
    "ends_with",
    "exp",
    "factorial",
    "find_in_set",
    "floor",
    "from_unixtime",
    "gcd",
    "get_field",
    "greatest",
    "ifnull",
    "initcap",
    "instr",
    "isnan",
    "iszero",
    "lcm",
    "least",
    "left",
    "length",
    "levenshtein",
    "ln",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "make_date",
    "make_time",
    "named_struct",
    "nanvl",
    "nullif",
    "nvl",
    "nvl2",
    "octet_length",
    "overlay",
    "pi",
    "position",
    "pow",
    "power",
    "radians",
    "regexp_count",
    "regexp_instr",
    "regexp_like",
    "regexp_match",
    "regexp_replace",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row",
    "rpad",
    "rtrim",
    "signum",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "strpos",
    "struct",
    "substr",
    "substr_index",
    "substring",
    "substring_index",
    "tan",
    "tanh",
    "to_char",
    "to_date",
    "to_hex",
    "to_local_time",
    "to_time",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_nanos",
    "to_timestamp_seconds",
    "to_unixtime",
    "translate",
    "trim",
    "trunc",
    "try_cast_to_type",
    "union_extract",
    "union_tag",
    "upper",
    "with_metadata",
];

/// The scalar UDFs the default DataFusion session registers that are **not**
/// admitted (ADR-0097 decision 4): nondeterministic or environment-reading, so
/// unattestable by the differential conformance oracle. `uuid`/`random`/`rand`
/// return a different answer for identical input; `now`/`current_timestamp`/
/// `current_date`/`current_time` read the wall clock (query-time is available
/// through the request's own time range); `version` reports the DataFusion
/// build to any caller. [`build_session`] deregisters these, and the drift test
/// keeps this list exhaustive against the upstream registrations.
///
/// `today` is here even though ADR-0097 decision 4 names only eight: it is a
/// DataFusion alias of `current_date` (datafusion-functions 54's
/// `current_date.rs` registers `aliases: vec!["today"]`), so it reads the same
/// wall clock and is caught by the same rationale. It also cannot be admitted
/// independently: deregistering `current_date` removes the shared UDF, dropping
/// the `today` key too, so admitting it would make the drift test fail closed.
pub const EXCLUDED_SCALARS: [&str; 9] = [
    "uuid",
    "random",
    "rand",
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "version",
    "today",
];

/// The v1 SQL window allowlist (ADR-0097 decision 6). The rank/offset families
/// carry no floating-point accumulation and are deterministic under the
/// single-partition rule; `cume_dist`/`percent_rank` return one correctly
/// rounded IEEE division of two exact integer counts, deterministic and
/// bit-reproducible. [`build_session`] enumerates `window_functions()` and
/// deregisters every name outside this set.
pub const ADMITTED_WINDOWS: [&str; 8] = [
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "cume_dist",
    "percent_rank",
];

/// The window UDWFs the default session registers that are **not** admitted
/// (ADR-0097 decision 6). Until this enumeration existed these three were
/// refused only incidentally, because their names collide with
/// [`crate::validate::EXCLUDED_AGGREGATES`], a list written about aggregates.
/// Deregistering them here makes the refusal explicit and window-aware; whether
/// they are ever readmitted is a conformance-row decision, not a default.
pub const EXCLUDED_WINDOWS: [&str; 3] = ["first_value", "last_value", "nth_value"];

/// The v1 SQL table-function allowlist (ADR-0097 decision 2): empty. No table
/// function is reachable today and none should become reachable by an upstream
/// addition. [`build_session`] enumerates `table_functions()` and deregisters
/// every name, replacing the earlier two hardcoded `deregister_udtf` calls so a
/// DataFusion release adding a third table function cannot ship it into the
/// surface.
pub const ADMITTED_TABLE_FUNCTIONS: [&str; 0] = [];

/// The table functions the default session registers, all excluded (ADR-0097
/// decision 2). `range`/`generate_series` generate rows in memory, so
/// [`EmptyObjectStoreRegistry`] does not block them; they reach the planner as
/// a second, ungoverned data source unless deregistered. Kept exhaustive
/// against the upstream registrations by the drift test.
pub const EXCLUDED_TABLE_FUNCTIONS: [&str; 2] = ["generate_series", "range"];

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
///
/// `exact_typed_aggregates` (ADR-0094 decision 2) is the one per-query input:
/// `true` only when the caller's classification proved the query's aggregates
/// and GROUP BY keys are order/partition-independent, allowing DataFusion to
/// fan the final aggregation across partitions. `false` for every other query,
/// which reproduces the old single-partition plan exactly.
pub fn session_config(config: &SqlConfig, exact_typed_aggregates: bool) -> SessionConfig {
    SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(config.engine.fetch_concurrency.max(1))
        // ADR-0094: the only knob that is ever true, and only for a query whose
        // aggregates and group keys are all exact-typed. See the module docs.
        .with_repartition_aggregations(exact_typed_aggregates)
        .with_repartition_joins(false)
        .with_repartition_sorts(false)
        .with_repartition_windows(false)
        .with_repartition_file_scans(false)
}

/// Build a fresh, single-tenant `SessionContext` around `table` with `pool`
/// installed as the query's memory pool.
///
/// `table` selects which single table the session exposes (ADR-0033 decision
/// C: one signal per query). The per-registry allowlists (aggregate, scalar,
/// window, table function), the total-order/sequential-fold UDAF replacements,
/// and the table-function deregistration run for every table, because those
/// are the hard registration boundaries behind the parse gate and must not
/// depend on which table is registered. The table-specific scalar UDFs are registered per table: the
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
    exact_typed_aggregates: bool,
) -> DFResult<SessionContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_object_store_registry(Arc::new(EmptyObjectStoreRegistry))
        // ADR-0102 decision 3: disable spill so a query over the memory budget
        // fails as a typed `ResourcesExhausted` error instead of silently
        // spilling to local disk (which ADR-0013 forbids: budget exhaustion is
        // an error, never a partial result, and no durability may depend on
        // local disk). Without this the disk manager defaults to
        // `OsTmpDirectory` with a 100 GB ceiling and routes around the pool's
        // `try_grow` enforcement.
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;

    let mut ctx =
        SessionContext::new_with_config_rt(session_config(config, exact_typed_aggregates), runtime);

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

    // The same fail-closed boundary for the scalar registry (ADR-0097
    // decisions 2, 4). Running before the per-table UDF registration below
    // keeps this enumeration over upstream defaults only. That order is
    // defensive, not load-bearing: `label`, `label_match`, and `has_word` are
    // in ADMITTED_SCALARS, so the filter spares them from either position. It
    // becomes load-bearing if a Ravel UDF ever leaves that list, which is the
    // membership `per_table_scalar_udfs_survive_the_scalar_gate` asserts.
    let excluded_scalars: Vec<String> = ctx
        .state()
        .scalar_functions()
        .keys()
        .filter(|name| !ADMITTED_SCALARS.contains(&name.to_ascii_lowercase().as_str()))
        .cloned()
        .collect();
    for name in &excluded_scalars {
        ctx.deregister_udf(name);
    }

    // And the window registry (ADR-0097 decision 6). Before this enumeration
    // existed, `first_value`/`last_value`/`nth_value` were refused only because
    // their names collide with the aggregate deny-list in crate::validate; the
    // other eight window functions executed with no gate at all.
    let excluded_windows: Vec<String> = ctx
        .state()
        .window_functions()
        .keys()
        .filter(|name| !ADMITTED_WINDOWS.contains(&name.to_ascii_lowercase().as_str()))
        .cloned()
        .collect();
    for name in &excluded_windows {
        ctx.deregister_udwf(name);
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
    // The table-function registry (ADR-0097 decision 2), enumerated like the
    // others rather than removed by hardcoded name. `with_default_features()`
    // (inside `new_with_config_rt` above) registers `range`/`generate_series`
    // unconditionally; they generate rows in memory and so are not blocked by
    // `EmptyObjectStoreRegistry`. The admitted set is empty: the one table this
    // session can query is the one registered below, and a DataFusion release
    // adding a third table function is deregistered by default instead of
    // shipping into the surface.
    let excluded_udtfs: Vec<String> = ctx
        .state()
        .table_functions()
        .keys()
        .filter(|name| !ADMITTED_TABLE_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str()))
        .cloned()
        .collect();
    for name in &excluded_udtfs {
        ctx.deregister_udtf(name);
    }

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
            // Rewrite `col LIKE 'pattern'` into the Ravel `like` UDF (#479) so a
            // declared `Str` column's dictionary reaches the matcher intact,
            // matched once per distinct value. Registered as a function rewrite
            // (runs before type coercion) so it sees the un-hydrated operands;
            // see `crate::like_udf`.
            ctx.register_function_rewrite(Arc::new(crate::like_udf::LikeToUdf))?;
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
            false,
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
        let config = session_config(&SqlConfig::default(), false);
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

    /// ADR-0094 decision 2: `exact_typed_aggregates = true` flips ONLY
    /// `repartition_aggregations` on; every other `repartition_*` knob stays
    /// unconditionally off, and `information_schema` stays disabled.
    #[test]
    fn exact_typed_aggregates_flips_only_repartition_aggregations() {
        let config = session_config(&SqlConfig::default(), true);
        let options = config.options();
        assert!(!options.catalog.information_schema);
        assert!(
            options.optimizer.repartition_aggregations,
            "exact-typed queries repartition their final aggregation (ADR-0094)"
        );
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

    fn empty_snapshot() -> Snapshot {
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        }
    }

    fn metrics_table(store: &Arc<dyn ravel_object_store::ObjectStoreBackend>) -> SessionTable {
        SessionTable::Metrics(Arc::new(crate::provider::RavelTableProvider::new(
            empty_snapshot(),
            ravel_types::TenantHash([0u8; 16]),
            ravel_query::SegmentFetcher::new(store.clone()),
            SqlConfig::default(),
            QueryAccounting::new(),
        )))
    }

    fn logs_table(store: &Arc<dyn ravel_object_store::ObjectStoreBackend>) -> SessionTable {
        SessionTable::Logs(Arc::new(crate::logs_provider::LogsTableProvider::new(
            empty_snapshot(),
            ravel_types::TenantHash([0u8; 16]),
            ravel_query::LogSegmentFetcher::new(store.clone()),
            QueryAccounting::new(),
        )))
    }

    fn spans_table(store: &Arc<dyn ravel_object_store::ObjectStoreBackend>) -> SessionTable {
        SessionTable::Spans(Arc::new(crate::spans_provider::SpansTableProvider::new(
            empty_snapshot(),
            ravel_types::TenantHash([0u8; 16]),
            SpanSegmentFetcher::new(store.clone()),
            QueryAccounting::new(),
        )))
    }

    /// ADR-0097 decisions 2, 3, 4, 6: the fail-closed boundary now covers all
    /// four registries, not aggregates alone. For every table variant and every
    /// registry, the admitted and excluded name sets must be disjoint, must
    /// together cover exactly the upstream default registrations (so a
    /// DataFusion upgrade that adds any function to any registry fails this test
    /// naming it), and a session built by [`build_session`] must register
    /// exactly the upstream admitted names plus that table's own Ravel UDFs (so
    /// a per-table UDF that goes missing or lands on the wrong table also fails
    /// it). All comparisons are lowercased.
    #[test]
    fn admitted_and_excluded_cover_all_registries_for_every_table() {
        use std::collections::BTreeSet;

        fn set<'a>(names: impl IntoIterator<Item = &'a &'a str>) -> BTreeSet<String> {
            names.into_iter().map(|n| n.to_ascii_lowercase()).collect()
        }
        fn keys(names: impl Iterator<Item = String>) -> BTreeSet<String> {
            names.map(|n| n.to_ascii_lowercase()).collect()
        }

        /// Assert one registry for one table. `admitted`/`excluded` are the
        /// full per-registry constants (scalar admits include Ravel's own
        /// UDFs); `all_ravel` is every Ravel-added name in this registry across
        /// all tables (empty except for scalars); `ravel_for_table` is the
        /// subset registered for THIS table; `default_upstream` is what a plain
        /// `SessionContext::new()` registers; `registered` is what
        /// `build_session` left after the gate.
        #[allow(clippy::too_many_arguments)]
        fn check(
            table: &str,
            registry: &str,
            admitted: &BTreeSet<String>,
            excluded: &BTreeSet<String>,
            all_ravel: &BTreeSet<String>,
            ravel_for_table: &BTreeSet<String>,
            default_upstream: &BTreeSet<String>,
            registered: &BTreeSet<String>,
        ) {
            let overlap: Vec<&String> = admitted.intersection(excluded).collect();
            assert!(
                overlap.is_empty(),
                "{table}/{registry}: names in both admitted and excluded: {overlap:?}"
            );

            // The upstream slice of the admitted set (Ravel's own UDFs are not
            // upstream defaults) plus the excluded set must exactly cover the
            // default registrations. This is the drift guard.
            let admitted_upstream: BTreeSet<String> =
                admitted.difference(all_ravel).cloned().collect();
            let classified: BTreeSet<String> = admitted_upstream.union(excluded).cloned().collect();
            let unclassified: Vec<&String> = default_upstream.difference(&classified).collect();
            let stale: Vec<&String> = classified.difference(default_upstream).collect();
            assert!(
                unclassified.is_empty() && stale.is_empty(),
                "{table}/{registry}: allowlist/excluded drifted from the default registrations.\n  \
                 registered but unclassified (surface widened; admit it or add to the excluded list): {unclassified:?}\n  \
                 classified but not registered (stale entry to remove): {stale:?}"
            );

            // build_session must leave exactly the upstream admitted names plus
            // this table's own Ravel UDFs: nothing excluded survives, and the
            // per-table UDFs land on the right table.
            let expected: BTreeSet<String> =
                admitted_upstream.union(ravel_for_table).cloned().collect();
            let missing: Vec<&String> = expected.difference(registered).collect();
            let extra: Vec<&String> = registered.difference(&expected).collect();
            assert!(
                missing.is_empty() && extra.is_empty(),
                "{table}/{registry}: build_session registrations differ from the admitted set.\n  \
                 admitted but not registered (per-table UDF missing, or gate dropped it): {missing:?}\n  \
                 registered but not admitted (gate failed to remove it, or wrong table): {extra:?}"
            );
        }

        // Upstream default registrations, the reference every registry covers.
        let default = SessionContext::new();
        let dstate = default.state();
        let default_scalars = keys(dstate.scalar_functions().keys().cloned());
        let default_windows = keys(dstate.window_functions().keys().cloned());
        let default_aggregates = keys(dstate.aggregate_functions().keys().cloned());
        let default_tables = keys(dstate.table_functions().keys().cloned());

        let admitted_scalars = set(ADMITTED_SCALARS.iter());
        let excluded_scalars = set(EXCLUDED_SCALARS.iter());
        let admitted_windows = set(ADMITTED_WINDOWS.iter());
        let excluded_windows = set(EXCLUDED_WINDOWS.iter());
        let admitted_aggregates = set(ADMITTED_AGGREGATES.iter());
        let excluded_aggregates = set(crate::validate::EXCLUDED_AGGREGATES.iter());
        let admitted_tables = set(ADMITTED_TABLE_FUNCTIONS.iter());
        let excluded_tables = set(EXCLUDED_TABLE_FUNCTIONS.iter());

        // Every Ravel-added scalar UDF across all tables, and the per-table
        // subset. Non-scalar registries have no per-table addendum.
        let all_ravel_scalars = set(["label", "label_match", "has_word"].iter());
        let empty: BTreeSet<String> = BTreeSet::new();

        let store: Arc<dyn ravel_object_store::ObjectStoreBackend> = Arc::new(MemoryStore::new());

        for (table_label, table, ravel_scalars) in [
            (
                "metrics",
                metrics_table(&store),
                set(["label", "label_match"].iter()),
            ),
            ("logs", logs_table(&store), set(["has_word"].iter())),
            ("spans", spans_table(&store), empty.clone()),
        ] {
            let ctx = build_session(&SqlConfig::default(), test_pool(), table, false)
                .expect("session builds");
            let state = ctx.state();
            let reg_scalars = keys(state.scalar_functions().keys().cloned());
            let reg_windows = keys(state.window_functions().keys().cloned());
            let reg_aggregates = keys(state.aggregate_functions().keys().cloned());
            let reg_tables = keys(state.table_functions().keys().cloned());

            check(
                table_label,
                "scalar",
                &admitted_scalars,
                &excluded_scalars,
                &all_ravel_scalars,
                &ravel_scalars,
                &default_scalars,
                &reg_scalars,
            );
            check(
                table_label,
                "window",
                &admitted_windows,
                &excluded_windows,
                &empty,
                &empty,
                &default_windows,
                &reg_windows,
            );
            check(
                table_label,
                "aggregate",
                &admitted_aggregates,
                &excluded_aggregates,
                &empty,
                &empty,
                &default_aggregates,
                &reg_aggregates,
            );
            check(
                table_label,
                "table",
                &admitted_tables,
                &excluded_tables,
                &empty,
                &empty,
                &default_tables,
                &reg_tables,
            );
        }
    }

    /// Ravel's per-table UDFs survive the scalar gate: a logs session keeps
    /// `has_word`, a metrics session keeps `label`/`label_match`.
    ///
    /// They survive by membership in [`ADMITTED_SCALARS`], not by the
    /// deregistration loop running before registration, so this test does not
    /// pin that ordering: inverting the two leaves every assertion here
    /// passing. What it pins is the membership that makes the ordering safe to
    /// get wrong, which is why the membership is asserted directly below.
    #[test]
    fn per_table_scalar_udfs_survive_the_scalar_gate() {
        for name in ["label", "label_match", "has_word"] {
            assert!(
                ADMITTED_SCALARS.contains(&name),
                "{name} must stay in ADMITTED_SCALARS: the scalar gate spares \
                 Ravel's own UDFs by admitting them, and dropping one here \
                 makes the loop's position relative to per-table registration \
                 load-bearing"
            );
        }

        let store: Arc<dyn ravel_object_store::ObjectStoreBackend> = Arc::new(MemoryStore::new());

        let logs = build_session(
            &SqlConfig::default(),
            test_pool(),
            logs_table(&store),
            false,
        )
        .expect("logs session builds");
        assert!(
            logs.state().scalar_functions().contains_key("has_word"),
            "a logs session must keep has_word after the scalar gate"
        );

        let metrics = build_session(
            &SqlConfig::default(),
            test_pool(),
            metrics_table(&store),
            false,
        )
        .expect("metrics session builds");
        let metrics_state = metrics.state();
        let metrics_scalars = metrics_state.scalar_functions();
        assert!(
            metrics_scalars.contains_key("label"),
            "a metrics session must keep label after the scalar gate"
        );
        assert!(
            metrics_scalars.contains_key("label_match"),
            "a metrics session must keep label_match after the scalar gate"
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
