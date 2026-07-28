//! Security invariant 2: per-query, single-tenant `SessionContext`
//! (docs/arrow-datafusion-plan.md section 2, review F17).
//!
//! [`build_session`] constructs a complete, throwaway DataFusion session for
//! exactly one query: its own `SessionConfig`, its own `RuntimeEnv`, its own
//! memory pool, and one registered table -- the requesting tenant's
//! `samples` provider over that query's already-resolved `Snapshot`. Nothing
//! is cached, pooled, or reused across queries or tenants.
//!
//! This is the tenant-isolation mechanism, not a performance shortcut
//! (review F17: framing it as "context construction is cheap enough" invites
//! a later caching optimization that silently converts it into a
//! cross-tenant leak). If context construction ever proves too expensive,
//! the fix is cheaper construction; sharing state requires an ADR.
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
//! - The `avg` UDAF and the stddev/variance family (`stddev`, `var`,
//!   `stddev_pop`, `var_pop`, `covar_samp`, `covar_pop`, `corr`, and their
//!   aliases) are deregistered, backstopping the subset check in
//!   crate::validate.
//! - The `range`/`generate_series` table functions are deregistered
//!   (checkpoint review finding, not in the original design):
//!   `SessionContext::new_with_config_rt` calls DataFusion's
//!   `with_default_features()`, which registers these unconditionally --
//!   `EmptyObjectStoreRegistry` blocks reading anything, but a table
//!   function generates rows in memory and needs no store, so
//!   `SELECT count(*) FROM range(0, 1e18)` would otherwise reach the
//!   planner as a second, ungoverned data source alongside `samples`.
//!
//! Determinism (docs/arrow-datafusion-plan.md section 2 "Exactness"):
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
use datafusion::object_store::ObjectStore;
use datafusion::prelude::{SessionConfig, SessionContext};
use url::Url;

use crate::config::SqlConfig;
use crate::minmax::{total_order_max_udaf, total_order_min_udaf};
use crate::provider::RavelTableProvider;
use crate::udf::{label_match_udf, label_udf};

/// The single table name every SQL query addresses.
pub const SAMPLES_TABLE: &str = "samples";

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

/// Build a fresh, single-tenant `SessionContext` around `provider` with
/// `pool` installed as the query's memory pool.
///
/// The caller owns `pool` (typically a `TenantDelegatingPool` from
/// [`SqlConfig::query_pool`](crate::SqlConfig::query_pool)) so it can read
/// the reserved-byte counters after the query finishes.
pub fn build_session(
    config: &SqlConfig,
    pool: Arc<dyn MemoryPool>,
    provider: Arc<RavelTableProvider>,
) -> DFResult<SessionContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_object_store_registry(Arc::new(EmptyObjectStoreRegistry))
        .build_arc()?;

    let ctx = SessionContext::new_with_config_rt(session_config(config), runtime);

    ctx.register_udf(label_udf());
    ctx.register_udf(label_match_udf());
    // Backstop for the v1 subset check in crate::validate: even a syntactic
    // form the AST walk missed cannot resolve an avg accumulator here.
    ctx.deregister_udaf("avg");
    ctx.deregister_udaf("mean");
    // The stddev/variance family shares avg's excluded floating-mean property
    // (Welford's online algorithm, no bit-identical naive reference); reject in
    // crate::validate, deregister here so a missed syntactic form cannot
    // resolve the accumulator. Every registered spelling and alias is removed.
    for name in [
        "stddev",
        "stddev_samp",
        "stddev_pop",
        "var",
        "var_samp",
        "variance",
        "var_pop",
        "covar",
        "covar_samp",
        "covar_pop",
        "corr",
    ] {
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
    // `with_default_features()` (inside `new_with_config_rt` above)
    // registers these unconditionally; they generate rows in memory and so
    // are not blocked by `EmptyObjectStoreRegistry`. `samples` must be the
    // only table this session can query.
    ctx.deregister_udtf("range");
    ctx.deregister_udtf("generate_series");

    ctx.register_table(SAMPLES_TABLE, provider)?;
    Ok(ctx)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

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
