//! ravel-sql query configuration.
//!
//! The series/segment/sample/deadline budgets live in `ravel_query::EngineConfig`
//! (shared with the PromQL path). This ticket (B2) adds a per-query
//! byte budget for the DataFusion memory pool. `EngineConfig` lives in
//! ravel-query, which is out of this crate's scope and must stay free of
//! any SQL-only concern, so the byte budget lives here in a ravel-sql-local
//! [`SqlConfig`] that embeds `EngineConfig` rather than growing it (the ticket
//! leaves this call to the implementer; this is the choice made).
//!
//! `max_query_bytes` is consumed only when the query's DataFusion memory pool
//! is built ([`SqlConfig::query_pool`]). It is a measured
//! RecordBatch-byte budget, never a sample-count-derived figure: per-row
//! footprint is cardinality-dependent once labels materialize as columns, so a
//! sample cap cannot stand in for a byte cap.

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use ravel_query::EngineConfig;
use ravel_types::accounting::QueryAccounting;

use crate::memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};

/// Default per-query RecordBatch byte budget: 256 MiB. This is the shipped
/// default, not a guess awaiting a number: an operator overrides it per process
/// with `--sql-max-query-bytes` (ADR-0088). Changing the compiled-in default
/// itself is a separate, measurement-backed follow-up; this value stays exactly
/// as it was so behavior is unchanged when the flag is unset.
pub const DEFAULT_MAX_QUERY_BYTES: usize = 256 * 1024 * 1024;

/// Default width threshold for TopK late materialization (ADR-0774): a `logs`
/// scan under a TopK must project more than eight columns BEYOND the ones the
/// filter and the sort read before the rewrite fires.
///
/// The rewrite trades decoding every projected column of every surviving block
/// for decoding the filter's and sort's columns of every surviving block plus
/// `k` extra block reads. Below a handful of surplus columns the `k` reads are
/// not obviously repaid, and the shapes this exists for are not marginal: the
/// measured statement on the ClickBench reference tenant (issue #680) projects
/// 105 columns and sorts and filters on two, i.e. 103 surplus columns.
pub const DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS: usize = 8;

/// Per-query ravel-sql configuration: the shared engine budgets plus the
/// SQL-only per-query memory-pool byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlConfig {
    /// Series/segment/sample/deadline budgets shared with the PromQL path.
    pub engine: EngineConfig,
    /// Ceiling, in bytes, on the query's DataFusion memory pool. Fed by
    /// measured `RecordBatch` sizes, never a sample count.
    pub max_query_bytes: usize,
    /// Whether an exact-typed query is allowed to repartition its final
    /// aggregation (ADR-0094 decision 4, amended 2026-08-26 by issue #741).
    /// Process-wide, default `true`: a
    /// per-query classification (ADR-0094 decision 1) only ever flips
    /// DataFusion's `repartition_aggregations` on when this is `true` *and*
    /// every aggregate expression and GROUP BY key in the query is provably
    /// order/partition-independent (`count`, `count distinct`, and
    /// `sum`/`min`/`max` over non-float input; never `avg` or any float
    /// accumulator or key). A non-exact-typed plan keeps the single-partition
    /// final byte for byte whatever this is set to. `false` is the operator
    /// opt-out, restoring the pre-amendment single-partition final for every
    /// query. Set once at server startup (`services/ravel-server`); no
    /// live-reload, like every other field here.
    pub parallel_final_aggregation: bool,
    /// Whether the partial aggregation stage may give up early on a
    /// high-cardinality group key (issue #680, ADR-0102 decision 2 amendment).
    /// Default `true`.
    ///
    /// DataFusion builds one partial hash table per input partition and merges
    /// them in a single final stage, so for a key whose distinct values all
    /// appear in every partition the pre-final state is roughly
    /// `partitions x distinct` entries. Measured on the `logs` table
    /// (`ravel_bench::groupby_scaling::run_distinct`), a 32-partition
    /// `COUNT(DISTINCT key)` peaked at 5x to 16x the single-partition peak for
    /// the same key. DataFusion's own probe already bounds this, but its stock
    /// thresholds (0.8 ratio after 100,000 probe rows) rarely fire on Ravel's
    /// partitions.
    ///
    /// When `true`, [`crate::session_config`] tightens both probe thresholds
    /// (see [`crate::SKIP_PARTIAL_AGGREGATION_PROBE_ROWS`] and
    /// [`crate::SKIP_PARTIAL_AGGREGATION_PROBE_RATIO`]).
    /// This changes where aggregation state lives, never a result: the final
    /// stage computes the same groups over the same rows either way.
    ///
    /// `false` restores DataFusion's stock thresholds. It is the operator
    /// escape hatch for a workload whose partial stage genuinely reduces well
    /// and would rather spend memory than push rows to the final stage, and it
    /// is the "before" side of the regression test in
    /// `tests/skip_partial_aggregation.rs`.
    pub skip_partial_aggregation: bool,
    /// Width threshold for TopK late materialization on the `logs` table
    /// (ADR-0774, issue #774). `Some(n)` installs
    /// [`crate::TopKLateMaterialization`] and lets it fire on a scan projecting
    /// more than `n` columns beyond what its TopK's filter and sort read;
    /// `None` does not install the rule at all, which is the "before" side of
    /// `tests/logs_topk_late_materialization.rs`.
    ///
    /// Default [`DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS`]. Set once at
    /// server startup, like every other field here.
    ///
    /// The rewrite is invisible to results: the same rows in the same order
    /// under the same schema, with the wide columns decoded for the `k`
    /// surviving rows instead of for every row. So this is a cost knob, never
    /// a correctness one.
    pub late_materialization_extra_columns: Option<usize>,
}

impl Default for SqlConfig {
    fn default() -> Self {
        SqlConfig {
            engine: EngineConfig::default(),
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            parallel_final_aggregation: true,
            skip_partial_aggregation: true,
            late_materialization_extra_columns: Some(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS),
        }
    }
}

impl From<EngineConfig> for SqlConfig {
    fn from(engine: EngineConfig) -> Self {
        SqlConfig {
            engine,
            ..SqlConfig::default()
        }
    }
}

impl SqlConfig {
    /// Build the query's DataFusion memory pool: a [`TenantDelegatingPool`]
    /// capped at `max_query_bytes` that forwards every grow/shrink to
    /// `tenant`. Install it on the query's `RuntimeEnv` via
    /// `RuntimeEnvBuilder::with_memory_pool` (the endpoint's job in B3); the
    /// scan then registers its `MemoryConsumer` against whatever pool the
    /// `TaskContext` carries.
    ///
    /// Returns the pool paired with the [`CeilingBreach`] it trips: the pool
    /// goes onto the `RuntimeEnv`, and the breach travels with the query's
    /// stream so a `grow` that overshoots either ceiling aborts the query at
    /// its next poll. The two are created together so the caller
    /// cannot install a pool whose breach nothing observes.
    ///
    /// `accounting` is the calling query's [`QueryAccounting`] handle
    /// (ADR-0044); the pool reports this query's reserved-bytes high-water
    /// mark into it on every grow, feeding `peak_intermediate_bytes`.
    pub fn query_pool(
        &self,
        tenant: Arc<TenantMemoryAccountant>,
        accounting: QueryAccounting,
    ) -> (Arc<dyn MemoryPool>, Arc<CeilingBreach>) {
        let breach = CeilingBreach::new();
        let pool = Arc::new(TenantDelegatingPool::new(
            self.max_query_bytes,
            tenant,
            Arc::clone(&breach),
            accounting,
        ));
        (pool, breach)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0094 amendment (2026-08-26, issue #741): a production-scale
    /// measurement on the 8,424-object ClickBench tenant (issue #680) found the
    /// single-partition final aggregate exhausted an 8 GiB per-query pool at 32
    /// scan partitions on high-cardinality GROUP BY / COUNT(DISTINCT) (nine
    /// statements failed), while repartitioning the exact-typed final ran those
    /// in 44-50 s with no determinism cost. The default is therefore `true`.
    /// This pins that decision -- a change that flips it back to `false` is a
    /// change to that ClickBench outcome and should update this assertion in the
    /// same change, not discover it broken in an unrelated PR.
    #[test]
    fn parallel_final_aggregation_defaults_to_true() {
        assert!(SqlConfig::default().parallel_final_aggregation);
    }

    /// Issue #680: the early give-up is on by default. It is the fix for a
    /// measured `partitions x distinct` blow-up on high-cardinality
    /// aggregates, not an opt-in tuning knob, so a change that flips this
    /// default off is a change to the ClickBench failure mode and should say
    /// so here.
    #[test]
    fn skip_partial_aggregation_defaults_to_true() {
        assert!(SqlConfig::default().skip_partial_aggregation);
    }

    /// ADR-0774: the rewrite is on by default, at eight surplus columns. A
    /// change that turns it off, or moves the threshold, changes which
    /// ClickBench shapes finish at all (`SELECT * ... ORDER BY ts LIMIT 10`
    /// exceeded a 900 s deadline without it) and belongs in the same change as
    /// this assertion, not discovered broken elsewhere.
    #[test]
    fn late_materialization_defaults_to_eight_extra_columns() {
        assert_eq!(
            SqlConfig::default().late_materialization_extra_columns,
            Some(8)
        );
        assert_eq!(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS, 8);
    }
}
