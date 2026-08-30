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

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use ravel_query::EngineConfig;
use ravel_types::accounting::QueryAccounting;

use crate::memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};

/// Environment variable naming the ephemeral spill scratch directory
/// (ADR-0954). Read only as a default source by [`SpillConfig::from_env`]; the
/// `SqlConfig::spill` field is the source of truth. Set alongside
/// [`ENV_SPILL_MAX_BYTES`]: both must be present for spill to be enabled.
pub const ENV_SPILL_DIR: &str = "RAVEL_SQL_SPILL_DIR";

/// Environment variable naming the per-query spill scratch byte quota
/// (ADR-0954), parsed as a base-10 `usize`. Read only as a default source by
/// [`SpillConfig::from_env`]. Set alongside [`ENV_SPILL_DIR`].
pub const ENV_SPILL_MAX_BYTES: &str = "RAVEL_SQL_SPILL_MAX_BYTES";

/// Explicit configuration for bounded ephemeral spill (ADR-0954, issue #954).
///
/// Both fields are required for spill to be enabled at all: a `SqlConfig`
/// carries `Some(SpillConfig)` only when a deployment has chosen both a
/// scratch directory and a per-query byte quota. An absent `SqlConfig::spill`
/// (the default) keeps the disk manager `Disabled` and reproduces today's
/// behaviour byte for byte, which is the no-spill deployment profile
/// (requirement 9 of #954).
///
/// Spill is per-query ephemeral execution state: it is never committed, never
/// read back for recovery, and cleaned up when the query's session drops (on
/// completion, error, or cancellation). Startup orphan cleanup and
/// node-wide/tenant quotas are explicit follow-up work, out of scope here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillConfig {
    /// The directory bounded spill scratch files are written under. DataFusion
    /// creates a per-session temporary subdirectory inside it, removed when the
    /// query's `RuntimeEnv` drops. Must exist (or be creatable) and be
    /// writable; otherwise a spilling query fails with
    /// [`crate::SqlError::SpillUnavailable`].
    pub dir: PathBuf,
    /// The per-query scratch byte quota, wired onto the query's disk manager as
    /// `max_temp_directory_size`. A spill write that pushes a query's scratch
    /// use over this fails with [`crate::SqlError::SpillBudgetExhausted`],
    /// independently of the memory budget.
    pub max_bytes: usize,
}

impl SpillConfig {
    /// Build a [`SpillConfig`] from [`ENV_SPILL_DIR`] and [`ENV_SPILL_MAX_BYTES`].
    ///
    /// Returns `Some` only when BOTH variables are present and the byte quota
    /// parses to a non-zero `usize`; any other combination (either unset, an
    /// unparseable or zero quota) returns `None`, so a partial or malformed
    /// environment leaves spill disabled rather than half-configured. The
    /// `SqlConfig::spill` field is the source of truth; this only supplies a
    /// default a deployment can fold in, and [`SqlConfig::default`] never reads
    /// the environment (so its behaviour is deterministic and off).
    pub fn from_env() -> Option<SpillConfig> {
        let dir = std::env::var_os(ENV_SPILL_DIR)?;
        if dir.is_empty() {
            return None;
        }
        let max_bytes: usize = std::env::var(ENV_SPILL_MAX_BYTES).ok()?.parse().ok()?;
        if max_bytes == 0 {
            return None;
        }
        Some(SpillConfig {
            dir: PathBuf::from(dir),
            max_bytes,
        })
    }
}

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

/// Under-count of DataFusion 54's `GroupValues::size()` against the real
/// hashbrown allocation of the group-key table (issue #740, finding 2).
/// `size()` charges `capacity() * entry_size`, where `capacity()` is the
/// table's usable slot count at the 7/8 load factor, and it counts no control
/// bytes. The real allocation is `buckets * entry_size` (with
/// `buckets = capacity / (7/8)`) plus one control byte per bucket and the
/// group width, so for an Int64 group table (16-byte entry: value plus group
/// index) the ratio is `(8/7) * ((entry_size + 1) / entry_size) = 8*17/(7*16)
/// = 1.2143`, which the #740 trace saw as 570 MB real against 470 MB reported
/// for 17M groups. Rounded up to two decimals so the factor is an upper
/// bound on the under-count, not an estimate of it.
pub const GROUP_VALUES_UNDERCOUNT_FACTOR: f64 = 1.22;

/// Transient over-allocation while a hashbrown group table doubles (issue
/// #740, finding 3). A grow allocates the new table before freeing the old,
/// so a doubling holds `old + new = steady/2 + steady = 1.5 * steady` at its
/// peak, and DataFusion grows the pool reservation only after the resize
/// completes (`row_hash.rs` ~745/793), so the pool holds the pre-batch figure
/// while that transient peak is live. The peak is bounded at 1.5x the settled
/// real size because hashbrown never grows by more than doubling.
pub const GROUP_VALUES_RESIZE_TRANSIENT_FACTOR: f64 = 1.5;

/// Combined compensation applied to a reported `GroupValues::size()` figure to
/// bound the real peak the pool must survive: the under-count times the resize
/// transient, `1.22 * 1.5 = 1.83`. Both defects are upstream in DataFusion 54
/// (documented in the ADR-0102 amendment for #740); this crate cannot fix
/// either from outside DataFusion, so it compensates its own ceiling math by
/// this factor rather than trusting the reported figure. See
/// [`compensated_group_values_ceiling`].
pub const GROUP_VALUES_CEILING_COMPENSATION: f64 =
    GROUP_VALUES_UNDERCOUNT_FACTOR * GROUP_VALUES_RESIZE_TRANSIENT_FACTOR;

/// Inflate a reported `GroupValues::size()` estimate to an upper bound on the
/// real hashbrown peak, by [`GROUP_VALUES_CEILING_COMPENSATION`]. A caller
/// sizing an aggregate stage against the memory budget must compare the budget
/// to this, not to the raw `size()`, or a table that reports fitting will in
/// fact allocate up to 1.83x that and overrun (issue #740, findings 2 and 3).
pub fn compensated_group_values_ceiling(reported_size: usize) -> usize {
    ((reported_size as f64) * GROUP_VALUES_CEILING_COMPENSATION).ceil() as usize
        + GROUP_VALUES_FIXED_OVERHEAD_CEILING
}

/// Control-group bytes hashbrown allocates alongside the buckets that
/// `GroupValues::size()` counts in neither its capacity nor its entry width.
/// Unlike the per-bucket control byte, which is a fixed fraction of the
/// reported figure and is therefore covered by the multiplicative
/// compensation, this part does not scale, so a purely multiplicative ceiling
/// under-bounds every small table.
const GROUP_VALUES_FIXED_OVERHEAD_BYTES: usize = 16;

/// [`GROUP_VALUES_FIXED_OVERHEAD_BYTES`] carried through the same resize
/// transient the multiplicative factor models, so the sum is an upper bound at
/// every table size rather than only asymptotically. Without it the ceiling is
/// below the modelled peak for every table under roughly 512 buckets: at 8
/// buckets the reported figure is 112, the real allocation 152, the modelled
/// peak 228, and the multiplicative ceiling alone returns 205.
const GROUP_VALUES_FIXED_OVERHEAD_CEILING: usize =
    (GROUP_VALUES_FIXED_OVERHEAD_BYTES as f64 * GROUP_VALUES_RESIZE_TRANSIENT_FACTOR) as usize + 1;

/// Per-query ravel-sql configuration: the shared engine budgets plus the
/// SQL-only per-query memory-pool byte budget.
///
/// No longer `Copy`: [`SqlConfig::spill`] holds an owned [`PathBuf`]. It stays
/// `Clone`, and the executor clones it into each query's providers.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Bounded ephemeral spill configuration (ADR-0954, issue #954). `None`
    /// (the default) keeps the disk manager `Disabled` and reproduces today's
    /// behaviour byte for byte: a query over its memory budget fails with a
    /// typed [`crate::SqlError::ResourcesExhausted`], never a silent spill.
    ///
    /// `Some(SpillConfig)` enables spill for a query ONLY when the query is
    /// also exactness-preserving under spill (the executor's eligibility
    /// predicate; an ineligible or off query still gets the disabled path).
    /// Both a directory and a byte quota must be set, which is what the
    /// [`SpillConfig`] type enforces. Set once at server startup, like every
    /// other field here; a deployment can source it from the environment via
    /// [`SpillConfig::from_env`] without a code change, but this field is the
    /// source of truth.
    pub spill: Option<SpillConfig>,
}

impl Default for SqlConfig {
    fn default() -> Self {
        SqlConfig {
            engine: EngineConfig::default(),
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            parallel_final_aggregation: true,
            skip_partial_aggregation: true,
            late_materialization_extra_columns: Some(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS),
            // Off by default: unset spill means today's disabled disk manager,
            // byte for byte (requirement 9 of #954). `default()` never reads
            // the environment, so it is deterministic regardless of process
            // configuration; a deployment folds in `SpillConfig::from_env()`
            // explicitly.
            spill: None,
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
    /// ADR-0954 / requirement 9: spill is OFF by default. An unset
    /// `SqlConfig::spill` is the no-spill deployment profile and keeps the disk
    /// manager `Disabled` in `build_session`, reproducing today's behaviour
    /// byte for byte. A change that defaults spill on is a change to that
    /// guarantee and belongs in the same change as this assertion.
    #[test]
    fn spill_defaults_to_off() {
        assert!(SqlConfig::default().spill.is_none());
    }

    #[test]
    fn late_materialization_defaults_to_eight_extra_columns() {
        assert_eq!(
            SqlConfig::default().late_materialization_extra_columns,
            Some(8)
        );
        assert_eq!(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS, 8);
    }
}
