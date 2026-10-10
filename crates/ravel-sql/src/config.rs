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

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
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

/// Default `LIMIT` ceiling for bounded top-k grouped aggregation (issue
/// #1402): a `GROUP BY ... ORDER BY max(col) DESC LIMIT k` keeps its aggregate
/// state bounded by `k` only while `k` is at or below this.
///
/// The rewrite replaces one accumulator per distinct group with a priority map
/// of `k` groups per aggregate stage, so the state it retains is `k` times the
/// per-group state times the number of stages (a partial per scan partition
/// plus one final). At 1,024 that is a few tens of thousands of retained
/// groups against a group table that the statements this exists for grow to
/// 10^8, which is the ratio the bound is worth having. Above it the priority
/// map stops being a bound and becomes the group table with a heap bolted on,
/// and its per-row comparison against the k-th best is pure overhead. The
/// number is a ceiling on where the trade stays clearly positive, not a tuned
/// optimum: the measured statement asks for `LIMIT 10`, three orders of
/// magnitude below it.
pub const DEFAULT_BOUNDED_TOPK_MAX_LIMIT: usize = 1024;

/// Under-count of DataFusion 55's `GroupValues::size()` against the real
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
/// transient, `1.22 * 1.5 = 1.83`. Both defects are upstream in DataFusion 55
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

/// Environment variable naming the directory under which a query may create
/// its ephemeral spill scratch (ADR-0954). Read by
/// [`SqlConfig::with_spill_resolved`] and [`SpillConfig::from_env`]; the
/// [`SqlConfig::spill`] field is the source of truth and this only supplies
/// its default when the caller left it unset.
pub const ENV_SPILL_DIR: &str = "RAVEL_SQL_SPILL_DIR";

/// Environment variable carrying the per-query scratch byte quota, a positive
/// decimal integer. See [`ENV_SPILL_DIR`].
pub const ENV_SPILL_MAX_BYTES: &str = "RAVEL_SQL_SPILL_MAX_BYTES";

/// Subdirectory of `--cache-dir` every process's spill root lives under
/// (ADR-0954 amendment, issue #2416): `<cache-dir>/sql-spill/<instance-id>`.
/// Shared with `crate::spill`'s ownership lock and startup sweep, which must
/// agree with [`cache_spill_dir`] on exactly this path for a given
/// `(cache_dir, instance_id)`.
pub const SQL_SPILL_SUBDIR: &str = "sql-spill";

/// The per-process spill root under a configured `--cache-dir`:
/// `<cache_dir>/sql-spill/<instance_id>`. `instance_id` is whatever identity
/// the process already carries (ADR-0954 amendment, issue #2416); this
/// function does not mint one.
pub fn cache_spill_dir(cache_dir: &Path, instance_id: &str) -> PathBuf {
    cache_dir.join(SQL_SPILL_SUBDIR).join(instance_id)
}

/// Bytes available to a non-privileged process on the volume backing `path`:
/// [`available_bytes`] of its `statvfs(2)`.
pub fn measure_free_bytes(path: &Path) -> std::io::Result<u64> {
    Ok(available_bytes(&rustix::fs::statvfs(path)?))
}

/// `f_bavail * f_frsize`: the blocks a non-privileged process may still
/// allocate, in bytes. Not `f_bfree`, which also counts the blocks reserved
/// for root, and not `f_blocks`, the volume's size.
fn available_bytes(stat: &rustix::fs::StatVfs) -> u64 {
    stat.f_bavail.saturating_mul(stat.f_frsize)
}

/// The multiple of the process memory budget that caps a `--cache-dir`-derived
/// spill ceiling (ADR-0954 amendment, issue #2416): see
/// [`derive_spill_max_bytes`].
pub const DERIVED_SPILL_MAX_BYTES_MEMORY_MULTIPLE: u64 = 4;

/// The floor under a `--cache-dir`-derived spill ceiling (ADR-0954 amendment,
/// issue #2416): see [`derive_spill_max_bytes`].
pub const DERIVED_SPILL_MAX_BYTES_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

/// Derive a `--cache-dir` spill ceiling from the volume's free bytes at
/// startup and the process memory budget (ADR-0954 amendment, issue #2416):
/// half the free space, capped at
/// [`DERIVED_SPILL_MAX_BYTES_MEMORY_MULTIPLE`] times the memory budget, and
/// floored at [`DERIVED_SPILL_MAX_BYTES_FLOOR_BYTES`]. Half, not all, of free
/// space so spill leaves room for whatever else shares the volume (another
/// process, the OS itself). The memory-budget cap keeps the derived ceiling
/// from outgrowing what the process's spilling queries could plausibly need.
///
/// `None`, spill off, when half the free space is below the floor: a ceiling
/// the volume cannot hold would let a spilling query write until the volume is
/// full. The floor therefore only ever raises a memory-budget cap below it,
/// never half of free space.
pub fn derive_spill_max_bytes(free_bytes: u64, memory_budget_bytes: u64) -> Option<u64> {
    let half_free = free_bytes / 2;
    if half_free < DERIVED_SPILL_MAX_BYTES_FLOOR_BYTES {
        return None;
    }
    let memory_cap = memory_budget_bytes.saturating_mul(DERIVED_SPILL_MAX_BYTES_MEMORY_MULTIPLE);
    Some(
        half_free
            .min(memory_cap)
            .max(DERIVED_SPILL_MAX_BYTES_FLOOR_BYTES),
    )
}

/// The inputs a `--cache-dir`-configured deployment resolves a
/// [`SpillConfig`] from, when neither half of the env pair names a
/// directory (ADR-0954 amendment, issue #2416). See [`SpillConfig::resolve`].
pub struct CacheDirSpill<'a> {
    /// The configured `--cache-dir`.
    pub cache_dir: &'a Path,
    /// This process's own instance identity (not minted here).
    pub instance_id: &'a str,
    /// Free bytes on the volume backing `cache_dir`, measured once at
    /// startup ([`measure_free_bytes`]).
    pub free_bytes: u64,
    /// The most the read cache's disk tier under the same `cache_dir` may
    /// hold. That tier can still grow after `free_bytes` is measured, so its
    /// whole bound is subtracted from `free_bytes` before the ceiling is
    /// derived.
    pub read_cache_bytes: u64,
    /// The process memory budget ([`derive_spill_max_bytes`]'s cap input).
    pub memory_budget_bytes: u64,
}

impl CacheDirSpill<'_> {
    /// The free bytes a derived spill ceiling is taken from: `free_bytes`
    /// less `read_cache_bytes`, saturating at zero.
    pub fn spillable_free_bytes(&self) -> u64 {
        self.free_bytes.saturating_sub(self.read_cache_bytes)
    }
}

/// Both halves of the spill configuration. Spill is enabled for a query only
/// when this whole struct is present AND the query's plan is exactness-eligible
/// (`crate::executor`'s spill eligibility predicate); either half missing means
/// [`DiskManagerMode::Disabled`](datafusion::execution::disk_manager::DiskManagerMode::Disabled)
/// and today's behavior byte for byte.
///
/// A directory alone would leave the 100 GB DataFusion default ceiling in
/// place, and a quota alone has nowhere to write, so neither is admitted on its
/// own: the two are one value, not two independent knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillConfig {
    /// Directory under which each query creates, and removes, its own scratch
    /// subdirectory. It must already exist and be writable; a query that finds
    /// otherwise fails with [`crate::SqlError::SpillUnavailable`] rather than
    /// creating anything outside it.
    pub dir: PathBuf,
    /// Ceiling, in bytes, on the scratch all of one executor's queries may
    /// hold on disk at once. Each query granted spill reserves its own cap
    /// out of it, `max_bytes` or whatever remains if that is less, and runs
    /// with spill disabled when less than
    /// [`MIN_SPILL_RESERVATION_BYTES`](crate::spill::MIN_SPILL_RESERVATION_BYTES)
    /// remains. The cap is enforced by DataFusion's disk manager
    /// (`max_temp_directory_size`), which counts bytes written to spill files,
    /// not bytes decoded from them. Exceeding it is
    /// [`crate::SqlError::SpillBudgetExhausted`], never a partial result.
    pub max_bytes: u64,
}

/// A [`SpillConfig`] could not be read from the environment. Loud on purpose:
/// a half-set or unparseable spill configuration silently selects a different
/// execution behavior than the operator asked for, and this codebase does not
/// let a default that selects which resources a query touches stay silent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpillConfigError {
    /// One of the two variables is set and the other is not.
    #[error(
        "RAVEL_SQL_SPILL_DIR and RAVEL_SQL_SPILL_MAX_BYTES must both be set to enable \
         SQL spill, or both be unset to disable it; only one of the two is set"
    )]
    Incomplete,
    /// `RAVEL_SQL_SPILL_MAX_BYTES` is not a positive decimal integer.
    #[error("RAVEL_SQL_SPILL_MAX_BYTES must be a positive decimal number of bytes, got {value:?}")]
    BadQuota { value: String },
    /// `RAVEL_SQL_SPILL_DIR` is set to an empty string.
    #[error("RAVEL_SQL_SPILL_DIR must name a directory, but is set to an empty string")]
    EmptyDir,
}

impl SpillConfig {
    /// Read both variables. `Ok(None)` when neither is set, which is the
    /// no-spill deployment profile and the compiled-in default.
    ///
    /// Set-but-invalid is an error, not a fall back to `None`: turning a typo
    /// in a deployment's spill quota into "spill silently stays off" is exactly
    /// the silent-default failure the measurement-discipline rules forbid.
    pub fn from_env() -> Result<Option<SpillConfig>, SpillConfigError> {
        let dir = std::env::var_os(ENV_SPILL_DIR);
        let quota = std::env::var_os(ENV_SPILL_MAX_BYTES);
        Self::resolve(dir.as_deref(), quota.as_deref(), None)
    }

    /// The full three-source precedence (ADR-0954 amendment, issue #2416):
    ///
    /// 1. `env_dir` and `env_quota` both set: used directly, unchanged from
    ///    [`SpillConfig::from_env`]'s original (pre-`--cache-dir`) behavior.
    ///    Wins outright over `cache_dir`.
    /// 2. Neither of the above, but `cache_dir` is `Some`: the spill root is
    ///    derived from it (`crate::config::cache_spill_dir`), and the
    ///    ceiling is `env_quota` when it alone is set (the env quota overrides
    ///    the derived ceiling under a `--cache-dir` deployment), else
    ///    [`derive_spill_max_bytes`] of `cache_dir`'s
    ///    [`CacheDirSpill::spillable_free_bytes`] and memory budget. When
    ///    that derivation finds too little free space it is
    ///    `Ok(None)`: spill off.
    /// 3. Neither 1 nor 2: `Ok(None)`, the no-spill default.
    ///
    /// `env_dir` set with `env_quota` unset is always
    /// [`SpillConfigError::Incomplete`], with or without `cache_dir`: there is
    /// no symmetric "derive a quota" fallback for a bare directory, only for a
    /// bare quota under a configured `--cache-dir`.
    pub fn resolve(
        env_dir: Option<&OsStr>,
        env_quota: Option<&OsStr>,
        cache_dir: Option<CacheDirSpill<'_>>,
    ) -> Result<Option<SpillConfig>, SpillConfigError> {
        match (env_dir, env_quota) {
            (Some(dir), Some(quota)) => return Ok(Some(Self::parse_pair(dir, quota)?)),
            (Some(_), None) => return Err(SpillConfigError::Incomplete),
            (None, Some(quota)) => {
                let Some(cache_dir) = cache_dir else {
                    return Err(SpillConfigError::Incomplete);
                };
                let max_bytes = Self::parse_quota(quota)?;
                return Ok(Some(SpillConfig {
                    dir: cache_spill_dir(cache_dir.cache_dir, cache_dir.instance_id),
                    max_bytes,
                }));
            }
            (None, None) => {}
        }
        let Some(cache_dir) = cache_dir else {
            return Ok(None);
        };
        let Some(max_bytes) = derive_spill_max_bytes(
            cache_dir.spillable_free_bytes(),
            cache_dir.memory_budget_bytes,
        ) else {
            return Ok(None);
        };
        Ok(Some(SpillConfig {
            dir: cache_spill_dir(cache_dir.cache_dir, cache_dir.instance_id),
            max_bytes,
        }))
    }

    fn parse_quota(quota: &OsStr) -> Result<u64, SpillConfigError> {
        let quota = quota.to_string_lossy().trim().to_string();
        quota
            .parse()
            .ok()
            .filter(|bytes| *bytes > 0)
            .ok_or(SpillConfigError::BadQuota { value: quota })
    }

    fn parse_pair(dir: &OsStr, quota: &OsStr) -> Result<SpillConfig, SpillConfigError> {
        if dir.is_empty() {
            return Err(SpillConfigError::EmptyDir);
        }
        let max_bytes = Self::parse_quota(quota)?;
        Ok(SpillConfig {
            dir: PathBuf::from(dir),
            max_bytes,
        })
    }
}

/// Per-query ravel-sql configuration: the shared engine budgets plus the
/// SQL-only per-query memory-pool byte budget.
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
    /// `LIMIT` ceiling for bounded top-k grouped aggregation (issue #1402).
    /// `Some(k)` installs [`crate::BoundedTopKAggregate`] and lets it bound a
    /// grouped aggregate whose only consumer is a top-k sort of at most `k`
    /// rows; `None` does not install the rule at all, which is the operator
    /// opt-out and the "before" side of `tests/bounded_topk_aggregate.rs`.
    ///
    /// Default [`DEFAULT_BOUNDED_TOPK_MAX_LIMIT`]. Set once at server startup,
    /// like every other field here.
    ///
    /// The rewrite is invisible to results, and narrowly so: it fires only for
    /// a value-selective ordering aggregate (`max` descending, `min`
    /// ascending) over a non-float input, where a group the bounded map evicts
    /// provably could not have been in the answer. It does NOT fire for
    /// `count` or `sum`, which cannot be pruned exactly at all; see the
    /// `crate::bounded_topk` module docs for the counterexample.
    pub bounded_topk_max_limit: Option<usize>,
    /// Bounded ephemeral spill scratch (ADR-0954). `None`, the default, means
    /// the disk manager stays
    /// [`Disabled`](datafusion::execution::disk_manager::DiskManagerMode::Disabled)
    /// exactly as ADR-0102 decision 3 left it: a query over its memory budget
    /// fails typed, nothing is written to local disk, and this crate behaves
    /// byte for byte as it did before spill existed. That is the no-spill
    /// deployment profile, and it is the compiled-in default so this whole
    /// mechanism is inert until an operator opts in.
    ///
    /// `Some` arms spill, but does not by itself grant it to a query: the
    /// query's plan must also pass the exactness eligibility predicate
    /// (`crate::executor::plan_is_spill_eligible`). An ineligible plan gets the
    /// disabled disk manager and today's typed refusal, whatever this is set
    /// to.
    ///
    /// A query that IS granted spill runs its final aggregation
    /// single-partitioned even when [`SqlConfig::parallel_final_aggregation`]
    /// is on: the eligibility predicate classifies logical nodes, and an
    /// enabled disk manager would also let the `RepartitionExec` that knob
    /// introduces spill unclassified. See `crate::session`'s module doc.
    ///
    /// This field is the source of truth. [`SqlConfig::with_spill_resolved`]
    /// fills it from [`ENV_SPILL_DIR`]/[`ENV_SPILL_MAX_BYTES`] and a cache
    /// directory only when it is still `None`, so an explicit setting is never
    /// overridden by the environment (`--sql-spill off` clears it).
    pub spill: Option<SpillConfig>,
    /// Whether a logs scan publishes its per-segment scan timeline
    /// (`seg_open_start_offset`/`seg_open_ready_offset`/`seg_done_offset`,
    /// folded into `SqlStats.scan_timing.segments`). Default `false`.
    ///
    /// The timeline is O(segments scanned): a new labelled metric per
    /// segment per partition, at three points each, on top of the O(1) sums
    /// and counts (`open_elapsed`, `decode_build_elapsed`, `segments_opened`,
    /// the min/max pairs) that stay unconditional regardless of this flag. A
    /// production logs query over thousands of segments has no reader for the
    /// per-segment rows, so this is off by default; `ravel-bench`'s
    /// `sql_latency` reporter is the one caller that turns it on. Set once at
    /// server startup, like every other field here.
    pub segment_timing: bool,
}

impl Default for SqlConfig {
    fn default() -> Self {
        SqlConfig {
            engine: EngineConfig::default(),
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            parallel_final_aggregation: true,
            skip_partial_aggregation: true,
            late_materialization_extra_columns: Some(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS),
            bounded_topk_max_limit: Some(DEFAULT_BOUNDED_TOPK_MAX_LIMIT),
            // Spill off. See the field doc: this is requirement 9 of #954 (a
            // no-spill deployment profile) and it is what makes enabling spill
            // an operator decision rather than a version upgrade.
            spill: None,
            segment_timing: false,
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
    /// Fill [`SqlConfig::spill`] from the full `--cache-dir`-aware precedence
    /// (ADR-0954 amendment, issue #2416) when it is still `None`:
    /// [`SpillConfig::resolve`] over the current environment and
    /// `cache_dir`. `sql_spill_off` is `--sql-spill off` (ADR-0954
    /// requirement 9's no-spill profile): it forces `spill` to `None`
    /// outright, regardless of the environment, `cache_dir`, or any prior
    /// value of this field, because it is the deployment's own declared
    /// refusal to spill.
    ///
    /// Call once at process startup, next to the other startup-only knobs on
    /// this struct; nothing here live-reloads.
    pub fn with_spill_resolved(
        mut self,
        sql_spill_off: bool,
        cache_dir: Option<CacheDirSpill<'_>>,
    ) -> Result<Self, SpillConfigError> {
        if sql_spill_off {
            self.spill = None;
            return Ok(self);
        }
        if self.spill.is_none() {
            let dir = std::env::var_os(ENV_SPILL_DIR);
            let quota = std::env::var_os(ENV_SPILL_MAX_BYTES);
            self.spill = SpillConfig::resolve(dir.as_deref(), quota.as_deref(), cache_dir)?;
        }
        Ok(self)
    }

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
    ///
    /// The pool holds a grouped hash aggregate's released bytes until its
    /// stream ends when that stream's memory consumer cannot spill, which
    /// DataFusion decides per stream from the aggregate mode and whether the
    /// session's runtime has a disk.
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
#[allow(clippy::expect_used)]
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
    /// ADR-0954: spill is off unless an operator configures both halves. A
    /// change that flips this default is a change to every deployment's disk
    /// behavior and to the ADR-0102 decision 3 refusal path, so it belongs in
    /// the same change as this assertion.
    #[test]
    fn spill_defaults_to_off() {
        assert_eq!(SqlConfig::default().spill, None);
    }

    /// Both halves are required. A directory with no quota would leave
    /// DataFusion's 100 GB default ceiling in place, and a quota with no
    /// directory has nowhere to write; neither is a valid enable.
    #[test]
    fn a_half_set_environment_is_an_error_not_a_silent_disable() {
        assert_eq!(
            SpillConfigError::Incomplete.to_string(),
            "RAVEL_SQL_SPILL_DIR and RAVEL_SQL_SPILL_MAX_BYTES must both be set to enable \
             SQL spill, or both be unset to disable it; only one of the two is set"
        );
        let bad = SpillConfigError::BadQuota {
            value: "1 GiB".to_string(),
        };
        assert!(bad.to_string().contains("positive decimal"));
    }

    #[test]
    fn late_materialization_defaults_to_eight_extra_columns() {
        assert_eq!(
            SqlConfig::default().late_materialization_extra_columns,
            Some(8)
        );
        assert_eq!(DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS, 8);
    }

    /// Issue #1402: the rule ships installed, with a `LIMIT` ceiling. Both
    /// halves are pinned, so neither the default nor the constant can drift
    /// without this failing.
    #[test]
    fn bounded_topk_defaults_to_a_limit_of_1024() {
        assert_eq!(SqlConfig::default().bounded_topk_max_limit, Some(1024));
        assert_eq!(DEFAULT_BOUNDED_TOPK_MAX_LIMIT, 1024);
    }

    fn cache_dir_spill(free_bytes: u64, memory_budget_bytes: u64) -> CacheDirSpill<'static> {
        CacheDirSpill {
            cache_dir: Path::new("/var/cache/ravel"),
            instance_id: "inst-1",
            free_bytes,
            read_cache_bytes: 0,
            memory_budget_bytes,
        }
    }

    /// The derived ceiling is taken from the free bytes less the read cache's
    /// disk-tier bound: 10 GiB free with a 9 GiB bound leaves 1 GiB, whose
    /// half is below the floor, so spill is off; 40 GiB free with the same
    /// bound derives half of 31 GiB; a bound above the free bytes saturates
    /// at zero rather than wrapping.
    ///
    /// Prove-the-test: pass `cache_dir.free_bytes` instead of
    /// `cache_dir.spillable_free_bytes()` in `SpillConfig::resolve` and the
    /// 10 GiB row reads `Some(5368709120)`; subtract with wrapping arithmetic
    /// and the 1 GiB row reads a ceiling near `u64::MAX / 2`.
    #[test]
    fn resolve_derives_from_free_bytes_less_the_read_cache_bound() {
        const GIB: u64 = 1024 * 1024 * 1024;
        for (free_bytes, read_cache_bytes, expected) in [
            (10 * GIB, 9 * GIB, None),
            (40 * GIB, 9 * GIB, Some(31 * GIB / 2)),
            (GIB, 9 * GIB, None),
        ] {
            let resolved = SpillConfig::resolve(
                None,
                None,
                Some(CacheDirSpill {
                    read_cache_bytes,
                    ..cache_dir_spill(free_bytes, u64::MAX)
                }),
            )
            .expect("a cache-dir-only resolution never errors");
            assert_eq!(
                resolved.map(|config| config.max_bytes),
                expected,
                "free_bytes={free_bytes} read_cache_bytes={read_cache_bytes}"
            );
        }
    }

    /// Neither the env pair nor `--cache-dir` is configured: spill stays off,
    /// the same as `from_env` with nothing set. A wrong implementation that
    /// derives a spill config whenever a memory budget is merely in scope
    /// (treating "a budget exists" as "cache-dir is configured") would enable
    /// spill here with no directory to write to.
    #[test]
    fn resolve_with_neither_env_nor_cache_dir_is_off() {
        assert_eq!(
            SpillConfig::resolve(None, None, None).expect("neither source is set"),
            None
        );
    }

    /// The full env pair wins outright over `--cache-dir`: this is
    /// unchanged, original `from_env` behavior, and a `--cache-dir`-derived
    /// implementation must not shadow it. A wrong implementation that checks
    /// `cache_dir` before the env pair would return the derived config
    /// instead of the operator's explicit directory and quota.
    #[test]
    fn resolve_full_env_pair_wins_over_cache_dir() {
        let resolved = SpillConfig::resolve(
            Some(OsStr::new("/explicit/spill")),
            Some(OsStr::new("4096")),
            Some(cache_dir_spill(1_000_000_000_000, 1024)),
        )
        .expect("a full env pair is always valid here");
        assert_eq!(
            resolved,
            Some(SpillConfig {
                dir: PathBuf::from("/explicit/spill"),
                max_bytes: 4096,
            })
        );
    }

    /// `--cache-dir` alone (no env pair) derives both the directory, under
    /// `cache_spill_dir`, and the ceiling, via `derive_spill_max_bytes`. A
    /// wrong implementation that derives the ceiling from total disk space
    /// instead of free space, or that omits the memory-budget cap entirely,
    /// would both pass a test that only checks the directory; this pins the
    /// byte figure too.
    #[test]
    fn resolve_cache_dir_alone_derives_dir_and_ceiling() {
        // free_bytes = 10 GiB, half = 5 GiB; memory_budget = 1 GiB, 4x cap = 4
        // GiB. The cap binds, so the ceiling is 4 GiB, not half of free space.
        let free_bytes = 10 * 1024 * 1024 * 1024;
        let memory_budget_bytes = 1024 * 1024 * 1024;
        let resolved = SpillConfig::resolve(
            None,
            None,
            Some(cache_dir_spill(free_bytes, memory_budget_bytes)),
        )
        .expect("a cache-dir-only resolution never errors")
        .expect("10 GiB free enables cache-dir spill");
        assert_eq!(
            resolved.dir,
            PathBuf::from("/var/cache/ravel/sql-spill/inst-1")
        );
        assert_eq!(resolved.max_bytes, 4 * 1024 * 1024 * 1024);
    }

    /// The 1 GiB floor raises a memory-budget cap below it, and only when half
    /// the free space can hold it: at exactly 2 GiB free (half = 1 GiB) under
    /// a 1 KiB budget the ceiling is 1 GiB, one byte pair less free and spill
    /// is off, and at 800 MiB and at 0 free spill is off.
    ///
    /// Prove-the-test: the floor applied last, after the clamp
    /// (`half_free.min(memory_cap).max(FLOOR)` with no early return), returns
    /// 1 GiB below 2 GiB free and the `2 GiB - 2` row reads
    /// `Some(1073741824)`; the floor applied before the clamp
    /// (`half_free.max(FLOOR).min(memory_cap)`, no early return) returns the
    /// 4 KiB cap and the 2 GiB row reads `Some(4096)`.
    #[test]
    fn derive_resolves_off_when_half_the_free_space_is_below_the_floor() {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;
        for (free_bytes, expected) in [
            (2 * GIB, Some(GIB)),
            (2 * GIB - 2, None),
            (800 * MIB, None),
            (0, None),
        ] {
            assert_eq!(
                derive_spill_max_bytes(free_bytes, 1024),
                expected,
                "free_bytes={free_bytes}"
            );
            let resolved =
                SpillConfig::resolve(None, None, Some(cache_dir_spill(free_bytes, 1024)))
                    .expect("a cache-dir-only resolution never errors");
            assert_eq!(
                resolved.map(|config| config.max_bytes),
                expected,
                "resolve, free_bytes={free_bytes}"
            );
        }
    }

    /// `RAVEL_SQL_SPILL_MAX_BYTES` alone, with no directory, overrides the
    /// derived ceiling under a configured `--cache-dir`: the directory still
    /// comes from `cache_dir`, but the quota is the operator's, not derived.
    /// A wrong implementation that ignores a dir-less env quota under
    /// `--cache-dir` would derive the ceiling anyway and silently discard the
    /// operator's number.
    #[test]
    fn resolve_env_quota_alone_overrides_derived_ceiling_under_cache_dir() {
        let resolved = SpillConfig::resolve(
            None,
            Some(OsStr::new("777")),
            Some(cache_dir_spill(1_000_000_000_000, 1024 * 1024 * 1024)),
        )
        .expect("a dir-less env quota under cache-dir never errors")
        .expect("an env quota under cache-dir enables spill");
        assert_eq!(
            resolved.dir,
            PathBuf::from("/var/cache/ravel/sql-spill/inst-1")
        );
        assert_eq!(resolved.max_bytes, 777);
    }

    /// `RAVEL_SQL_SPILL_MAX_BYTES` alone with no `--cache-dir` at all is still
    /// `Incomplete`: there is no cache-dir-less derivation to fall back to.
    #[test]
    fn resolve_env_quota_alone_without_cache_dir_is_incomplete() {
        assert_eq!(
            SpillConfig::resolve(None, Some(OsStr::new("777")), None).expect_err(
                "a dir-less env quota with no cache-dir has nowhere to derive a directory from"
            ),
            SpillConfigError::Incomplete
        );
    }

    /// `RAVEL_SQL_SPILL_DIR` alone is always `Incomplete`, with or without
    /// `--cache-dir`: there is no symmetric "derive a directory" fallback for
    /// a bare env directory, only for a bare env quota.
    #[test]
    fn resolve_env_dir_alone_is_incomplete_even_with_cache_dir() {
        assert_eq!(
            SpillConfig::resolve(
                Some(OsStr::new("/explicit/spill")),
                None,
                Some(cache_dir_spill(1_000_000_000_000, 1024)),
            )
            .expect_err("a bare env dir has no quota fallback, with or without cache-dir"),
            SpillConfigError::Incomplete
        );
    }

    /// `--sql-spill off` forces spill off outright, regardless of a fully-set
    /// env pair or a configured `--cache-dir`. A wrong implementation that
    /// only checks `sql_spill_off` after already resolving the environment
    /// (rather than short-circuiting first) would still return the env pair's
    /// config here instead of `None`.
    #[test]
    fn with_spill_resolved_off_wins_over_a_fully_configured_cache_dir() {
        let config = SqlConfig::default()
            .with_spill_resolved(true, Some(cache_dir_spill(1_000_000_000_000, 1024)))
            .expect("off never errors");
        assert_eq!(config.spill, None);
    }

    /// The free figure is `f_bavail` fragments of `f_frsize` bytes. Every
    /// field below differs, so reading the volume size (`f_blocks`), the
    /// root-inclusive free count (`f_bfree`) or the preferred I/O size
    /// (`f_bsize`) gives a different number.
    #[test]
    fn measure_free_bytes_is_available_not_total() {
        let stat = rustix::fs::StatVfs {
            f_bsize: 1 << 20,
            f_frsize: 4096,
            f_blocks: 1_000_000,
            f_bfree: 600_000,
            f_bavail: 550_000,
            f_files: 0,
            f_ffree: 0,
            f_favail: 0,
            f_fsid: 0,
            f_flag: rustix::fs::StatVfsMountFlags::empty(),
            f_namemax: 255,
        };
        assert_eq!(available_bytes(&stat), 550_000 * 4096);
        assert!(measure_free_bytes(Path::new(".")).is_ok());
    }

    /// `with_spill_resolved` with `sql_spill_off` false and an already-set
    /// field leaves it untouched: a process that configured spill in code
    /// cannot have it silently replaced by the environment or a cache
    /// directory.
    #[test]
    fn with_spill_resolved_does_not_override_an_explicit_setting() {
        let explicit = SpillConfig {
            dir: PathBuf::from("/explicit"),
            max_bytes: 4096,
        };
        let config = SqlConfig {
            spill: Some(explicit.clone()),
            ..SqlConfig::default()
        };
        let after = config
            .with_spill_resolved(false, Some(cache_dir_spill(1_000_000_000_000, 1024)))
            .expect("an already-set field reads nothing else");
        assert_eq!(after.spill, Some(explicit));
    }
}
