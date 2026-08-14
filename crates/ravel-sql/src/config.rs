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

/// Default per-query RecordBatch byte budget: 256 MiB. A placeholder pending
/// measurement; documented as such so it is not mistaken for a tuned value.
pub const DEFAULT_MAX_QUERY_BYTES: usize = 256 * 1024 * 1024;

/// Per-query ravel-sql configuration: the shared engine budgets plus the
/// SQL-only per-query memory-pool byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlConfig {
    /// Series/segment/sample/deadline budgets shared with the PromQL path.
    pub engine: EngineConfig,
    /// Ceiling, in bytes, on the query's DataFusion memory pool. Fed by
    /// measured `RecordBatch` sizes, never a sample count.
    pub max_query_bytes: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        SqlConfig {
            engine: EngineConfig::default(),
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
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
