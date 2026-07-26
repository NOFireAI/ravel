//! Catalog configuration (docs/catalog-and-mvcc.md).

/// Default `max_ingest_lag`: 2 hours, in nanoseconds.
pub const DEFAULT_MAX_INGEST_LAG_NS: i64 = 2 * 60 * 60 * 1_000_000_000;
/// Default `clock_skew_allowance`: 5 minutes, in nanoseconds.
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 5 * 60 * 1_000_000_000;
/// Default bound on decoded commit records cached per tenant. Not named in
/// the ADR; a capacity cap is the simpler of the two eviction strategies it
/// explicitly allows ("simple LRU or capacity cap per tenant").
pub const DEFAULT_CACHE_CAPACITY_PER_TENANT: usize = 10_000;

/// Catalog configuration.
///
/// `shard_count` is immutable per (tenant, signal) in v1 (ADR-0010 §9):
/// once segments for a (tenant, signal) exist, changing this value is a
/// data-loss operation (segments already routed to a shard index become
/// unreachable if the shard count changes) and is forbidden. Phase 1 reads
/// it from static config; resolvers will read it from a per-tenant manifest
/// object once that lands (ADR-0010 §9), at which point this field becomes
/// a cache of that manifest rather than the source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogConfig {
    /// Number of shards for the (tenant, signal) this catalog serves. See
    /// the struct docs: immutable per (tenant, signal), never hot-reloaded.
    pub shard_count: u32,
    /// How far behind `range.start_ns` the commit listing window extends,
    /// in nanoseconds. Default 2h ([`DEFAULT_MAX_INGEST_LAG_NS`]).
    pub max_ingest_lag_ns: i64,
    /// Padding added past `now_ns` for the listing window's upper bound, to
    /// absorb writer clock skew, in nanoseconds. Default 5m
    /// ([`DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`]).
    pub clock_skew_allowance_ns: i64,
    /// Bound on decoded commit records cached per tenant. Default
    /// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`].
    pub cache_capacity_per_tenant: usize,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        CatalogConfig {
            shard_count: 1,
            max_ingest_lag_ns: DEFAULT_MAX_INGEST_LAG_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            cache_capacity_per_tenant: DEFAULT_CACHE_CAPACITY_PER_TENANT,
        }
    }
}
