//! Catalog configuration (docs/catalog-and-mvcc.md).

use crate::snapshot_format;

/// Default `max_ingest_lag`: 2 hours, in nanoseconds.
pub const DEFAULT_MAX_INGEST_LAG_NS: i64 = 2 * 60 * 60 * 1_000_000_000;
/// Default `clock_skew_allowance`: 5 minutes, in nanoseconds.
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 5 * 60 * 1_000_000_000;
/// Default bound on decoded commit records cached per tenant. Not named in
/// the ADR; a capacity cap is the simpler of the two eviction strategies it
/// explicitly allows ("simple LRU or capacity cap per tenant").
pub const DEFAULT_CACHE_CAPACITY_PER_TENANT: usize = 10_000;
/// Default `max_flush_lifetime`: 1 hour, in nanoseconds. The GC interlock
/// (ADR-0010 §11) forbids publishing a commit record after this long past
/// its ingest hour's end; the seal watermark relies on that bound
/// (docs/metric-index-plan.md, ADR-0020).
pub const DEFAULT_MAX_FLUSH_LIFETIME_NS: i64 = 60 * 60 * 1_000_000_000;
/// Default `fold_safety_margin`: 15 minutes, in nanoseconds. Extra padding
/// past `max_flush_lifetime + clock_skew_allowance` before a fold trusts an
/// ingest hour to be sealed (docs/metric-index-plan.md, ADR-0020).
pub const DEFAULT_FOLD_SAFETY_MARGIN_NS: i64 = 15 * 60 * 1_000_000_000;
/// Default `head_cache_ttl`: 30 seconds, in nanoseconds
/// (docs/metric-index-plan.md 5.1, ADR-0020).
pub const DEFAULT_HEAD_CACHE_TTL_NS: i64 = 30 * 1_000_000_000;
/// Default bound on decoded snapshot parts cached per tenant. Parts are
/// content-addressed and immutable, so this cache never invalidates on
/// write, only evicts by capacity.
pub const DEFAULT_SNAPSHOT_CACHE_PARTS: usize = 32;

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
    /// Longest a writer may take to publish a commit record after its
    /// ingest hour ends, in nanoseconds. Part of the seal-watermark margin
    /// (docs/metric-index-plan.md, ADR-0020). Default
    /// [`DEFAULT_MAX_FLUSH_LIFETIME_NS`].
    pub max_flush_lifetime_ns: i64,
    /// Extra margin added past `max_flush_lifetime_ns +
    /// clock_skew_allowance_ns` before a fold trusts an ingest hour to be
    /// sealed, in nanoseconds. Default [`DEFAULT_FOLD_SAFETY_MARGIN_NS`].
    pub fold_safety_margin_ns: i64,
    /// How long a decoded HEAD may be served from cache before `resolve`
    /// re-reads it, in nanoseconds (docs/metric-index-plan.md 5.1, 5.3: a
    /// stale cache only ever widens the listed suffix by up to this much,
    /// never a correctness issue). Default [`DEFAULT_HEAD_CACHE_TTL_NS`].
    pub head_cache_ttl_ns: i64,
    /// Bound on decoded snapshot parts cached per tenant. Default
    /// [`DEFAULT_SNAPSHOT_CACHE_PARTS`].
    pub snapshot_cache_parts: usize,
    /// Resource cap applied to a snapshot part's declared decompressed size
    /// at resolve time (docs/metric-index-plan.md 3.1). Default
    /// [`snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES`].
    pub max_snapshot_part_bytes: u64,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        CatalogConfig {
            shard_count: 1,
            max_ingest_lag_ns: DEFAULT_MAX_INGEST_LAG_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            cache_capacity_per_tenant: DEFAULT_CACHE_CAPACITY_PER_TENANT,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            fold_safety_margin_ns: DEFAULT_FOLD_SAFETY_MARGIN_NS,
            head_cache_ttl_ns: DEFAULT_HEAD_CACHE_TTL_NS,
            snapshot_cache_parts: DEFAULT_SNAPSHOT_CACHE_PARTS,
            max_snapshot_part_bytes: snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES,
        }
    }
}
