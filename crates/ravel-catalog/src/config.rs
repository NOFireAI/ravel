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
/// Default bound on decoded name-postings objects cached per tenant (P5b).
/// Postings objects are content-addressed and immutable, same eviction
/// rationale as [`DEFAULT_SNAPSHOT_CACHE_PARTS`].
pub const DEFAULT_POSTINGS_CACHE_ENTRIES: usize = 32;
/// Default bound on the total number of (tenant, signal) entries
/// [`crate::cache::HeadCache`] holds at once, process-wide (issue #421: the
/// cache had a TTL but no capacity bound, so it grew one entry per (tenant,
/// signal) with no limit on the number of tenants). `Signal` has at most a
/// handful of variants, so this bound admits thousands of actively-queried
/// tenants per process before the oldest (tenant, signal) pair is evicted.
pub const DEFAULT_HEAD_CACHE_CAPACITY: usize = 10_000;
/// Default total byte budget for the byte cache (ADR-0046 decisions 1-2): the
/// RAM tier of raw, content-addressed bytes consulted at `guarded_get`-adjacent
/// call sites before a store GET, ahead of decode into a [`crate::cache::PartCache`]
/// or [`crate::cache::PostingsCache`] entry. 512 MiB, twice
/// [`DEFAULT_MAX_SNAPSHOT_PART_BYTES`](snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES),
/// enough headroom for a handful of hot parts and postings objects at once.
pub const DEFAULT_BYTE_CACHE_MAX_BYTES: u64 = 512 << 20;
/// Default entry-count bound for the byte cache. Modest: entries are whole
/// parts/postings objects, not small pages, so a large count is never needed
/// to fill [`DEFAULT_BYTE_CACHE_MAX_BYTES`].
pub const DEFAULT_BYTE_CACHE_MAX_ENTRIES: usize = 512;
/// Default per-entry byte cap for the byte cache, matching the largest object
/// class it admits
/// ([`DEFAULT_MAX_SNAPSHOT_PART_BYTES`](snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES) ==
/// [`DEFAULT_MAX_POSTINGS_BYTES`](snapshot_format::DEFAULT_MAX_POSTINGS_BYTES), both 256 MiB).
pub const DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES: u64 = 256 << 20;

/// Catalog configuration.
///
/// `shard_count` is immutable per (tenant, signal) (ADR-0010 §9): once
/// segments for a (tenant, signal) exist, changing this value is a data-loss
/// operation (segments already routed to a shard index become unreachable if
/// the shard count changes) and is forbidden. It is no longer merely a static
/// process config that resolvers trust blindly: ADR-0050 section 5 makes it a
/// durable, startup-checked property. A (tenant, signal)'s first write pins the
/// configured value in an immutable provisioning record at
/// `t/<tenant_hash>/<sig>/prov` ([`crate::validate_or_adopt`]); every later
/// ingest, catalog, and maintain touch validates this configured value against
/// that record and refuses (static tenant) or fails the request (dynamic
/// tenant) on disagreement, rather than silently resolving over a subset of
/// shards. This field is therefore the configured value validated against the
/// durable record, not an unchecked source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogConfig {
    /// Number of shards for the (tenant, signal) this catalog serves. See
    /// the struct docs: immutable per (tenant, signal), never hot-reloaded,
    /// and validated at startup/first-touch against the durable provisioning
    /// record (ADR-0050 section 5).
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
    /// Resource cap applied to a name-postings object's declared
    /// decompressed body size at decode time (docs/metric-index-plan.md
    /// 3.3). Default [`snapshot_format::DEFAULT_MAX_POSTINGS_BYTES`].
    pub max_postings_bytes: u64,
    /// Bound on decoded name-postings objects cached per tenant (P5b).
    /// Default [`DEFAULT_POSTINGS_CACHE_ENTRIES`].
    pub postings_cache_entries: usize,
    /// Bound on the total number of (tenant, signal) entries
    /// [`crate::cache::HeadCache`] holds at once, process-wide. Default
    /// [`DEFAULT_HEAD_CACHE_CAPACITY`].
    pub head_cache_capacity: usize,
    /// Total byte budget for the byte cache (ADR-0046), the RAM tier of raw
    /// content-addressed bytes consulted ahead of a store GET for snapshot
    /// parts and postings objects. Default [`DEFAULT_BYTE_CACHE_MAX_BYTES`].
    pub byte_cache_max_bytes: u64,
    /// Entry-count bound for the byte cache. Default
    /// [`DEFAULT_BYTE_CACHE_MAX_ENTRIES`].
    pub byte_cache_max_entries: usize,
    /// Per-entry byte cap for the byte cache; an object larger than this is
    /// never admitted. Default [`DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES`].
    pub byte_cache_max_entry_bytes: u64,
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
            max_postings_bytes: snapshot_format::DEFAULT_MAX_POSTINGS_BYTES,
            postings_cache_entries: DEFAULT_POSTINGS_CACHE_ENTRIES,
            head_cache_capacity: DEFAULT_HEAD_CACHE_CAPACITY,
            byte_cache_max_bytes: DEFAULT_BYTE_CACHE_MAX_BYTES,
            byte_cache_max_entries: DEFAULT_BYTE_CACHE_MAX_ENTRIES,
            byte_cache_max_entry_bytes: DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES,
        }
    }
}
