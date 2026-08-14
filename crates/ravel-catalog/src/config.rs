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
/// its ingest hour's end; the seal watermark relies on that bound (ADR-0020).
pub const DEFAULT_MAX_FLUSH_LIFETIME_NS: i64 = 60 * 60 * 1_000_000_000;
/// Default `fold_safety_margin`: 15 minutes, in nanoseconds. Extra padding
/// past `max_flush_lifetime + clock_skew_allowance` before a fold trusts an
/// ingest hour to be sealed (ADR-0020).
pub const DEFAULT_FOLD_SAFETY_MARGIN_NS: i64 = 15 * 60 * 1_000_000_000;
/// Default `head_cache_ttl`: 30 seconds, in nanoseconds (ADR-0020).
pub const DEFAULT_HEAD_CACHE_TTL_NS: i64 = 30 * 1_000_000_000;
/// Default bound on decoded snapshot parts cached per tenant. Parts are
/// content-addressed and immutable, so this cache never invalidates on
/// write, only evicts by capacity.
pub const DEFAULT_SNAPSHOT_CACHE_PARTS: usize = 32;
/// Default bound on decoded name-postings objects cached per tenant.
/// Postings objects are content-addressed and immutable, same eviction
/// rationale as [`DEFAULT_SNAPSHOT_CACHE_PARTS`].
pub const DEFAULT_POSTINGS_CACHE_ENTRIES: usize = 32;
/// Default bound on the total number of (tenant, signal) entries
/// [`crate::cache::HeadCache`] holds at once, process-wide (without this bound the
/// cache would grow one entry per (tenant,
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
/// Default ceiling on the pre-execution catalog-request estimate
/// (`Catalog::estimated_catalog_requests`, ADR-0044 decision 3): a resolve whose `shard_count * hour_buckets +
/// SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` exceeds this is refused before any
/// LIST is issued. 100,000 catalog requests permits roughly an 11-year window
/// at `shard_count = 1` and roughly 8.5 months at `shard_count = 16`, while
/// refusing the epoch-width `start: 0.0` query (about 496,089 LISTs at a
/// single shard) that motivated the guard. Sized to catch runaways, not to
/// cap a tuned deployment; a wider worst case raises the field.
pub const DEFAULT_MAX_CATALOG_LIST_REQUESTS: u64 = 100_000;
/// Default `snapshot_part_max_entries`: the tail part seals once its
/// accumulated entry count reaches this bound and the next fold starts a
/// fresh tail (ADR-0063 section 1, "Sealing policy"). 250,000 entries is
/// about 27 MB raw / 8 MB compressed at the measured 100-115 B/entry, well
/// under [`snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES`]'s decode cap.
/// Splits are always at hour boundaries, so a single hour larger than this
/// still produces one oversized (but decode-cap-bounded) part.
pub const DEFAULT_SNAPSHOT_PART_MAX_ENTRIES: usize = 250_000;
/// Default `fold_bucket_concurrency`: the number of per-(shard, hour) bucket
/// discovery LISTs a single fold keeps in flight at once (ADR-0063 section 3,
/// mirroring the resolve path's in-flight bound). 8 matches the resolve
/// path's fan-out order without saturating a small backend.
pub const DEFAULT_FOLD_BUCKET_CONCURRENCY: usize = 8;
/// Default `fold_reconcile_window_hours`: how far back of the previous fold's
/// watermark the post-fold reconcile pass re-lists commit buckets to catch a
/// compaction record or retention tombstone that landed in an hour already
/// sealed and folded (ADR-0063 section 4). 26 covers `protection_horizon`
/// (24 h, the age gate before the sweeper may delete a superseded compaction
/// input) plus slack, so any late record whose supersession could invalidate
/// a folded snapshot entry is observed by a reconcile pass before its inputs
/// can be physically deleted. A late record older than this bound is not
/// picked up: a stated, bounded staleness tradeoff, not a bug.
pub const DEFAULT_FOLD_RECONCILE_WINDOW_HOURS: u32 = 26;
/// Default crossover at which `Catalog::resolve` switches from the
/// per-(shard, ingest-hour) LIST loop to a single per-shard recursive prefix
/// LIST (ADR-0056). Expressed in per-bucket request units, i.e. the number of
/// `(shard, hour)` buckets the listing suffix spans: at or above this, the
/// prefix scan (cost `O(objects / page_size)`, independent of window width)
/// wins over the per-bucket loop (one LIST per bucket, empty buckets
/// included). 720 is thirty days of hourly buckets at `shard_count = 1`: warm
/// operational windows stay on the watermark-pruning per-bucket path, and the
/// per-bucket path's request amplification is capped at 720 LISTs before the
/// handoff. Purely a performance heuristic -- both paths return identical
/// snapshots and both respect [`DEFAULT_MAX_CATALOG_LIST_REQUESTS`], so any
/// value is correct.
pub const DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS: u64 = 720;

/// Catalog configuration.
///
/// `shard_count` is immutable per generation; the generation history is
/// append-only; the shard-index domain of hour `h` is `0..scan_count(h)`
/// (ADR-0052, online resharding, superseding ADR-0010 §9's "immutable per
/// (tenant, signal)"). Existing data is never moved or re-keyed by a reshard;
/// a reshard appends a new `(generation, shard_count, activation_hour)` entry
/// to the durable provisioning record under `CasVersion`, and readers derive
/// the per-hour shard fan-out from that history via
/// [`crate::scan_count`] rather than from this single value.
///
/// This field is the process's configured baseline, equal to generation 0's
/// count. It is not merely a static config that resolvers trust blindly:
/// ADR-0050 section 5 makes it a durable, startup-checked property. A
/// (tenant, signal)'s first write pins it in a provisioning record at
/// `t/<tenant_hash>/<sig>/prov` ([`crate::validate_or_adopt`]); every later
/// ingest, catalog, and maintain touch validates this configured value
/// against that record's scalar `shard_count` and refuses (static tenant) or
/// fails the request (dynamic tenant) on disagreement. The read-side scan set
/// for any hour, however, comes from the generation history, not this field,
/// so a resharded tenant is resolved over the correct per-hour shard range
/// even though this configured baseline never changes.
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
    /// ingest hour ends, in nanoseconds. Part of the seal-watermark margin (ADR-0020). Default
    /// [`DEFAULT_MAX_FLUSH_LIFETIME_NS`].
    pub max_flush_lifetime_ns: i64,
    /// Extra margin added past `max_flush_lifetime_ns +
    /// clock_skew_allowance_ns` before a fold trusts an ingest hour to be
    /// sealed, in nanoseconds. Default [`DEFAULT_FOLD_SAFETY_MARGIN_NS`].
    pub fold_safety_margin_ns: i64,
    /// How long a decoded HEAD may be served from cache before `resolve`
    /// re-reads it, in nanoseconds (a
    /// stale cache only ever widens the listed suffix by up to this much,
    /// never a correctness issue). Default [`DEFAULT_HEAD_CACHE_TTL_NS`].
    pub head_cache_ttl_ns: i64,
    /// Bound on decoded snapshot parts cached per tenant. Default
    /// [`DEFAULT_SNAPSHOT_CACHE_PARTS`].
    pub snapshot_cache_parts: usize,
    /// Resource cap applied to a snapshot part's declared decompressed size
    /// at resolve time. Default
    /// [`snapshot_format::DEFAULT_MAX_SNAPSHOT_PART_BYTES`].
    pub max_snapshot_part_bytes: u64,
    /// Resource cap applied to a name-postings object's declared
    /// decompressed body size at decode time. Default [`snapshot_format::DEFAULT_MAX_POSTINGS_BYTES`].
    pub max_postings_bytes: u64,
    /// Bound on decoded name-postings objects cached per tenant.
    /// Default [`DEFAULT_POSTINGS_CACHE_ENTRIES`].
    pub postings_cache_entries: usize,
    /// Bound on the total number of (tenant, signal) entries
    /// [`crate::cache::HeadCache`] holds at once, process-wide. Default
    /// [`DEFAULT_HEAD_CACHE_CAPACITY`].
    pub head_cache_capacity: usize,
    /// Total byte budget for the byte cache (ADR-0046), the RAM tier of raw
    /// content-addressed bytes consulted ahead of a store GET for snapshot
    /// parts and postings objects. Default [`DEFAULT_BYTE_CACHE_MAX_BYTES`].
    ///
    /// `0` is the disabled sentinel: [`crate::Catalog::new`] then builds no
    /// byte cache at all, so a resolve reads every content-addressed object
    /// straight through [`crate::Catalog`]'s store funnel with no RAM tier in
    /// front of it and no cache hit/miss accounting for it. This is how the
    /// server's `--disable-cache` reaches the catalog byte cache, not just the
    /// fetcher cache; it is the byte-cache analogue of building
    /// the query fetchers with no `Cache` attached.
    pub byte_cache_max_bytes: u64,
    /// Entry-count bound for the byte cache. Default
    /// [`DEFAULT_BYTE_CACHE_MAX_ENTRIES`].
    pub byte_cache_max_entries: usize,
    /// Per-entry byte cap for the byte cache; an object larger than this is
    /// never admitted. Default [`DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES`].
    pub byte_cache_max_entry_bytes: u64,
    /// Ceiling on the pre-execution catalog-request estimate (ADR-0044
    /// decision 3). A resolve whose
    /// estimate ([`Catalog::estimated_catalog_requests`](crate::Catalog::estimated_catalog_requests))
    /// exceeds this is refused with [`CatalogError::WindowTooWide`](crate::CatalogError::WindowTooWide)
    /// before any LIST is issued, so an unbounded client window cannot make a
    /// single request fan out to hundreds of thousands of LISTs. Fail-closed:
    /// over the ceiling the query is refused, never silently narrowed. Default
    /// [`DEFAULT_MAX_CATALOG_LIST_REQUESTS`].
    pub max_catalog_list_requests: u64,
    /// Crossover, in `(shard, hour)` bucket units, at which `Catalog::resolve`
    /// switches from the per-bucket LIST loop to a single per-shard recursive
    /// prefix LIST (ADR-0056). When the listing suffix spans at least this many
    /// buckets (`shard_count * listing_hours`), the prefix scan is used;
    /// narrower windows keep the per-bucket loop, which prunes better behind a
    /// folded snapshot watermark. A performance heuristic only: both paths
    /// return identical snapshots and both respect
    /// [`max_catalog_list_requests`](Self::max_catalog_list_requests). Default
    /// [`DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS`].
    pub prefix_list_crossover_requests: u64,
    /// Entry-count ceiling at which the fold seals the current tail part and
    /// starts a fresh one (ADR-0063 section 1). Once a tail's accumulated
    /// entries reach this, the next hour boundary seals it into an immutable
    /// sealed part carried by reference on later folds, and a new tail
    /// accumulates past it. Default [`DEFAULT_SNAPSHOT_PART_MAX_ENTRIES`].
    pub snapshot_part_max_entries: usize,
    /// Number of per-(shard, hour) bucket discovery LISTs the fold keeps in
    /// flight at once (ADR-0063 section 3). Bounds the fold's concurrent
    /// discovery I/O, mirroring the resolve path's in-flight semaphore.
    /// Default [`DEFAULT_FOLD_BUCKET_CONCURRENCY`].
    pub fold_bucket_concurrency: usize,
    /// How far back of the previous fold's watermark the post-fold reconcile
    /// pass re-lists commit buckets, in hours (ADR-0063 section 4). Each fold
    /// re-lists the window `[watermark_hour_old - fold_reconcile_window_hours,
    /// watermark_hour_old]` to catch a compaction record or retention
    /// tombstone that landed in an already-sealed, already-folded hour, which
    /// the incremental path (hours strictly after the old watermark) never
    /// rediscovers. A late record older than this bound is a stated, bounded
    /// staleness tradeoff, never applied. Default
    /// [`DEFAULT_FOLD_RECONCILE_WINDOW_HOURS`].
    pub fold_reconcile_window_hours: u32,
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
            max_catalog_list_requests: DEFAULT_MAX_CATALOG_LIST_REQUESTS,
            prefix_list_crossover_requests: DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS,
            snapshot_part_max_entries: DEFAULT_SNAPSHOT_PART_MAX_ENTRIES,
            fold_bucket_concurrency: DEFAULT_FOLD_BUCKET_CONCURRENCY,
            fold_reconcile_window_hours: DEFAULT_FOLD_RECONCILE_WINDOW_HOURS,
        }
    }
}
