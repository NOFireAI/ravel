//! Catalog configuration (docs/catalog-and-mvcc.md).

use std::time::Duration;

use crate::snapshot_format;

/// Default `max_ingest_lag`: 2 hours, in nanoseconds.
pub const DEFAULT_MAX_INGEST_LAG_NS: i64 = 2 * 60 * 60 * 1_000_000_000;
/// Default `clock_skew_allowance`: 5 minutes, in nanoseconds.
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 5 * 60 * 1_000_000_000;
/// Floor on decoded commit records cached per tenant, in entries. Not named
/// in the ADR, which allows "simple LRU or capacity cap per tenant"; the
/// cache is an LRU (crate::cache).
///
/// The capacity actually configured is derived per deployment by
/// [`derive_cache_capacity_per_tenant`] from `shard_count` and
/// `max_flush_delay`: `shards * signals * flushes_per_hour *
/// hot_region_hours`, clamped between this floor and the `25_000`-entry cap
/// `MAX_CACHE_CAPACITY_PER_TENANT`. It holds as much of a tenant's unsealed
/// hot-region tail -- the part every query touches, since no fold has sealed
/// it yet -- as those 25,000 entries cover, and a repeated resolve re-issues
/// no per-record GET while the tail stays inside the bound (issue #783, issue
/// #1735). Read that as "up to 25,000 records of tail", not "the tail": three
/// things put a real tail over the bound.
///
/// * The cap. At the shipped defaults the uncapped derivation is already
///   `4 * 6 * 1800 * 3` = 129,600 entries and the clamp returns 25,000, so a
///   default deployment that really does ingest six signals across four
///   shards for three unsealed hours is over the bound by the derivation's
///   own estimate.
/// * The size trigger. `flushes_per_hour` counts the age trigger only. A
///   shard also flushes the moment its estimated object bytes reach
///   `ravel_ingest::IngestConfig::target_bytes` (8 MiB by default),
///   whatever its age, so a high-volume tenant seals more records per
///   shard-hour than `ceil(3600 / max_flush_delay_secs)` assumes. The
///   derivation cannot see ingest rate, so that term is a lower bound on
///   records per shard-hour rather than the worst case.
/// * The byte budget. The capacity is an entry CAP, not a guaranteed
///   residency: the cache evicts on whichever of the entry cap and the byte
///   budget binds first, so a tenant whose records carry declared column
///   statistics holds proportionally fewer than `capacity` of them. That is
///   the point of the byte bound -- memory stays inside the stated figure
///   whatever the records look like -- and it is why the capacity buys a bound
///   on cost, not a promise about hit rate.
///
/// Past the bound the resolve's two passes evict each other and the tenant
/// pays per-record GETs again, which is what
/// `a_bound_below_the_hot_region_loses_the_saving` in
/// tests/hot_record_cache.rs pins. The byte bound never over-allocates, but
/// it is not free of a hit-rate cost the old flat entry count did not have:
/// each declared column statistic adds at least
/// `DECLARED_COLUMN_STAT_FIXED_BYTES` to an entry's charge, as
/// `commit_entry_charge_counts_the_declared_column_list` in cache.rs pins, so
/// a tenant whose records carry them holds fewer records than the flat 10,000
/// the old constant held for everyone, and a wide-column tenant whose
/// unsealed tail no longer fits re-reads that tail on each resolve. That is
/// the trade the bound makes: a memory figure the code enforces, in place of
/// an entry count that promised nothing about bytes. This constant is the
/// saturating
/// floor `derive_cache_capacity_per_tenant` never returns below, so a
/// deployment with very few shards and a very long flush delay still gets the
/// same protection the fixed constant used to give everyone.
///
/// An ordinary commit-record entry costs about 864 bytes, which
/// [`RECORD_CACHE_ENTRY_BYTES`] rounds up to 900: a 119-byte commit key held
/// twice (the map key and the recency index), the 224-byte decoded
/// `CommitRecord` plus 210 bytes of its own heap (16-byte tenant hash, 36-byte
/// writer uuid, 126-byte data object key, 32-byte content hash), and the two
/// maps' slot overhead. That is a PLANNING figure for a record carrying no
/// declared column statistics; what each cache enforces is the byte budget
/// below, charged per entry against what the entry actually holds
/// (`the_per_entry_planning_figure_bounds_an_ordinary_records_charge` in
/// `crate::cache` pins the 864 against the constant).
///
/// The capacity sizes TWO per-tenant caches, not one
/// (`RECORD_CACHES_PER_TENANT`), and each is held to an equal share of the
/// per-tenant budget: `capacity * RECORD_CACHE_ENTRY_BYTES` bytes, 22.5 MB
/// each at the 25,000-entry cap. Neither is bounded by its entry count alone,
/// because neither record type has a bounded size:
///
/// * The commit-record cache (`crate::cache::RecordCache`) is bounded at
///   [`CatalogConfig::commit_cache_max_bytes_per_tenant`].
///   `CommitRecord.declared_column_stats` is a repeated field capped neither by
///   the format (proto/ravel/commit.proto) nor by validation nor by the
///   tenant-config declared-column path, so a record carrying 200 declared
///   columns charges roughly 20 KB rather than 900 bytes, more than twenty
///   times the planning figure
///   (`commit_records_with_large_declared_column_lists_evict_on_bytes` in
///   `crate::cache` pins that charge). It charges each entry an estimate of its
///   live heap (`crate::cache::commit_entry_resident_bytes`) and evicts
///   least-recently-used until the summed charge is inside the budget; the
///   entry count is capped at the capacity as well, so both bounds hold.
/// * The L1 compaction-record cache (`crate::cache::CompactionRecordCache`) is
///   bounded at [`CatalogConfig::compaction_cache_max_bytes_per_tenant`], the
///   same share. A `CompactionRecord` carries one `CompactionInputIdentity` per
///   compacted L0 segment, capped neither by the format nor by
///   `ravel_commit::record::validate_compaction`, so one L1 record over a
///   shard-hour that sealed 1,800 L0 records holds about 1,800 inputs and
///   charges roughly 137 KB, about 150 times the planning figure
///   (`oversized_compaction_entries_evict_on_bytes_not_count` in
///   `crate::cache` pins that charge). Under an
///   entry-count bound that cache could exceed its share of the figure below
///   by two orders of magnitude before evicting anything. It charges
///   `crate::cache::compaction_entry_resident_bytes` per entry and evicts
///   oldest-first, with the entry count capped at the capacity as well.
///
/// So the worst case per actively-queried tenant is `capacity *
/// RECORD_CACHE_ENTRY_BYTES * RECORD_CACHES_PER_TENANT`: at the cap, 25,000 *
/// 900 * 2 = 45 MB (22.5 MB per cache), reclaimed by the idle-tenant sweep
/// ([`Catalog::evict_idle_tenants`](crate::Catalog::evict_idle_tenants)). Both
/// halves are enforced by their byte budget, so 45 MB is a bound the eviction
/// paths hold rather than an estimate of a typical entry times a count.
///
/// This is not one of the ADR-1170 carved caches: it is not a share of
/// `memory_budget_bytes`, it is a per-tenant bound sized from ingest
/// cadence, and its footprint scales with how many tenants are actively
/// queried, not with a fixed process-wide ceiling. That is what the cap is
/// for: an operator multiplies 45 MB by the number of tenants queried
/// concurrently (100 of them is 4.5 GB worst case) and compares that with
/// what is left of the host after ADR-1170's carved shares, rather than
/// against a number that also moves with `--shards`.
///
/// `0` is the disabled sentinel: nothing is admitted and every record read
/// falls through to a store GET, matching
/// [`CatalogConfig::byte_cache_max_bytes`]'s own `0` sentinel. `0` is passed
/// explicitly (never derived) when the cache is meant to be off.
pub const DEFAULT_CACHE_CAPACITY_PER_TENANT: usize = 10_000;

/// Memory budget the derived capacity is capped at, per actively-queried
/// tenant, across BOTH caches the capacity bounds. 45 MB.
///
/// The cap exists because the derivation multiplies shard count by signal
/// count by flush cadence and so grows without bound on a wide deployment:
/// `--shards 64` at the shipped 2-second cadence derives 2,073,600 entries
/// uncapped, 3.7 GB per actively-queried tenant, with nothing process-wide to
/// stop a second tenant costing the same again. Capping in BYTES rather than
/// in entries is what makes it checkable: whatever `--shards` and
/// `--max-flush-delay` are set to, one actively-queried tenant costs at most
/// this, so the operator's question is only how many tenants are queried at
/// once (100 of them is 4.5 GB worst case). These caches sit OUTSIDE
/// ADR-1170's carved shares -- their bounds are per-tenant, not slices of
/// `memory_budget_bytes` -- so that product is what to subtract from what the
/// host has left after the carved fetch, catalog-byte and SQL shares, not
/// something already inside them.
///
/// 45 MB is chosen against that multiplication rather than against a single
/// tenant's appetite: it keeps a 100-tenant working set inside single-digit
/// GB on the 30 GiB reference box ADR-1170 measures, while still holding
/// three signals' worth of an ordinary tenant's unsealed tail.
pub const MAX_RECORD_CACHE_BYTES_PER_TENANT: u64 = 45_000_000;

/// Cap on the value [`derive_cache_capacity_per_tenant`] returns, in entries:
/// [`MAX_RECORD_CACHE_BYTES_PER_TENANT`] divided by what one entry costs in
/// each of the two caches. 25,000 entries. Deliberately above the
/// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`] floor, so the clamp cannot invert.
///
/// Neither cache is denominated in entries alone: each is held to the product
/// [`CatalogConfig::commit_cache_max_bytes_per_tenant`] /
/// [`CatalogConfig::compaction_cache_max_bytes_per_tenant`] in BYTES, because
/// neither record type has a bounded size (see
/// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`]). This entry cap is the second bound
/// each also carries, and the unit the derivation is expressed in.
///
/// The cost of the cap is stated rather than hidden: a tenant whose unsealed
/// tail exceeds 25,000 records does not get the whole tail cached, and
/// because a bound below the working set makes the resolve's two passes evict
/// each other (docs/catalog-and-mvcc.md, and
/// `a_bound_below_the_hot_region_loses_the_saving` in
/// tests/hot_record_cache.rs), such a tenant is back to paying per-record
/// GETs. The levers for it are a coarser `max_flush_delay` or fewer shards,
/// both of which shrink the tail itself; raising the ceiling is a code change
/// today, since no flag exposes it.
pub const MAX_CACHE_CAPACITY_PER_TENANT: usize = (MAX_RECORD_CACHE_BYTES_PER_TENANT
    / (RECORD_CACHE_ENTRY_BYTES * RECORD_CACHES_PER_TENANT))
    as usize;

/// Signal streams whose commit records share one tenant's cache partition.
///
/// The record caches are keyed by `TenantHash` alone
/// (`crate::cache::RecordCache`), not by `(tenant, signal)`, so every signal a
/// tenant ingests holds its unsealed tail in the same LRU and the per-signal
/// tails add up. Shard indices are per (tenant, signal) as well, so the worst
/// case is one record per shard per signal per flush cycle. Six, one per
/// `ravel_types::Signal` variant, pinned against the enum by
/// `signal_streams_matches_the_signal_enum` below so a new variant fails a
/// test rather than silently shrinking the derivation. (Alerts and audit do
/// not fan out over `shard_count` at all: their writers pin fixed shard
/// indices, one shard for alerts and two for audit
/// (`ravel_types::Signal::fixed_read_shards`), so counting them at the full
/// shard count is an over-estimate for any deployment with two or more
/// shards, never an under-estimate.)
pub const SIGNAL_STREAMS: u64 = 6;

/// Hours of a tenant's ingest timeline that can be unsealed at once.
///
/// A fold may seal ingest hour `H` only `max_flush_lifetime +
/// clock_skew_allowance + fold_safety_margin` after `H` ends (ADR-0020),
/// which at the defaults is 1h + 5m + 15m = 1h20m. The oldest unsealed hour
/// can therefore have STARTED 2h20m ago, so an unsealed tail spans up to
/// 2.34 hours of ingest; 3 is that rounded up. Every record in it is read by
/// every query over the tail, because no snapshot part covers it yet.
///
/// Like the flush-cadence term in [`derive_cache_capacity_per_tenant`], this
/// is a lower bound on the tail rather than its worst case: it assumes the
/// default seal parameters ([`DEFAULT_MAX_FLUSH_LIFETIME_NS`] 1h,
/// [`DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`] 5m,
/// [`DEFAULT_FOLD_SAFETY_MARGIN_NS`] 15m), and `max_flush_lifetime` is
/// operator-settable through `--gc-max-flush-lifetime`. At
/// `--gc-max-flush-lifetime 4h` the margin is 4h20m, the oldest unsealed hour
/// can have started 5h20m ago, and the tail spans up to 5.34 hours, which
/// this term under-counts by about 1.8x. Under-counting only ever shrinks the
/// derived capacity, never grows it, so it cannot over-allocate; what it
/// costs is hit rate on the tail that no longer fits, the same trade the byte
/// bound makes above. At the shipped flush cadence
/// [`MAX_CACHE_CAPACITY_PER_TENANT`] decides the result whatever this term
/// says.
pub const HOT_REGION_HOURS: u64 = 3;

/// Bytes one cached record entry is budgeted at, the figure the operator-facing
/// footprints in this module and in docs/guides/operations.md are computed
/// from, and the per-entry rate each cache's byte budget is `capacity` times.
/// See [`DEFAULT_CACHE_CAPACITY_PER_TENANT`] for the breakdown.
///
/// It is a planning rate, not a per-entry cap: a cache charges each entry what
/// that entry actually holds and evicts against the summed charge, so an
/// ordinary 864-byte commit record leaves headroom under it and a record
/// carrying declared column statistics costs more than one of these and takes
/// more than one entry's worth of the budget.
pub const RECORD_CACHE_ENTRY_BYTES: u64 = 900;

/// Number of per-tenant caches [`CatalogConfig::cache_capacity_per_tenant`]
/// sizes: the commit-record cache
/// ([`CatalogConfig::commit_cache_max_bytes_per_tenant`]) and the L1
/// compaction-record cache
/// ([`CatalogConfig::compaction_cache_max_bytes_per_tenant`]). Each gets an
/// equal share, so a per-tenant worst case is this many times capacity times
/// [`RECORD_CACHE_ENTRY_BYTES`].
pub const RECORD_CACHES_PER_TENANT: u64 = 2;

/// Derive the per-tenant record-cache capacity (in entries) from the tenant's
/// shard count and the deployment's configured `max_flush_delay` (issue
/// #1735). See [`DEFAULT_CACHE_CAPACITY_PER_TENANT`] for the rationale and
/// the memory figures. The result is
///
/// ```text
/// shards * SIGNAL_STREAMS * ceil(3600 / max_flush_delay_secs) * HOT_REGION_HOURS
/// ```
///
/// clamped into `DEFAULT_CACHE_CAPACITY_PER_TENANT ..=
/// MAX_CACHE_CAPACITY_PER_TENANT`: one commit record per shard per signal per
/// flush cycle, over as many hours as can be unsealed at once, never below
/// what the old fixed constant guaranteed and never above the capped
/// per-tenant memory footprint. The signal term is load-bearing rather than
/// cosmetic: the caches are partitioned by tenant, not by (tenant, signal),
/// so a tenant ingesting metrics, logs and spans keeps three tails resident
/// at once and a single-signal derivation under-sizes it by that multiple
/// (`a_multi_signal_tenant_resolves_its_unsealed_tail_without_record_gets`
/// in tests/hot_record_cache.rs is red without it).
///
/// At the shipped 2-second cadence the cap is what decides the result,
/// whatever `--shards` is: one shard alone derives `1 * 6 * 1800 * 3` =
/// 32,400, already above the 25,000-entry cap. The shard and cadence terms
/// only move the value at coarser cadences, from about 2.6 seconds at one
/// shard and about 10.4 seconds at four. Do not expect `--shards` to change
/// the number a default-cadence deployment runs with.
///
/// `max_flush_delay` of zero (or a duration so short it would otherwise
/// overflow) saturates to the cap rather than dividing by zero or panicking;
/// a deployment configuring a zero flush delay is already rejected elsewhere
/// (`ravel-server`'s `Cli::validate`), so this is defense in depth, not a
/// path exercised in practice.
pub fn derive_cache_capacity_per_tenant(shards: u32, max_flush_delay: Duration) -> usize {
    let flush_secs = max_flush_delay.as_secs_f64();
    let flushes_per_hour: u64 = if flush_secs > 0.0 {
        (3_600.0 / flush_secs).ceil() as u64
    } else {
        u64::MAX
    };
    let derived = u64::from(shards)
        .saturating_mul(SIGNAL_STREAMS)
        .saturating_mul(flushes_per_hour)
        .saturating_mul(HOT_REGION_HOURS)
        .clamp(
            DEFAULT_CACHE_CAPACITY_PER_TENANT as u64,
            MAX_CACHE_CAPACITY_PER_TENANT as u64,
        );
    usize::try_from(derived).unwrap_or(usize::MAX)
}
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
/// Default total byte budget for the column-statistics reuse cache (issue
/// #905): the decoded `.cstat` objects the resolve path caches per
/// `(tenant, signal)` so a repeated eligible plan against an unchanged folded
/// HEAD skips the stats-object GET (ADR-0850, issue #888). A `.cstat` payload
/// scales with a tenant's declared typed columns times its live segments, and
/// before this budget the map grew one such entry per served tenant for the
/// process lifetime, with nothing reclaiming an active tenant's entry and
/// nothing reporting the footprint. 64 MiB holds a handful of active tenants'
/// current stats at once; the least-recently-used entry is evicted past it and
/// the eviction is counted
/// ([`Catalog::column_stats_cache_evictions`](crate::Catalog::column_stats_cache_evictions)),
/// so an operator who sees that counter climbing knows to raise this.
pub const DEFAULT_COLUMN_STATS_CACHE_MAX_BYTES: u64 = 64 << 20;
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
/// Default `protection_horizon_ns`, mirrored from ravel-maintain's
/// `DEFAULT_PROTECTION_HORIZON_NS` (`max_query_duration` 1h + `grace` 24h +
/// `clock_skew_allowance` 5m = 25h05m). ravel-catalog cannot depend on
/// ravel-maintain (the dependency runs the other way, and ravel-maintain
/// already mirrors [`DEFAULT_MAX_INGEST_LAG_NS`] from this crate for the same
/// reason), so the value is duplicated here and this comment is the sync
/// contract. The fold uses it only to size the bounded retention-frontier
/// reconcile band (ADR-0020, docs/catalog-and-mvcc.md); a drift from a
/// deployment's true horizon only widens or narrows that bounded band and is
/// never a correctness property, because the sweep's HEAD-reachability gate
/// (crates/ravel-maintain/src/retention.rs) is the actual delete blocker.
pub const DEFAULT_PROTECTION_HORIZON_NS: i64 = 25 * 3_600 * 1_000_000_000 + 5 * 60 * 1_000_000_000;
/// Default `frontier_reconcile_max_hours`: the per-fold cap on how many
/// retention-frontier hours the reconcile pass re-lists (ADR-0020, the
/// retirement-frontier half of the delete-blocker mechanism). Deliberately far
/// above `DEFAULT_PROTECTION_HORIZON_NS` expressed in hours (~25), so in steady
/// state (the frontier advances one hour per hour, folds run far more often
/// than once per hour) the fold never defers a frontier hour and every
/// retention tombstone is observed well within the protection horizon of being
/// written. The cap bounds only the recovery case (a shortened retention
/// window or a long-stopped folder produces a backlog of frontier hours): the
/// oldest `frontier_reconcile_max_hours` are reconciled this fold and the
/// remainder is carried to the next fold, never silently skipped
/// ([`crate::FoldReport::frontier_hours_deferred`]). 168 = seven days of
/// hourly buckets.
pub const DEFAULT_FRONTIER_RECONCILE_MAX_HOURS: u32 = 168;
/// Default crossover at which `Catalog::resolve` switches from the non-prefix
/// bounded LIST path to the per-shard recursive prefix scan (ADR-0056).
/// Expressed in per-bucket request units, i.e. the number of `(shard, hour)`
/// buckets the listing suffix spans. Both paths now list one bounded LIST per
/// shard, resuming strictly after the watermark via `start_after`, so both
/// cost `O(objects above the watermark / page_size)` and neither pages
/// through a shard's below-watermark history. The remaining difference is only
/// how each drains a shard: the non-prefix path fans the shards out
/// concurrently and stops each at the first hour past the window; the prefix
/// scan drains them sequentially under a page-by-page request cap, which is
/// what a very wide window wants. 720 is thirty days of hourly buckets at
/// `shard_count = 1`. Purely a performance heuristic -- both paths return
/// identical snapshots and both respect [`DEFAULT_MAX_CATALOG_LIST_REQUESTS`],
/// so any value is correct.
pub const DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS: u64 = 720;
/// Default `resolve_prefix_concurrency`: the number of resolve-path
/// object-store requests one shard-hour commit prefix
/// `t/<tenant_hash>/<signal>/c/<shard>/<ingest_hour>/` may have in flight at
/// once (ADR-1733 decision 1), via a semaphore created for that prefix on
/// demand. 128, derived from a measured S3 GET round trip of about 30ms: 128
/// requests in flight sustain roughly 128 / 0.030s ~= 4,300 GET/s, under S3's
/// published guidance of about 5,500 GET/s per prefix, which is stated per
/// prefix and is what this bound is stated against. Measured end to end on a
/// 10,000-record unsealed tail (one cold resolve each, 10,001 GETs and 13
/// LISTs at every concurrency level -- request count does not move, only the
/// number of concurrency-bound rounds does): 23.157s at 16 (the prior fixed
/// constant), 4.374s at 64, 2.341s at 128.
///
/// It has no CLI flag: the guidance does not vary by deployment, and a flag
/// is added only if a measurement shows a backend that needs one.
pub const DEFAULT_RESOLVE_PREFIX_CONCURRENCY: usize = 128;

/// Upper bound on `resolve_get_concurrency`, the process ceiling, enforced by
/// both [`crate::Catalog::new`] (typed
/// [`CatalogError::InvalidConfig`](crate::CatalogError::InvalidConfig)) and
/// `ravel-server`'s `Cli::validate`. At the ~30ms per-round GET latency
/// [`DEFAULT_RESOLVE_PREFIX_CONCURRENCY`]'s doc comment measures against,
/// 4,096 in flight sustains roughly 4,096 / 0.030s ~= 136,000 GET/s, about
/// twenty-five times S3's published per-prefix guidance of ~5,500 GET/s.
/// Nothing above this is a sane operator value; anything above it is a typo.
/// The bound sits here for that arithmetic, not for tokio's sake: tokio's own
/// ceiling (`Semaphore::MAX_PERMITS`, `usize::MAX >> 3`) is far above it, and
/// only a value past that would panic in `Semaphore::new`; with a bound, both
/// the typo and that extreme fail with a typed error at startup.
pub const MAX_RESOLVE_GET_CONCURRENCY: usize = 4_096;

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
    /// Entry-count bound on decoded commit records cached per tenant, evicted
    /// least-recently-used. `0` disables the cache entirely. Neither record
    /// cache rests on this count alone: entries of both have no bounded size,
    /// so each cache also carries a byte budget of this many times
    /// [`RECORD_CACHE_ENTRY_BYTES`]
    /// ([`commit_cache_max_bytes_per_tenant`](Self::commit_cache_max_bytes_per_tenant),
    /// [`compaction_cache_max_bytes_per_tenant`](Self::compaction_cache_max_bytes_per_tenant)),
    /// and evicts on whichever binds first. Default
    /// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`], which carries the per-entry size
    /// and the hot-region sizing rationale.
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
    /// Total byte budget for the column-statistics reuse cache (issue #905),
    /// the decoded `.cstat` objects cached per `(tenant, signal)` on the
    /// [`crate::Catalog::load_column_stats`] path. The budget is over the bytes
    /// actually held
    /// ([`LoadedColumnStats::heap_bytes`](crate::column_stats_resolve::LoadedColumnStats::heap_bytes)),
    /// not an entry count: a `.cstat` payload's size varies by orders of
    /// magnitude across tenants, so an entry-count bound would not bound memory.
    /// When admitting a freshly resolved object would exceed this, the
    /// least-recently-used entries are evicted first, each counted by
    /// [`crate::Catalog::column_stats_cache_evictions`], so an undersized budget
    /// is observable rather than silent; an object larger than the whole budget
    /// is not cached at all and counted by
    /// [`crate::Catalog::column_stats_cache_refusals`]. Eviction only ever
    /// forces a cache miss and a re-resolve, never a partial or stale statistic
    /// reaching the exact MIN/MAX path.
    ///
    /// `0` is the disabled sentinel, matching
    /// [`byte_cache_max_bytes`](Self::byte_cache_max_bytes): [`crate::Catalog::new`]
    /// then builds no column-stats cache at all, so every eligible load
    /// re-fetches the stats object with no reuse and no eviction accounting.
    /// Default [`DEFAULT_COLUMN_STATS_CACHE_MAX_BYTES`].
    pub column_stats_cache_max_bytes: u64,
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
    /// switches from the non-prefix bounded LIST path to the per-shard
    /// recursive prefix scan (ADR-0056). When the listing suffix spans at least
    /// this many buckets (`shard_count * listing_hours`), the prefix scan is
    /// used. Both paths issue one bounded `list_after` per shard, resuming
    /// strictly after the watermark, so neither pages through a shard's
    /// below-watermark history; narrower windows keep the non-prefix path,
    /// which fans the shards out concurrently and stops each at the first hour
    /// past the window, while the prefix scan drains them sequentially under a
    /// page-by-page request cap. A performance heuristic only: both paths
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
    /// Protection horizon in nanoseconds, used by the fold ONLY to size the
    /// bounded retention-frontier reconcile band (ADR-0020,
    /// docs/catalog-and-mvcc.md). The frontier reconcile catches a retention
    /// tombstone that lands in an hour `R` days behind the watermark, far
    /// outside [`fold_reconcile_window_hours`](Self::fold_reconcile_window_hours):
    /// the fold re-lists snapshot-named hours at or approaching the tenant's
    /// retirement frontier (derived from the tenant's durable retention window
    /// and this horizon) so the snapshot stops naming a bucket before the
    /// retention sweep's own horizon lets it delete that bucket's objects. Not
    /// a correctness input: a value that drifts from a deployment's true
    /// horizon only resizes the bounded band, and the sweep's HEAD-reachability
    /// gate is the actual delete blocker. Default
    /// [`DEFAULT_PROTECTION_HORIZON_NS`].
    pub protection_horizon_ns: i64,
    /// Per-fold cap on retention-frontier hours the reconcile pass re-lists
    /// (ADR-0020). Bounds the recovery case (a shortened retention window or a
    /// long-stopped folder): the oldest capped hours are reconciled this fold
    /// and the remainder is carried to the next fold and reported on
    /// [`crate::FoldReport::frontier_hours_deferred`], never silently skipped.
    /// Default [`DEFAULT_FRONTIER_RECONCILE_MAX_HOURS`].
    pub frontier_reconcile_max_hours: u32,
    /// Ceiling on the object-store requests (LISTs and record GETs) this
    /// `Catalog` keeps in flight across every tenant, shard and hour at once,
    /// via a per-instance semaphore (`Catalog::request_semaphore`) every
    /// resolve-path request takes a permit from (ADR-1733 decision 1).
    ///
    /// `ravel-server` builds exactly one `Catalog` and shares it (via `Arc`
    /// clone, so one underlying semaphore) across every request, so this is
    /// the process ceiling there, and `--catalog-resolve-concurrency` sets
    /// it; unset, `ravel-server` derives it from the process's query
    /// concurrency (ADR-1733 decision 2). A CLI invocation or a test that
    /// builds its own `Catalog` gets a per-instance ceiling instead, which is
    /// what a single-process-per-invocation caller wants.
    ///
    /// This is a ceiling on the aggregate, not a per-prefix bound: what any
    /// one shard-hour prefix may have in flight is
    /// [`Self::resolve_prefix_concurrency`], and a request holds both permits
    /// before it is issued. Must be greater than zero;
    /// [`crate::Catalog::new`] rejects `0` with
    /// [`CatalogError::InvalidConfig`](crate::CatalogError::InvalidConfig)
    /// rather than silently clamping it to 1, and rejects anything above
    /// [`MAX_RESOLVE_GET_CONCURRENCY`]. Default
    /// [`DEFAULT_RESOLVE_PREFIX_CONCURRENCY`]: one prefix's worth, the value
    /// a `Catalog` resolving a single shard-hour can use anyway.
    pub resolve_get_concurrency: usize,
    /// Number of resolve-path requests any one shard-hour commit prefix
    /// `t/<tenant_hash>/<signal>/c/<shard>/<ingest_hour>/` may have in flight
    /// at once (ADR-1733 decision 1). Requests that do not sit under such a
    /// prefix (the head object, snapshot parts and postings, and every LIST
    /// whose prefix is broader than one shard-hour) take only
    /// [`Self::resolve_get_concurrency`]'s permit.
    ///
    /// Prefix semaphores are created on demand and dropped once the prefix
    /// goes idle, so the map holds one entry per prefix in flight rather than
    /// one per prefix in the bucket's history. Must be greater than zero;
    /// [`crate::Catalog::new`] rejects `0` the same way it rejects a zero
    /// ceiling. Default [`DEFAULT_RESOLVE_PREFIX_CONCURRENCY`].
    pub resolve_prefix_concurrency: usize,
}

impl CatalogConfig {
    /// One record cache's equal share of the per-tenant byte budget:
    /// [`cache_capacity_per_tenant`](Self::cache_capacity_per_tenant) times
    /// [`RECORD_CACHE_ENTRY_BYTES`]. 22.5 MB at the 25,000-entry cap, 9 MB at
    /// the 10,000-entry floor `--disable-cache` holds. Two caches hold a share
    /// each ([`RECORD_CACHES_PER_TENANT`]), so the per-tenant total is twice
    /// this: [`MAX_RECORD_CACHE_BYTES_PER_TENANT`] at the cap.
    fn record_cache_share_bytes_per_tenant(&self) -> u64 {
        u64::try_from(self.cache_capacity_per_tenant)
            .unwrap_or(u64::MAX)
            .saturating_mul(RECORD_CACHE_ENTRY_BYTES)
    }

    /// Byte budget the commit-record cache is held to, per tenant: one share
    /// of the per-tenant budget
    /// ([`record_cache_share_bytes_per_tenant`](Self::record_cache_share_bytes_per_tenant)).
    ///
    /// That cache cannot be bounded by an entry count alone:
    /// `CommitRecord.declared_column_stats` is a repeated field with no cap in
    /// the format and none in validation, so one entry can cost many times
    /// [`RECORD_CACHE_ENTRY_BYTES`] (see
    /// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`]). `crate::cache::RecordCache`
    /// charges each entry an estimate of its live heap and evicts
    /// least-recently-used until the sum is within this.
    ///
    /// `0` follows the `cache_capacity_per_tenant == 0` disabled sentinel:
    /// nothing is admitted at all.
    pub fn commit_cache_max_bytes_per_tenant(&self) -> u64 {
        self.record_cache_share_bytes_per_tenant()
    }

    /// Byte budget the L1 compaction-record cache is held to, per tenant: the
    /// other share of the same budget
    /// ([`record_cache_share_bytes_per_tenant`](Self::record_cache_share_bytes_per_tenant)).
    ///
    /// That cache cannot be bounded by an entry count either: a
    /// `CompactionRecord` carries one input identity per compacted L0 segment
    /// with no cap in the format, so one entry can cost hundreds of times
    /// [`RECORD_CACHE_ENTRY_BYTES`] (see
    /// [`DEFAULT_CACHE_CAPACITY_PER_TENANT`]). `crate::cache::CompactionRecordCache`
    /// charges each entry an estimate of its live heap and evicts oldest-first
    /// until the sum is within this.
    ///
    /// `0` follows the `cache_capacity_per_tenant == 0` disabled sentinel:
    /// nothing stays resident.
    pub fn compaction_cache_max_bytes_per_tenant(&self) -> u64 {
        self.record_cache_share_bytes_per_tenant()
    }
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
            column_stats_cache_max_bytes: DEFAULT_COLUMN_STATS_CACHE_MAX_BYTES,
            max_catalog_list_requests: DEFAULT_MAX_CATALOG_LIST_REQUESTS,
            prefix_list_crossover_requests: DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS,
            snapshot_part_max_entries: DEFAULT_SNAPSHOT_PART_MAX_ENTRIES,
            fold_bucket_concurrency: DEFAULT_FOLD_BUCKET_CONCURRENCY,
            fold_reconcile_window_hours: DEFAULT_FOLD_RECONCILE_WINDOW_HOURS,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            frontier_reconcile_max_hours: DEFAULT_FRONTIER_RECONCILE_MAX_HOURS,
            resolve_get_concurrency: DEFAULT_RESOLVE_PREFIX_CONCURRENCY,
            resolve_prefix_concurrency: DEFAULT_RESOLVE_PREFIX_CONCURRENCY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ravel_types::Signal;

    /// Pins the derivation at the shipped ingest defaults (`shard_count` 4,
    /// `max_flush_delay` 2s, issue #1735): `4 shards * 6 signals * ceil(3600
    /// / 2) flushes * 3 hours = 129_600` uncapped, which the 45 MB per-tenant
    /// budget caps at 25,000.
    #[test]
    fn derive_at_ingest_defaults_is_the_cap() {
        let uncapped = 4 * SIGNAL_STREAMS * 1_800 * HOT_REGION_HOURS;
        assert_eq!(uncapped, 129_600, "the shipped defaults derive this much");
        assert!(
            uncapped > MAX_CACHE_CAPACITY_PER_TENANT as u64,
            "sanity: the cap is what decides the shipped default"
        );
        assert_eq!(
            derive_cache_capacity_per_tenant(4, Duration::from_secs(2)),
            25_000
        );
    }

    /// The signal term is what the fix adds (issue #1735 fix round): at one
    /// shard on a 4-second cadence, `1 * 6 * 900 * 3 = 16_200` entries, clear
    /// of both the floor and the cap, where the single-signal derivation
    /// (`1 * 900 * 3 = 2_700`) would have fallen back to the 10,000 floor.
    #[test]
    fn derive_folds_the_signal_count_in() {
        assert_eq!(
            derive_cache_capacity_per_tenant(1, Duration::from_secs(4)),
            16_200
        );
        assert_eq!(
            16_200 / SIGNAL_STREAMS as usize,
            2_700,
            "without the signal term the derivation would floor at 10,000 instead"
        );
    }

    /// A single shard on a coarse hour-long flush cadence derives well below
    /// the old fixed constant; the floor keeps it from going lower still.
    #[test]
    fn derive_never_goes_below_the_default_floor() {
        assert_eq!(
            derive_cache_capacity_per_tenant(1, Duration::from_secs(3_600)),
            DEFAULT_CACHE_CAPACITY_PER_TENANT
        );
    }

    /// A zero flush delay would otherwise divide by zero; it must saturate
    /// into the cap, not into `usize::MAX`.
    #[test]
    fn derive_saturates_on_a_zero_flush_delay() {
        assert_eq!(
            derive_cache_capacity_per_tenant(4, Duration::ZERO),
            MAX_CACHE_CAPACITY_PER_TENANT
        );
    }

    /// `--shards 64`, the case that motivated the cap: 2,073,600 entries
    /// uncapped, 3.7 GB per actively-queried tenant across the two caches the
    /// bound sizes. Capped it is 25,000 entries and exactly the 45 MB budget,
    /// the figure docs/guides/operations.md quotes.
    #[test]
    fn a_64_shard_deployment_is_capped_at_the_stated_memory_budget() {
        let uncapped = 64 * SIGNAL_STREAMS * 1_800 * HOT_REGION_HOURS;
        assert_eq!(uncapped, 2_073_600);
        assert_eq!(
            uncapped * RECORD_CACHE_ENTRY_BYTES * RECORD_CACHES_PER_TENANT,
            3_732_480_000,
            "uncapped, one tenant would cost 3.7 GB across both caches"
        );

        let capped = derive_cache_capacity_per_tenant(64, Duration::from_secs(2));
        assert_eq!(capped, 25_000);
        assert_eq!(
            capped as u64 * RECORD_CACHE_ENTRY_BYTES * RECORD_CACHES_PER_TENANT,
            MAX_RECORD_CACHE_BYTES_PER_TENANT,
            "the capped capacity is exactly the 45 MB per-tenant budget"
        );
        assert_eq!(MAX_RECORD_CACHE_BYTES_PER_TENANT, 45_000_000);
    }

    /// The clamp cannot invert: the cap sits above the floor.
    #[test]
    fn the_cap_is_above_the_floor() {
        const { assert!(MAX_CACHE_CAPACITY_PER_TENANT > DEFAULT_CACHE_CAPACITY_PER_TENANT) };
    }

    /// `SIGNAL_STREAMS` is the `Signal` variant count, not a number that
    /// drifts. The exhaustive match fails to compile on a new variant, and
    /// the length assertion fails if the constant is not updated with it.
    #[test]
    fn signal_streams_matches_the_signal_enum() {
        let all = [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ];
        for signal in all {
            match signal {
                Signal::Metrics
                | Signal::Logs
                | Signal::Spans
                | Signal::Profiles
                | Signal::Alerts
                | Signal::Audit => {}
            }
        }
        assert_eq!(all.len() as u64, SIGNAL_STREAMS);
    }
}
