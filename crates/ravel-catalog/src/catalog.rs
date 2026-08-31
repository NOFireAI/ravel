//! Snapshot resolution: listing-based discovery over commit records
//! (docs/catalog-and-mvcc.md "Snapshot resolution", ADR-0003, ADR-0010 §2/§10).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use parking_lot::Mutex;
use prost::Message;
use ravel_cache::{
    Cache, CacheKey, CacheLimits, DiskCache, SingleFlightError, Source, TieredCache,
};
use ravel_commit::keys::BucketEntry;
use ravel_commit::{erasure, keys, record, signal};
use ravel_object_store::{GetOutcome, GetRange, ObjectMeta, ObjectStoreBackend, StoreError};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, CompactionRecord, ErasureRequest, RewriteRecord,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use tracing::Instrument;
use uuid::Uuid;

use crate::cache::{CompactionRecordCache, HeadCache, PartCache, PostingsCache, RecordCache};
use crate::column_stats_resolve::{self, LoadColumnStatsError, LoadedColumnStats};
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::provisioning::ShardGeneration;
use crate::snapshot::{SegmentLevel, SegmentOrigin, SegmentOrigins, SegmentRef, Snapshot};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Bound on the object-store requests one resolve keeps in flight at once.
/// A cold resolve issues one LIST per (shard, hour) plus one
/// GET per uncached commit/compaction record and per snapshot part; these
/// used to run strictly one await at a time. They now run concurrently up to
/// this bound, held by [`Catalog::request_semaphore`] and acquired only
/// around a single leaf request, so the fan-out collapses from k sequential
/// round trips toward ceil(k / this) without ever exceeding it.
pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 16;
/// One shard's listed commit-bucket entries, keyed by `(shard, ingest_hour)`,
/// as produced by [`Catalog::list_shard_hours`] and merged in
/// [`Catalog::list_window_bounded`].
type ShardBuckets = HashMap<(u32, u32), Vec<ObjectMeta>>;
/// Delay before the single retry on an exact `min_token` GET
/// (docs/catalog-and-mvcc.md step 4: "GET it directly ... with one retry").
/// `MemoryStore` is strongly consistent so tests never observe this delay;
/// it exists for real backends with brief propagation lag.
const MIN_TOKEN_RETRY_DELAY: Duration = Duration::from_millis(20);
/// Upper bound on store requests `Catalog::resolve_snapshot_window` issues
/// before any LIST runs, folded into `Catalog::estimated_catalog_requests`
/// (ADR-0044 decision 3): one HEAD GET, always attempted whenever the window
/// is non-empty (`resolve_impl` calls it unconditionally); plus one GET per
/// part a usable HEAD names, capped here at 1 part because every writer
/// today emits exactly one ("v1 writes exactly
/// one part; readers accept N parts" is an unused sharding escape hatch);
/// plus one postings GET, worst case, when the query has an equality
/// `__name__` filter.
///
/// This is not a structural bound: `SnapshotHead.parts` is `repeated` in the
/// wire format, and `resolve_snapshot_window` issues one GET per named part
/// regardless of how many there are. Part count is only knowable after the
/// HEAD GET this constant is trying to avoid, so a future multi-part writer
/// (the escape hatch the wire format reserves) would silently make
/// `estimated_catalog_requests` an under-estimate again. Flagged as an open
/// gap in ADR-0044 decision 3, not resolved here.
const SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND: u64 = 3;
/// One LIST of `t/<tenant_hash>/<signal>/del/` (ADR-0064 decision 2), issued
/// unconditionally by every resolve regardless of whether the query's window
/// is empty. Folded into `Catalog::estimated_catalog_requests` unconditionally
/// too, unlike [`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`] which only applies
/// when the window is non-empty: an empty-window resolve still issues this
/// LIST, so `estimated_catalog_requests` must count it on the `None` branch
/// as well or it stops being a true upper envelope.
const PENDING_ERASURE_LIST_UPPER_BOUND: u64 = 1;

/// The catalog's raw-byte cache (ADR-0046 decisions 1-3), in one of the two
/// tier configurations `Catalog::fetch_content_addressed` reads through.
///
/// - [`ByteCache::Ram`] is the RAM tier alone, consulted with the plain
///   get-then-insert idiom the five decoded caches use. This is the only
///   configuration [`Catalog::new`] builds: with no local `--cache-dir` a
///   query node has no disk tier (ADR-0046 decision 3), and building one here
///   would both write files in production before #97 wires the flag and
///   require a Tokio runtime at construction ([`ravel_cache::DiskCache::new`]),
///   which `Catalog::new` does not promise.
/// - [`ByteCache::Tiered`] is the RAM-over-disk [`TieredCache`] read through
///   [`TieredCache::get_or_fetch`], so a disk-served hit is single-flighted and
///   corruption-gated exactly as ADR-0046 decisions 3-5 require. A disk tier is
///   attached here (in #97, and in this crate's tests today); the read funnel
///   already routes through it the moment it is present.
///
/// The two variants carry different fetch-error types on purpose. The RAM path
/// never constructs a fetch-closure error (it does its own get-then-insert), so
/// its `E` stays `Infallible`, exactly as before this cache gained a disk tier.
/// The tiered path's `get_or_fetch` closure runs the upstream store GET, whose
/// error is `StoreError`; the tiered single-flight (like [`Cache`] itself)
/// requires that error type to be `Clone` so one leader's error reaches every
/// follower, and `StoreError` is deliberately not `Clone`, so it is threaded as
/// `Arc<StoreError>` and `fetch_content_addressed` reconstructs the owned
/// `StoreError` its callers match on before returning.
enum ByteCache {
    /// RAM tier only (no disk configured). The plain get-then-insert path.
    Ram(Cache<std::convert::Infallible>),
    /// RAM over local disk. The `get_or_fetch` read-through path. Constructed
    /// in production by [`Catalog::with_disk_byte_cache`] (the server's
    /// `--cache-dir` wiring, #97) and under test by
    /// `set_tiered_byte_cache_for_test`; the read funnel routes through it
    /// wherever it is present.
    Tiered(TieredCache<Arc<StoreError>>),
}

/// Reconstruct an owned [`StoreError`] from a borrowed one. The tiered byte
/// cache threads its upstream fetch error as `Arc<StoreError>` because the
/// single-flight requires a `Clone` error type and `StoreError` is not `Clone`;
/// `fetch_content_addressed`'s callers match on an owned `StoreError`, so the
/// shared error is rebuilt here rather than cloned. The exhaustive match is
/// deliberate: a new `StoreError` variant is a compile error here, not a silent
/// fall-through, so this stays a faithful copy of the taxonomy it mirrors.
fn clone_store_error(err: &StoreError) -> StoreError {
    match err {
        StoreError::NotFound => StoreError::NotFound,
        StoreError::AlreadyExists => StoreError::AlreadyExists,
        StoreError::PreconditionFailed => StoreError::PreconditionFailed,
        StoreError::AccessDenied(msg) => StoreError::AccessDenied(msg.clone()),
        StoreError::Throttled { retry_after_ms } => StoreError::Throttled {
            retry_after_ms: *retry_after_ms,
        },
        StoreError::Timeout => StoreError::Timeout,
        StoreError::Corrupted(msg) => StoreError::Corrupted(msg.clone()),
        StoreError::InvalidRange(msg) => StoreError::InvalidRange(msg.clone()),
        StoreError::Transient(msg) => StoreError::Transient(msg.clone()),
        StoreError::Permanent(msg) => StoreError::Permanent(msg.clone()),
    }
}

/// One tenant/signal's last-resolved column-statistics object, cached so a
/// repeated eligible plan reuses it instead of re-fetching the same object
/// (ADR-0850, issue #888). Keyed for validity by the object's own content hash
/// (`stats_blake3`) AND the folded HEAD's covered part set (`part_blake3`):
/// the stats object is bound to the CURRENT folded HEAD, not the pinned query
/// snapshot, so a fold that changes either value must re-resolve. Every load
/// still reads HEAD (one GET) to learn the current `(blake3, parts)`, so a
/// changed fold is always detected; the cache only ever avoids the SECOND GET,
/// the stats-object fetch, and only when both keys still match. The covered
/// part set is compared against the loaded object's own `part_blake3` (which
/// `heap_bytes` charges), never a second copy: a duplicate buffer here would
/// hold 32 uncharged bytes per covered part per entry, silently exceeding the
/// budget.
struct CachedColumnStats {
    stats_blake3: [u8; 32],
    stats: Arc<LoadedColumnStats>,
    /// The entry's charge against the budget, cached at insert time so the
    /// running total and an eviction's refund never re-walk the object. Equal
    /// to [`LoadedColumnStats::heap_bytes`] of `stats`.
    bytes: u64,
    /// Recency tick for LRU eviction: the entry with the smallest value is the
    /// least recently used. Stamped on insert and refreshed on every cache hit.
    last_used: u64,
}

/// Byte-budgeted, process-wide reuse cache for resolved column-statistics
/// objects (issue #905), one entry per `(tenant, signal)`. It fronts the
/// stats-object GET on [`Catalog::load_column_stats`] the same way
/// [`CachedColumnStats`] describes, but with a total byte budget so a
/// multi-tenant process cannot accumulate one unbounded entry per served
/// tenant for its lifetime.
///
/// The budget is over the bytes actually held
/// ([`LoadedColumnStats::heap_bytes`]), not an entry count: a `.cstat`
/// payload's size varies by orders of magnitude across tenants, so an
/// entry-count bound would not bound memory. Admitting an object that would
/// push the total over `max_bytes` first evicts least-recently-used entries,
/// each counted in `evictions`; an object larger than the whole budget is not
/// cached at all and counted in `refusals`. Both counters make an undersized
/// budget observable: they climb while the statistics plainly exist, which an
/// operator reads apart from ABSENT statistics (a load returning `Ok(None)`,
/// leaving both flat). Eviction only ever forces a later cache miss and a
/// re-resolve; a returned entry is still gated on the exact
/// `(stats_blake3, part_blake3)` binding, so no partial or stale statistic
/// reaches the exact MIN/MAX path (issue #905 deliverable 4, guarding against a
/// third route past #970/#973).
struct ColumnStatsCache {
    /// Total byte budget. Always positive for a live cache: a `0`
    /// `column_stats_cache_max_bytes` builds no cache at all (the catalog holds
    /// `None`), so this type is never constructed with a zero budget.
    max_bytes: u64,
    state: Mutex<ColumnStatsCacheState>,
    /// Entries dropped to keep the held bytes within `max_bytes`, cumulative.
    evictions: AtomicU64,
    /// Loads whose object alone exceeds `max_bytes`, so no eviction could ever
    /// make room and the object is served but not cached, cumulative.
    refusals: AtomicU64,
}

struct ColumnStatsCacheState {
    entries: HashMap<(TenantHash, Signal), CachedColumnStats>,
    /// Sum of every live entry's `bytes`; the invariant this cache bounds.
    held_bytes: u64,
    /// Monotonic access counter; the next recency stamp. Incremented on every
    /// lookup and insert so `last_used` totally orders the entries by recency.
    tick: u64,
}

impl ColumnStatsCache {
    fn new(max_bytes: u64) -> Self {
        ColumnStatsCache {
            max_bytes,
            state: Mutex::new(ColumnStatsCacheState {
                entries: HashMap::new(),
                held_bytes: 0,
                tick: 0,
            }),
            evictions: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    /// Return the cached object for `key` only when its content hash AND covered
    /// part set both still match a freshly resolved HEAD (the issue #888
    /// binding), refreshing the entry's recency. A mismatch or a miss returns
    /// `None`, and the caller re-fetches; nothing stale is ever handed back.
    fn get(
        &self,
        key: (TenantHash, Signal),
        stats_blake3: &[u8; 32],
        part_blake3: &[[u8; 32]],
    ) -> Option<Arc<LoadedColumnStats>> {
        let mut state = self.state.lock();
        state.tick += 1;
        let tick = state.tick;
        let entry = state.entries.get_mut(&key)?;
        if entry.stats_blake3 == *stats_blake3 && entry.stats.part_blake3 == part_blake3 {
            entry.last_used = tick;
            Some(Arc::clone(&entry.stats))
        } else {
            None
        }
    }

    /// Admit a freshly resolved object for `key`, evicting least-recently-used
    /// entries as needed to keep the held bytes within `max_bytes`. Replaces any
    /// existing entry for `key`. An object larger than the whole budget is not
    /// cached (a `refusals` bump); it is still returned to the caller by
    /// `load_column_stats`, so refusing to cache never changes an answer.
    fn insert(
        &self,
        key: (TenantHash, Signal),
        stats_blake3: [u8; 32],
        stats: Arc<LoadedColumnStats>,
    ) {
        let bytes = stats.heap_bytes();
        let mut state = self.state.lock();
        state.tick += 1;
        let tick = state.tick;

        // Replacing an existing entry: refund its bytes before accounting the
        // new one, so a re-resolve of the same key never double-counts.
        if let Some(old) = state.entries.remove(&key) {
            state.held_bytes = state.held_bytes.saturating_sub(old.bytes);
        }

        // An object larger than the entire budget can never be made to fit by
        // evicting others: refuse it loudly rather than evict everything for
        // nothing. The caller still gets the object; it is just not cached.
        if bytes > self.max_bytes {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Evict the least-recently-used entry until the new one fits. `bytes <=
        // max_bytes` guarantees this terminates with room, at worst after the
        // map empties.
        while state.held_bytes + bytes > self.max_bytes {
            let victim = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(k, _)| *k);
            match victim {
                Some(victim_key) => {
                    if let Some(removed) = state.entries.remove(&victim_key) {
                        state.held_bytes = state.held_bytes.saturating_sub(removed.bytes);
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            }
        }

        state.held_bytes += bytes;
        state.entries.insert(
            key,
            CachedColumnStats {
                stats_blake3,
                stats,
                bytes,
                last_used: tick,
            },
        );
    }

    /// Drop every entry whose tenant is in `idle`, refunding its bytes. Called
    /// from [`Catalog::evict_idle_tenants`] alongside the other per-tenant
    /// caches; an idle-tenant drop is not counted as a budget eviction.
    fn evict_tenants(&self, idle: &[TenantHash]) {
        let mut state = self.state.lock();
        let mut freed = 0u64;
        state.entries.retain(|(tenant, _), entry| {
            if idle.contains(tenant) {
                freed += entry.bytes;
                false
            } else {
                true
            }
        });
        state.held_bytes = state.held_bytes.saturating_sub(freed);
    }

    fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }

    fn held_bytes(&self) -> u64 {
        self.state.lock().held_bytes
    }
}

/// Listing-based catalog over an object store backend (Phase 1, ADR-0003).
/// A future compaction phase folds commit records into immutable snapshot
/// objects behind a CAS'd HEAD pointer; this type's public API does not
/// change when that lands.
pub struct Catalog {
    store: Arc<dyn ObjectStoreBackend>,
    config: CatalogConfig,
    cache: RecordCache,
    compaction_cache: CompactionRecordCache,
    head_cache: HeadCache,
    part_cache: PartCache,
    postings_cache: PostingsCache,
    /// The byte cache (ADR-0046 decisions 1-3): raw bytes of content-addressed
    /// objects (snapshot parts, postings), keyed by
    /// `(tenant_hash, content_hash, offset, len)`, consulted at
    /// [`Catalog::fetch_content_addressed`] before a store GET. See
    /// [`ByteCache`] for the two tier configurations. [`Catalog::new`] always
    /// builds the RAM-only [`ByteCache::Ram`] variant; a disk tier
    /// ([`ByteCache::Tiered`]) is attached separately (#97, and this crate's
    /// tests). The RAM path is still the plain get-then-insert idiom the five
    /// decoded caches use, so nothing about the not-yet-disk-configured path
    /// changes from before this cache gained a disk tier.
    ///
    /// The catalog HEAD is structurally excluded from this cache in both
    /// variants: `SnapshotHead` carries no content hash, so `read_head` has no
    /// value to pass `fetch_content_addressed`, the one function that reaches
    /// the byte cache, and never calls it. HEAD is CAS-written and must never
    /// be admitted; keeping it out is the type's job, not a rule's.
    ///
    /// `None` when [`CatalogConfig::byte_cache_max_bytes`] is `0`: the byte
    /// cache is then absent entirely (not a zero-capacity cache), so
    /// `fetch_content_addressed` reads every object straight through
    /// [`Catalog::guarded_get`] with no cache hit/miss accounting, exactly as
    /// a build with no byte-cache wiring would. This is how the server's
    /// `--disable-cache` disables the catalog byte cache alongside the fetcher
    /// cache.
    byte_cache: Option<ByteCache>,
    /// Count of unlisted L0 records observed postdating a compaction record
    /// in their bucket (docs/catalog-and-mvcc.md step 3: an interlock
    /// breach, since a flush should have sealed before compaction ran). The
    /// segment is still included for correctness (overlap harmlessness); the
    /// counter surfaces the anomaly to metrics.
    interlock_violations: AtomicU64,
    /// Count of buckets observed holding two compaction records with
    /// different `input_set_hash` (docs/catalog-and-mvcc.md step 3, §3.6 row
    /// 11: a sealed bucket must yield exactly one input set). Both parts
    /// sets plus all uncovered L0s are still included (harmless overlap);
    /// the counter surfaces the invariant breach for a human to investigate.
    compaction_input_set_conflicts: AtomicU64,
    /// Count of buckets observed holding two or more live (non-superseded)
    /// `RewriteRecord`s neither superseding the other (ADR-0064 decision 3
    /// point 5). Unlike two compaction records, this is NOT a harmless
    /// overlap: a rewrite's output deliberately lacks records its sibling's
    /// input set still carries, so both parts sets being included resurrects
    /// each rewrite's own erased subject through the other's un-rewritten
    /// copy. Normal operation batches every pending request for a bucket into
    /// one rewrite (Consequences), so two live siblings should never occur;
    /// this counter exists to surface it immediately if it ever does, rather
    /// than let it pass as ordinary overlap.
    rewrite_sibling_conflicts: AtomicU64,
    /// Count of hard isolation-breach failures observed: a HEAD or postings
    /// object whose `tenant_hash` does not match the requesting tenant, or a
    /// listing helper result whose key does not begin with the requesting
    /// tenant's prefix (ADR-0050 §2). Unlike the two counters above, each of
    /// these also fails the query: the count is a record of hard failures,
    /// not a harmless-overlap anomaly tally.
    isolation_breaches: AtomicU64,
    /// Bounds the object-store requests one resolve keeps in flight. Ephemeral, process-local, correctness-free: it changes only
    /// how many round trips overlap, never which segments a resolve returns.
    request_semaphore: Arc<tokio::sync::Semaphore>,
    /// Whether resolve validates the configured `shard_count` against each
    /// (tenant, signal)'s durable provisioning record (ADR-0050 section 5). Off for the many in-crate and `ravel-query`/`ravel-sql` callers
    /// that build a `Catalog` directly; the server turns it on
    /// ([`Catalog::with_provisioning_enforcement`]) so a query for a tenant
    /// whose record disagrees fails with a typed error instead of silently
    /// resolving over `0..shard_count` and dropping the missing shards.
    enforce_provisioning: bool,
    /// The (tenant, signal) pairs already validated against their provisioning
    /// record this process, so the read-path check is one store GET per pair,
    /// not one per resolve. Read-only enforcement: a record disagreement fails
    /// the query and is never adopted here (adoption is an ingest/maintain/CLI
    /// action; a query-only node may hold write-restricted credentials).
    provisioning_checked: Mutex<HashSet<(TenantHash, Signal)>>,
    /// Last-touch wall-clock (`now_ns`) per tenant, stamped at the start of
    /// every [`Catalog::resolve_impl`] (ADR-0069 decision 2). The
    /// idle-tenant sweep loop reads it to decide which tenants' re-derivable
    /// per-tenant cache entries to evict, and stamps nothing itself, so no
    /// clock is read in this crate. A tenant absent from this map has never
    /// resolved through this catalog, so it holds no per-tenant cache state to
    /// evict; a present-but-idle tenant is evicted by
    /// [`Catalog::evict_idle_tenants`]. `now_ns` is caller-supplied and may go
    /// backwards across callers with skewed clocks, which only ever delays an
    /// eviction, never causes a wrong one.
    tenant_activity: Mutex<HashMap<TenantHash, i64>>,
    /// Last-resolved column-statistics object per `(tenant, signal)` (ADR-0850,
    /// issue #888). Consulted in [`Catalog::load_column_stats`] after the HEAD
    /// GET and before the stats-object GET, so a repeated eligible plan against
    /// an unchanged folded HEAD reuses the decoded object instead of
    /// re-fetching it.
    ///
    /// Reclamation runs on both dimensions, which differ. ENTRY COUNT is
    /// bounded by (tenants x signals) and [`Catalog::evict_idle_tenants`]
    /// removes a tenant's entries once it passes the idle TTL. ENTRY SIZE is
    /// bounded by [`ColumnStatsCache`]'s byte budget
    /// ([`CatalogConfig::column_stats_cache_max_bytes`], issue #905): a
    /// `LoadedColumnStats` holds one `ColumnStatsSegment` per live segment, so
    /// an ACTIVE tenant's entry grows with that tenant, and without a byte
    /// budget an active tenant's entry was never reclaimed. The cache accounts
    /// the bytes actually held and evicts the least-recently-used entry past the
    /// budget, counting the eviction so an undersized budget is observable.
    ///
    /// `None` when [`CatalogConfig::column_stats_cache_max_bytes`] is `0`: the
    /// cache is then absent entirely, so every eligible load re-fetches the
    /// stats object with no reuse, matching the `byte_cache_max_bytes == 0`
    /// disabled sentinel.
    column_stats_cache: Option<ColumnStatsCache>,
}

/// Adapts [`Catalog::guarded_get`] to the provisioning module's
/// [`crate::provisioning::AccountedRecordGet`], so the resolve-path provisioning
/// reads (the generation history and the `shard_count` enforcement check) run
/// through the same semaphore-bounded, accounted GET funnel as every other
/// resolve GET (issue #729) rather than a raw `store.get`. Holds only borrows,
/// so it is built cheaply per read from the resolve's own `&self` and
/// `accounting`.
struct GuardedRecordGet<'a> {
    catalog: &'a Catalog,
    accounting: &'a QueryAccounting,
}

impl crate::provisioning::AccountedRecordGet for GuardedRecordGet<'_> {
    async fn accounted_get_full(&self, key: &str) -> Result<GetOutcome, StoreError> {
        self.catalog
            .guarded_get(key, GetRange::Full, self.accounting)
            .await
    }
}

impl Catalog {
    /// Errors if `config.shard_count == 0` (a resolvable catalog needs at
    /// least one shard).
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        config: CatalogConfig,
    ) -> Result<Self, CatalogError> {
        if config.shard_count == 0 {
            return Err(CatalogError::InvalidConfig);
        }
        // `byte_cache_max_bytes == 0` is the disabled sentinel:
        // build no byte cache at all rather than a zero-capacity one, so the
        // resolve path reads straight through the store with no RAM tier and no
        // byte-cache accounting, byte-for-byte a build with no byte-cache
        // wiring. Any other value builds the cache at the configured limits.
        let byte_cache = (config.byte_cache_max_bytes != 0).then(|| {
            ByteCache::Ram(Cache::new(CacheLimits::new(
                config.byte_cache_max_bytes,
                config.byte_cache_max_entries,
                config.byte_cache_max_entry_bytes,
            )))
        });
        // `column_stats_cache_max_bytes == 0` is the disabled sentinel,
        // matching `byte_cache_max_bytes`: build no cache at all, so every
        // eligible load re-fetches the stats object with no reuse.
        let column_stats_cache = (config.column_stats_cache_max_bytes != 0)
            .then(|| ColumnStatsCache::new(config.column_stats_cache_max_bytes));
        Ok(Catalog {
            store,
            config,
            cache: RecordCache::default(),
            compaction_cache: CompactionRecordCache::default(),
            head_cache: HeadCache::default(),
            part_cache: PartCache::default(),
            postings_cache: PostingsCache::default(),
            byte_cache,
            interlock_violations: AtomicU64::new(0),
            compaction_input_set_conflicts: AtomicU64::new(0),
            rewrite_sibling_conflicts: AtomicU64::new(0),
            isolation_breaches: AtomicU64::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            enforce_provisioning: false,
            provisioning_checked: Mutex::new(HashSet::new()),
            tenant_activity: Mutex::new(HashMap::new()),
            column_stats_cache,
        })
    }

    /// Enable durable `shard_count` enforcement on the resolve path (ADR-0050
    /// section 5). With it on, the first resolve for each (tenant, signal)
    /// validates this catalog's configured `shard_count` against the
    /// `t/<tenant_hash>/<sig>/prov` record; a disagreement is a hard
    /// [`CatalogError`], never a silent resolve over a subset of shards. Absent
    /// or fresh records pass through (a query never writes a record). Off by
    /// default so only the process that opts in (the server's `build_catalog`)
    /// pays the one GET per (tenant, signal); every existing direct-construction
    /// caller is unchanged.
    pub fn with_provisioning_enforcement(mut self) -> Self {
        self.enforce_provisioning = true;
        self
    }

    /// Attach a local-disk tier to the byte cache (ADR-0046 decision 3), turning
    /// the RAM-only [`ByteCache::Ram`] this catalog was built with into a
    /// RAM-over-disk [`ByteCache::Tiered`]. The server's `--cache-dir` wiring
    /// (#97) calls this on the constructed catalog, parallel to the
    /// [`Catalog::with_provisioning_enforcement`] builder already chained beside
    /// it in `ravel-server`'s `build_catalog`.
    ///
    /// # Ordering constraint
    ///
    /// This MUST be called immediately after [`Catalog::new`], before the
    /// catalog serves any read. When the current byte cache is
    /// [`ByteCache::Ram`], its `E` is [`std::convert::Infallible`] and cannot be
    /// composed into a [`TieredCache`] whose upstream-fetch error is
    /// `Arc<StoreError>` (the two must share one error type), so this discards
    /// the existing RAM `Cache` and builds a fresh `Cache<Arc<StoreError>>` at
    /// `ram_limits` for the RAM tier. Discarding it is safe only because nothing
    /// has been admitted yet; calling this after the catalog has served reads
    /// would silently drop a warm RAM tier.
    ///
    /// # Precedence
    ///
    /// The `byte_cache_max_bytes == 0` disabled sentinel always wins over a
    /// configured disk dir: when the byte cache is `None` (disabled) this returns
    /// `self` unchanged rather than building a cache the operator's
    /// `--disable-cache` said not to build. An already-[`ByteCache::Tiered`]
    /// cache is likewise left as is (idempotent). `ram_limits` is an explicit
    /// parameter because [`Cache`] exposes no accessor for its own configured
    /// [`CacheLimits`]; the caller passes the same limits [`Catalog::new`] built
    /// the RAM tier from.
    ///
    /// [`DiskCache::new`] spawns the ADR-0064 background age-sweep and so must be
    /// called inside a Tokio runtime.
    pub fn with_disk_byte_cache(
        mut self,
        ram_limits: CacheLimits,
        dir: PathBuf,
        disk_limits: CacheLimits,
    ) -> Self {
        match self.byte_cache {
            // Disabled sentinel wins over a disk dir: build nothing.
            None => self,
            // Already tiered: idempotent, leave it.
            Some(ByteCache::Tiered(_)) => self,
            Some(ByteCache::Ram(_)) => {
                // The existing RAM `Cache<Infallible>` cannot be composed into a
                // `TieredCache<Arc<StoreError>>` (different `E`), and it holds
                // nothing yet (called before any read), so build a fresh RAM
                // tier at the same limits and compose it over the disk tier.
                let ram = Cache::<Arc<StoreError>>::new(ram_limits);
                let disk = DiskCache::new(dir, disk_limits);
                self.byte_cache = Some(ByteCache::Tiered(TieredCache::new(ram, disk)));
                self
            }
        }
    }

    /// Validate the configured `shard_count` for one (tenant, signal) against
    /// its provisioning record, once per pair (ADR-0050 section 5). A no-op
    /// when enforcement is off or the pair was already checked. Read-only:
    /// [`crate::AbsentPolicy::CheckOnly`] never writes, so a query-only node
    /// with write-restricted credentials still resolves.
    async fn enforce_provisioning_once(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        if !self.enforce_provisioning {
            return Ok(());
        }
        if self
            .provisioning_checked
            .lock()
            .contains(&(*tenant, signal))
        {
            return Ok(());
        }
        // Route the check's record GET through `guarded_get` (issue #729): the
        // read-only `CheckOnly` policy never writes or lists, so this is the
        // same record read `validate_or_adopt(.., CheckOnly)` performs, now
        // semaphore-bounded and recorded like every other resolve GET.
        let getter = GuardedRecordGet {
            catalog: self,
            accounting,
        };
        let check = crate::provisioning::validate_check_only_accounted(
            &getter,
            tenant,
            signal,
            self.config.shard_count,
        )
        .await?;
        // Only a `Matched` result is safe to cache forever: the record is
        // immutable, so a match stays a match. `FreshNoData` means "no record
        // exists yet, nothing to validate against" -- caching it would skip the
        // real check once a later, higher-shard_count process writes the record
        // and lands data across shards this process would then silently omit
        // (records are immutable; a stale cache hit never re-checks). Re-check
        // on the next resolve until the record actually appears; that is one
        // extra GET per resolve until first ingest, which is self-limiting.
        if !matches!(check, crate::provisioning::ProvisioningCheck::FreshNoData) {
            self.provisioning_checked.lock().insert((*tenant, signal));
        }
        Ok(())
    }

    /// The (tenant, signal)'s shard-generation history for the read-side scan
    /// rule (ADR-0052 section 4), read fresh and uncached on every resolve
    /// (and fold): a stale-in-the-wrong-direction cached view could silently
    /// under-scan, so the scan set is always derived from the current record.
    ///
    /// When provisioning enforcement is off, there is no provisioning record
    /// to consult and `shard_count` is a static process config, so this
    /// returns the single implicit generation 0 at the configured count with
    /// no store read: `scan_count` over it is `config.shard_count` for every
    /// hour, exactly the pre-ADR-0052 `0..shard_count` fan-out. The many
    /// in-crate and `ravel-query`/`ravel-sql` callers that build a `Catalog`
    /// directly are therefore unchanged; only a process that opted into
    /// enforcement (the server's `build_catalog`) reads the record and gets
    /// generation-aware scanning. An absent record under enforcement is the
    /// same single implicit generation (a tenant whose first write has not
    /// landed yet).
    pub(crate) async fn read_scan_generations(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        accounting: &QueryAccounting,
    ) -> Result<Vec<crate::provisioning::ShardGeneration>, CatalogError> {
        if !self.enforce_provisioning {
            return Ok(vec![implicit_generation_zero(self.config.shard_count)]);
        }
        // Route the generation-history GET through `guarded_get` (issue #729):
        // this read runs on every resolve, so leaving it as a raw `store.get`
        // under-counted a query's GETs by one and escaped the resolve
        // semaphore. `accounting` may be a discarded handle for non-query
        // callers (the fold path); the read still funnels through the same
        // bounded entry point.
        let getter = GuardedRecordGet {
            catalog: self,
            accounting,
        };
        match crate::provisioning::read_generations_accounted(&getter, tenant, signal).await? {
            Some(generations) => Ok(generations),
            None => Ok(vec![implicit_generation_zero(self.config.shard_count)]),
        }
    }

    /// One store GET bounded by the resolve-wide in-flight semaphore. The permit is released the moment the GET resolves and is
    /// never held across another guarded request, so a resolve fanning out
    /// many records cannot wait on permits it already holds.
    ///
    /// The sole funnel for every GET a query issues (ADR-0044 decision 2): `accounting` is credited one [`AccountedOp::Get`] request
    /// unconditionally, and its bytes only on success (`got.data.len()`,
    /// mirroring `InstrumentedStore`'s convention that a failed GET moves no
    /// bytes). Call sites never account for themselves.
    pub(crate) async fn guarded_get(
        &self,
        key: &str,
        range: GetRange,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, StoreError> {
        let _permit =
            self.request_semaphore.acquire().await.map_err(|_| {
                StoreError::Transient("catalog request semaphore closed".to_string())
            })?;
        let result = self.store.get(key, range).await;
        accounting.record_s3_request(AccountedOp::Get);
        if let Ok(got) = &result {
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        }
        result
    }

    /// One store GET for an object named by a content-addressed ref
    /// (`SnapshotPartRef`/`SnapshotPostingsRef`), consulted through the byte
    /// cache before falling through to [`Catalog::guarded_get`] (ADR-0046
    /// decisions 1-2). `content_hash` and `size` are the ref's own blake3 and
    /// declared size, both known before the fetch is planned; a hit records
    /// one cache hit and skips the GET (so `guarded_get`'s own accounting,
    /// which credits an S3 request unconditionally, never runs), a miss
    /// records one cache miss and falls through, admitting the bytes on
    /// success.
    ///
    /// When `content_hash` is not exactly 32 bytes the ref is malformed; this
    /// bypasses the cache entirely and calls `guarded_get` directly, exactly
    /// as an uncached GET would, leaving the existing hash-mismatch handling
    /// at the call site (which re-derives the same malformed comparison) to
    /// catch it. This never happens for a well-formed ref.
    ///
    /// The catalog HEAD has no counterpart to this method: `SnapshotHead`
    /// carries no content hash, so `read_head` has no `content_hash` value it
    /// could pass here, and it never calls this method. The byte cache is
    /// reachable only through this one function, which is the structural
    /// reason HEAD cannot be admitted to it.
    pub(crate) async fn fetch_content_addressed(
        &self,
        tenant: &TenantHash,
        key: &str,
        content_hash: &[u8],
        size: u64,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, StoreError> {
        let Ok(content_hash) = <[u8; 32]>::try_from(content_hash) else {
            return Ok(self
                .guarded_get(key, GetRange::Full, accounting)
                .await?
                .data);
        };
        // No byte cache (disabled via `byte_cache_max_bytes == 0`):
        // read straight through with no cache hit/miss accounting, exactly as
        // an uncached GET would.
        let Some(byte_cache) = &self.byte_cache else {
            return Ok(self
                .guarded_get(key, GetRange::Full, accounting)
                .await?
                .data);
        };
        let cache_key = CacheKey::new(tenant.0, content_hash, 0, size);
        match byte_cache {
            // RAM tier only: the plain get-then-insert path, unchanged. Its own
            // hit/miss accounting brackets the store GET exactly as before.
            ByteCache::Ram(ram) => {
                if let Some(bytes) = ram.get(&cache_key) {
                    accounting.record_cache_hit();
                    accounting.add_cache_bytes(bytes.len() as u64);
                    return Ok(bytes);
                }
                accounting.record_cache_miss();
                let got = self.guarded_get(key, GetRange::Full, accounting).await?;
                ram.insert(cache_key, got.data.clone());
                Ok(got.data)
            }
            // RAM over disk: read through `get_or_fetch`. The upstream fetch is
            // the same `guarded_get` the RAM path runs on a miss, moved into the
            // single-flight closure so only one leader fetches a given key even
            // when both tiers miss concurrently (ADR-0046 decision 5). The
            // returned [`Source`] says whether a tier served the bytes (a hit,
            // possibly disk-served) or the closure fetched them (a miss), so the
            // same hit/miss accounting the RAM path records is preserved.
            //
            // The closure threads `Arc<StoreError>` (the single-flight requires
            // a `Clone` error); the owned `StoreError` callers match on is
            // reconstructed on the way out. `SingleFlightError::LeaderLost` --
            // the leader's future was dropped or panicked before producing a
            // result -- surfaces as a retryable transient, never a wrong result.
            ByteCache::Tiered(tiered) => {
                let (bytes, source) = tiered
                    .get_or_fetch(cache_key, move || async move {
                        let got = self
                            .guarded_get(key, GetRange::Full, accounting)
                            .await
                            .map_err(Arc::new)?;
                        Ok(got.data)
                    })
                    .await
                    .map_err(|err| match err {
                        SingleFlightError::Upstream(store_err) => clone_store_error(&store_err),
                        SingleFlightError::LeaderLost => StoreError::Transient(
                            "byte cache single-flight leader lost".to_string(),
                        ),
                    })?;
                match source {
                    Source::Cache => {
                        accounting.record_cache_hit();
                        accounting.add_cache_bytes(bytes.len() as u64);
                    }
                    Source::Upstream => accounting.record_cache_miss(),
                }
                Ok(bytes)
            }
        }
    }

    /// Prefix listing bounded by the same in-flight semaphore, draining every
    /// page (the [`ravel_object_store::list_all`] contract) with a permit
    /// acquired per page rather than one held across the whole listing.
    ///
    /// The sole funnel for every LIST a query issues (ADR-0044 decision 2):
    /// `accounting` is credited one [`AccountedOp::List`] request per page,
    /// unconditionally, mirroring `InstrumentedStore`'s convention that a
    /// LIST never moves bytes.
    ///
    /// Also the sole funnel for the ADR-0050 §2 LIST-prefix assertion: every
    /// returned key must begin with `tenant`'s prefix, or this is a hard
    /// isolation-breach `CatalogError::FieldMismatch`, never a silently
    /// dropped or served foreign key.
    async fn guarded_list_all(
        &self,
        tenant: &TenantHash,
        prefix: &str,
        accounting: &QueryAccounting,
    ) -> Result<Vec<ObjectMeta>, CatalogError> {
        let tenant_prefix = format!("t/{}/", tenant.to_hex());
        let mut out: Vec<ObjectMeta> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut page_token = None;
        loop {
            let page = {
                let _permit = self.request_semaphore.acquire().await.map_err(|_| {
                    StoreError::Transient("catalog request semaphore closed".to_string())
                })?;
                let page = self.store.list(prefix, page_token).await;
                accounting.record_s3_request(AccountedOp::List);
                page?
            };
            for meta in page.objects {
                if !meta.key.starts_with(&tenant_prefix) {
                    self.record_isolation_breach();
                    return Err(CatalogError::FieldMismatch {
                        key: prefix.to_string(),
                        field: "list_prefix",
                        expected: tenant_prefix,
                        actual: meta.key,
                    });
                }
                if seen.insert(meta.key.clone()) {
                    out.push(meta);
                }
            }
            match page.next {
                Some(next) => page_token = Some(next),
                None => break,
            }
        }
        Ok(out)
    }

    pub fn config(&self) -> &CatalogConfig {
        &self.config
    }

    /// Number of writer-interlock violations observed across this catalog's
    /// lifetime (an L0 commit record not named in any compaction record's
    /// input list, whose `created_unix_ns` postdates that record). See
    /// docs/catalog-and-mvcc.md step 3.
    pub fn interlock_violations(&self) -> u64 {
        self.interlock_violations.load(Ordering::Relaxed)
    }

    /// Number of buckets observed with two compaction records carrying
    /// different `input_set_hash` (docs/catalog-and-mvcc.md step 3, §3.6
    /// row 11).
    pub fn compaction_input_set_conflicts(&self) -> u64 {
        self.compaction_input_set_conflicts.load(Ordering::Relaxed)
    }

    /// Number of buckets observed with two or more live (non-superseded)
    /// rewrite records, neither superseding the other (ADR-0064 decision 3
    /// point 5). Unlike [`Self::compaction_input_set_conflicts`], this is not
    /// a harmless-overlap anomaly: it means each sibling's own erased subject
    /// can still be served through the other's un-rewritten copy.
    pub fn rewrite_sibling_conflicts(&self) -> u64 {
        self.rewrite_sibling_conflicts.load(Ordering::Relaxed)
    }

    /// Raise [`Self::rewrite_sibling_conflicts`] when one bucket holds two or
    /// more live (non-superseded) rewrite records, neither superseding the
    /// other. Unlike two compaction records, this is NOT harmless overlap: a
    /// rewrite's output deliberately lacks the records its own erasure
    /// dropped, so a sibling rewrite's un-rewritten copy of those same records
    /// silently defeats it (ADR-0064 decision 3 point 5). Normal operation
    /// never produces this (one rewrite batches every pending request for a
    /// bucket); alarm loudly rather than serve it as ordinary overlap.
    ///
    /// Called from every site that resolves rewrite supersession: snapshot
    /// resolution's `process_bucket`, the index fold's `classify_bucket`, and
    /// the read-your-write `resolve_min_token_fallback`. Wiring it into only
    /// `process_bucket` would leave it silent for the ordinary case: ADR-0064
    /// §3.1 scopes the rewrite pass to already-sealed buckets, and a folded
    /// snapshot serves those hours without ever calling `process_bucket`.
    pub(crate) fn check_rewrite_siblings(
        &self,
        shard: u32,
        hour: u32,
        rewrite_records: &[(String, Arc<RewriteRecord>)],
        superseded_records: &HashSet<String>,
    ) {
        let live = rewrite_records
            .iter()
            .filter(|(rkey, _)| !superseded_records.contains(rkey))
            .count();
        if live > 1 {
            self.rewrite_sibling_conflicts
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                shard,
                hour,
                records = live,
                "rewrite interlock breach: sealed bucket holds multiple live, non-superseding \
                 rewrite records -- each sibling's erasure may be defeated by the other's \
                 un-rewritten copy"
            );
        }
    }

    /// Count of hard isolation-breach failures observed across this
    /// catalog's lifetime (ADR-0050 §2): a HEAD/postings tenant_hash
    /// mismatch or an out-of-prefix listing result. See
    /// docs/catalog-and-mvcc.md.
    pub fn isolation_breaches(&self) -> u64 {
        self.isolation_breaches.load(Ordering::Relaxed)
    }

    /// `pub(crate)`: lets `snapshot_resolve` (ADR-0050 §2) bump the
    /// isolation-breach counter from its own `impl Catalog` block before
    /// returning the hard `CatalogError::FieldMismatch`.
    pub(crate) fn record_isolation_breach(&self) {
        self.isolation_breaches.fetch_add(1, Ordering::Relaxed);
    }

    /// `pub(crate)`: lets `fold` issue
    /// its own LIST/GET/PUT calls through the same store handle, in its own
    /// `impl Catalog` block in `fold.rs`, without duplicating the
    /// `store`/`config`/`cache` fields in a separate type.
    pub(crate) fn store(&self) -> &dyn ObjectStoreBackend {
        self.store.as_ref()
    }

    /// `pub(crate)`: lets `snapshot_resolve`
    /// share the decoded-HEAD cache from its own `impl Catalog` block.
    pub(crate) fn head_cache(&self) -> &HeadCache {
        &self.head_cache
    }

    /// `pub(crate)`: lets `snapshot_resolve` share the decoded-part cache.
    pub(crate) fn part_cache(&self) -> &PartCache {
        &self.part_cache
    }

    /// Resolve exact per-segment column statistics for `(tenant, signal)`
    /// from the current folded snapshot HEAD (ADR-0850), for a query engine
    /// to join against its own resolved snapshot by identity. `Ok(None)`
    /// means no usable column-stats object exists right now (nothing folded
    /// yet, no configured typed columns, or the last fold's column-stats
    /// build/PUT failed): the caller must fall back to scanning, never treat
    /// this as "zero columns configured means zero rows". See
    /// [`column_stats_resolve::load_column_stats`] for the full
    /// degrade-to-`Ok(None)` contract.
    pub async fn load_column_stats(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        accounting: &QueryAccounting,
    ) -> Result<Option<Arc<LoadedColumnStats>>, LoadColumnStatsError> {
        // Route every GET through the same semaphore-bounded, accounted funnel
        // (`guarded_get`) every other query read uses, so this path's requests
        // and bytes are credited to `accounting` under `AccountedOp::Get` and
        // bounded by the request semaphore (issue #850).
        let getter = GuardedRecordGet {
            catalog: self,
            accounting,
        };
        // Always read HEAD (one GET): the stats object is bound to the CURRENT
        // folded HEAD, not the pinned snapshot, so this is the only way to
        // detect a fold that superseded a cached object (ADR-0850, issue #888).
        let Some(resolved) =
            column_stats_resolve::resolve_stats_ref(&getter, tenant, signal).await?
        else {
            return Ok(None);
        };
        // Reuse the cached object only when its content hash AND the HEAD's
        // covered part set both still match `resolved`: a changed fold produces
        // a different `blake3` (a rebuilt object) or a different part set (the
        // binding a stale object fails), and either misses. This skips the
        // second GET, the stats-object fetch, and nothing else. An entry the
        // byte budget evicted also misses here and re-fetches, never returning a
        // partial or stale statistic.
        let cache_hit = self.column_stats_cache.as_ref().and_then(|cache| {
            cache.get(
                (*tenant, signal),
                &resolved.blake3,
                &resolved.expected_part_blake3,
            )
        });
        if let Some(stats) = cache_hit {
            return Ok(Some(stats));
        }
        let Some(loaded) =
            column_stats_resolve::fetch_stats_object(&getter, tenant, &resolved).await?
        else {
            return Ok(None);
        };
        let stats = Arc::new(loaded);
        if let Some(cache) = self.column_stats_cache.as_ref() {
            cache.insert((*tenant, signal), resolved.blake3, Arc::clone(&stats));
        }
        Ok(Some(stats))
    }

    /// Evict every per-tenant cache outer-map entry for tenants last touched
    /// before `now_ns - ttl_ns` (ADR-0069 decision 2, idle-tenant state
    /// eviction). Returns the number of tenants evicted.
    ///
    /// A tenant's last touch is stamped in [`Catalog::resolve_impl`] on the
    /// injected `now_ns`; both `now_ns` and `ttl_ns` here are caller-supplied
    /// (the server's idle-tenant sweep loop), so this reads no clock and is
    /// deterministic under test. For each idle tenant every per-tenant cache
    /// map is swept — the commit-record, compaction-record, decoded-HEAD,
    /// decoded-part, and decoded-postings caches — dropping that tenant's whole
    /// outer-map entry. Every one of those caches holds only immutable,
    /// content-addressed or TTL-revalidated state, so an evicted entry is
    /// re-derived on the tenant's next resolve by a re-read, never a wrong
    /// result (the caches are "already reconstructible by definition",
    /// ADR-0069). The byte cache is a process-wide LRU keyed by content hash,
    /// not partitioned per tenant, so it is not swept here; its own capacity
    /// bound reclaims it.
    ///
    /// Admission-controller state is not held by the catalog and is explicitly
    /// out of scope for eviction (ADR-0069 decision 2); nothing here touches it.
    pub fn evict_idle_tenants(&self, now_ns: i64, ttl_ns: i64) -> usize {
        // Collect the idle tenants under the activity lock, then drop the lock
        // before touching the cache locks: never hold two cache-related locks
        // at once, so this can never deadlock against a concurrent resolve.
        let idle: Vec<TenantHash> = {
            let mut activity = self.tenant_activity.lock();
            let idle: Vec<TenantHash> = activity
                .iter()
                .filter(|&(_, &last)| now_ns.saturating_sub(last) > ttl_ns)
                .map(|(tenant, _)| *tenant)
                .collect();
            for tenant in &idle {
                activity.remove(tenant);
            }
            idle
        };
        for tenant in &idle {
            self.cache.evict_tenant(tenant);
            self.compaction_cache.evict_tenant(tenant);
            self.head_cache.evict_tenant(tenant);
            self.part_cache.evict_tenant(tenant);
            self.postings_cache.evict_tenant(tenant);
        }
        if let Some(cache) = self
            .column_stats_cache
            .as_ref()
            .filter(|_| !idle.is_empty())
        {
            cache.evict_tenants(&idle);
        }
        idle.len()
    }

    /// `pub(crate)`: lets `snapshot_resolve` share the decoded-postings
    /// cache.
    pub(crate) fn postings_cache(&self) -> &PostingsCache {
        &self.postings_cache
    }

    /// The byte cache's counters handle (ADR-0046), or `None` when the byte
    /// cache is disabled ([`CatalogConfig::byte_cache_max_bytes`] `== 0`). The server threads this to `/metrics` so the catalog byte cache's
    /// hits/misses/bytes render alongside the fetcher cache's, and a
    /// `--disable-cache` process renders no catalog cache family at all, the
    /// same absence a disabled fetcher cache produces.
    pub fn byte_cache_metrics(&self) -> Option<Arc<ravel_cache::CacheMetrics>> {
        self.byte_cache.as_ref().map(|cache| match cache {
            ByteCache::Ram(ram) => ram.metrics(),
            // The RAM tier's counters, the ones ADR-0046's hit/miss/byte SLIs
            // read; the disk tier keeps its own separate counters.
            ByteCache::Tiered(tiered) => tiered.ram_metrics(),
        })
    }

    /// The byte cache's disk-tier counters handle (ADR-0046), or `None` when the
    /// byte cache is disabled or is RAM-only (no `--cache-dir`). The
    /// counterpart to [`Catalog::byte_cache_metrics`] above: the server threads
    /// this to `/metrics` so the catalog byte cache's disk tier renders under
    /// `cache="catalog",tier="disk"`. `None` for [`ByteCache::Ram`] (there is no
    /// disk tier) and `Some(`[`TieredCache::disk_metrics`]`)` for
    /// [`ByteCache::Tiered`].
    pub fn byte_cache_disk_metrics(&self) -> Option<Arc<ravel_cache::CacheMetrics>> {
        match self.byte_cache.as_ref()? {
            ByteCache::Ram(_) => None,
            ByteCache::Tiered(tiered) => Some(tiered.disk_metrics()),
        }
    }

    /// Cumulative column-statistics cache evictions (issue #905): entries the
    /// byte budget ([`CatalogConfig::column_stats_cache_max_bytes`]) dropped to
    /// stay within its limit. The server threads this to `/metrics`. A climbing
    /// value means the cache is UNDERSIZED for the working set of tenants a
    /// process serves, which an operator reads apart from ABSENT statistics (a
    /// load returning `Ok(None)`, which touches no counter): both make a query
    /// scan, but the first is fixed by raising the budget and the second is not.
    /// `0` when the cache is disabled (no cache exists to evict from).
    pub fn column_stats_cache_evictions(&self) -> u64 {
        self.column_stats_cache
            .as_ref()
            .map_or(0, ColumnStatsCache::evictions)
    }

    /// Cumulative column-statistics cache refusals (issue #905): loads whose
    /// object alone exceeds the whole byte budget, so no eviction could make
    /// room and the object was served but not cached. Distinct from an
    /// eviction: it means the budget is below a SINGLE object's size, not merely
    /// below the working set. `0` when the cache is disabled.
    pub fn column_stats_cache_refusals(&self) -> u64 {
        self.column_stats_cache
            .as_ref()
            .map_or(0, ColumnStatsCache::refusals)
    }

    /// Bytes currently held by the column-statistics cache (issue #905), the
    /// sum of every cached object's [`LoadedColumnStats::heap_bytes`]. Bounded
    /// by [`CatalogConfig::column_stats_cache_max_bytes`]. `0` when the cache is
    /// disabled.
    pub fn column_stats_cache_held_bytes(&self) -> u64 {
        self.column_stats_cache
            .as_ref()
            .map_or(0, ColumnStatsCache::held_bytes)
    }

    /// `pub(crate)`: exposed for `cache`'s own tests to inspect and seed the
    /// RAM byte cache directly (ADR-0046). Not used by `snapshot_resolve`, which
    /// only ever reaches the byte cache through
    /// [`Catalog::fetch_content_addressed`] -- hence `cfg(test)`, since no
    /// production code path needs this accessor. Panics if the byte cache is
    /// disabled; every caller builds a config with a non-zero byte-cache
    /// budget, so a `None` here is a test-setup bug, not a runtime state.
    ///
    /// RAM-only: it returns the [`ByteCache::Ram`] tier's `Cache`, the only
    /// variant [`Catalog::new`] builds. A test that attaches a disk tier holds
    /// the [`TieredCache`] it injected and asserts on that directly, so this
    /// panics rather than pretend a disk-backed cache is a bare `Cache`.
    #[cfg(test)]
    #[allow(clippy::expect_used)]
    pub(crate) fn byte_cache(&self) -> &Cache<std::convert::Infallible> {
        match self
            .byte_cache
            .as_ref()
            .expect("byte_cache() called on a catalog built with the byte cache disabled")
        {
            ByteCache::Ram(ram) => ram,
            ByteCache::Tiered(_) => {
                panic!("byte_cache() is RAM-only; a tiered byte cache is asserted on directly")
            }
        }
    }

    /// `#[cfg(test)]`: replace the byte cache with a disk-backed
    /// [`TieredCache`], for the acceptance test that proves a corrupt
    /// disk-served hit falls back through this crate's read funnel exactly as a
    /// corrupt store read does (ADR-0046 decision 4). Production attaches a disk
    /// tier through its own wiring (#97); this is the in-test equivalent, which
    /// is why the disk tier need not be reachable from the server yet for the
    /// funnel to already handle it correctly.
    #[cfg(test)]
    pub(crate) fn set_tiered_byte_cache_for_test(&mut self, tiered: TieredCache<Arc<StoreError>>) {
        self.byte_cache = Some(ByteCache::Tiered(tiered));
    }

    /// Resolve a query-time snapshot (docs/catalog-and-mvcc.md "Snapshot
    /// resolution"):
    ///
    /// 1. List every commit prefix for (shard, ingest-hour bucket)
    ///    overlapping `[range.start_ns - max_ingest_lag, now_ns +
    ///    clock_skew_allowance]`, decode and cache the records, and filter
    ///    by event-time overlap with `range`.
    /// 2. For each `min_token`, reconstruct its exact commit key and GET it
    ///    directly (never by re-listing), including its segment even if its
    ///    event range does not overlap `range` (read-your-write). Missing
    ///    after one retry is [`CatalogError::UnsatisfiableToken`].
    ///
    /// `now_ns` is always caller-supplied: this crate never reads a clock.
    ///
    /// Wide windows are traversed by a single per-shard recursive prefix LIST
    /// rather than one LIST per (shard, ingest-hour) bucket (ADR-0056): the
    /// per-bucket loop's cost is `shard_count * hours` and grows with window
    /// width, while the prefix scan is `O(objects / page_size)` and does not.
    /// The prefix path carries a runtime request cap: a scan that would issue
    /// more than
    /// [`CatalogConfig::max_catalog_list_requests`](crate::CatalogConfig::max_catalog_list_requests)
    /// LISTs is refused with [`CatalogError::WindowTooWide`] (the request bound,
    /// enforced at runtime; ADR-0044 decision 3 as amended for
    /// ADR-0056). A wide-but-sparse window is served cheaply; only a scan whose
    /// actual object volume is unsustainable is refused. Callers should still
    /// keep `config.shard_count` bounded.
    pub async fn resolve(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_with_accounting(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            &QueryAccounting::new(),
        )
        .await
    }

    /// Like [`Catalog::resolve`], but records every S3 request and cache
    /// access this resolve makes into `accounting` (ADR-0044).
    /// A separate method rather than an added `resolve` parameter: `resolve`
    /// is called from `ravel-query`, `ravel-sql`, and `services/ravel-server`
    /// (out of this task's scope), so its signature and every existing call
    /// site stay exactly as they were; only callers that want accounting
    /// need to switch to this entry point.
    pub async fn resolve_with_accounting(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(tenant, signal, range, min_tokens, now_ns, None, accounting)
            .await
            .map(|(snapshot, _origins, _generations)| snapshot)
    }

    /// Like [`Catalog::resolve`], but applies postings-based segment pruning
    /// when `name_filter` is `Some`: an
    /// equality `__name__` matcher value from the caller's query.
    ///
    /// Pruning only ever removes snapshot-sourced segments this snapshot's
    /// postings provably do not carry that name; listing- and
    /// `min_token`-sourced segments are never touched (they never pass
    /// through the postings-aware code path at all), and missing or corrupt
    /// postings silently degrade to the same behavior as [`Catalog::resolve`]
    /// (exact semantics by default: approximate
    /// or unavailable pruning data must never fail a query or drop a
    /// segment that a matcher could actually match).
    ///
    /// A separate method rather than a `resolve` parameter: this keeps
    /// `Catalog::resolve`'s existing signature and every call site
    /// (in-crate and external) unchanged, per this ticket's "keep the
    /// `Catalog` public API stable if possible" requirement.
    pub async fn resolve_pruned(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_pruned_with_accounting(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            &QueryAccounting::new(),
        )
        .await
    }

    /// Like [`Catalog::resolve_pruned`], but records every S3 request and
    /// cache access this resolve makes into `accounting` (ADR-0044). See [`Catalog::resolve_with_accounting`] for why this is a
    /// separate method rather than a `resolve_pruned` parameter.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_pruned_with_accounting(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            accounting,
        )
        .await
        .map(|(snapshot, _origins, _generations)| snapshot)
    }

    /// Like [`Catalog::resolve_pruned_with_accounting`], but also returns the
    /// per-segment origin breakdown (ADR-0073 decision 1): sealed-below-
    /// watermark, recent (listed live above the watermark), or
    /// token-resolved. A separate method rather than an added
    /// `resolve_pruned_with_accounting` return value, for the same reason
    /// `resolve_pruned`/`resolve_with_accounting` exist as separate methods
    /// above: that method's signature and every existing call site
    /// (`ravel-sql`'s executor calls it directly) stay exactly as they were.
    /// The recent-hours admission seam (`ravel-query`'s `SegmentAdmission`)
    /// is the only consumer of the returned [`SegmentOrigins`] today.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_pruned_with_admission(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, SegmentOrigins), CatalogError> {
        self.resolve_impl(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            accounting,
        )
        .await
        .map(|(snapshot, origins, _generations)| (snapshot, origins))
    }

    /// Like [`Catalog::resolve_pruned_with_admission`], but also returns the
    /// shard-generation history **this resolve itself** computed its scan set
    /// from (ADR-0052 section 4), for the ADR-0103 pushdown eligibility gate.
    ///
    /// The gate (`ravel_query::distrib::pushdown::is_pushdown_eligible`) asks
    /// whether every resolved segment's ingest hour sits inside one
    /// generation's stable interval. ADR-0103 decision 1(b) requires it to read
    /// the same history object the resolve used, never a second store read or a
    /// separately-cached copy: a generation appended between the two reads would
    /// otherwise be invisible to the gate while its segments are already
    /// visible in the resolved set, and the gate would call a
    /// generation-straddling query eligible. Returning the history the resolve
    /// already read makes that skew structurally impossible, and costs no
    /// additional request (`read_scan_generations` runs once per resolve
    /// regardless).
    ///
    /// When head validation performed its one-shot record re-read, the returned
    /// history is the *revalidated* one -- the same one the listing suffix
    /// scanned under, and never staler than the one that validated the head.
    ///
    /// A separate method rather than a changed
    /// `resolve_pruned_with_admission` signature, for the same reason each
    /// `resolve*` variant above is separate: every existing call site
    /// (`ravel-sql`'s executor, `ravel-query`'s admission seam) stays exactly
    /// as it was.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_pruned_with_generations(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, SegmentOrigins, Vec<ShardGeneration>), CatalogError> {
        self.resolve_impl(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            accounting,
        )
        .await
    }

    /// Instruments the whole LIST/GET fan-out with the `catalog_resolve` span
    /// (ADR-0044 decision 5). The span lives here rather than on
    /// ravel-query's `resolve_bounded` wrapper so that every caller of
    /// `Catalog::resolve*` gets it, including ravel-sql's executor which calls
    /// `resolve_pruned_with_accounting` directly. Per-call `s3_requests`,
    /// `s3_bytes`, and `segments_pruned` are recorded from this resolve's own
    /// `accounting` delta once the fan-out returns, mirroring the
    /// record-after-call pattern in `services/ravel-server`'s request spans.
    /// Only ADR-0044 section 4 allowlist fields (`tenant_hash`) plus per-span
    /// count fields are recorded; never a query, object key, or shard number.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_impl(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, SegmentOrigins, Vec<ShardGeneration>), CatalogError> {
        // Idle-tenant eviction last-touch (ADR-0069 decision 2):
        // stamp this tenant's activity with the caller's injected `now_ns`
        // before the fan-out. Every resolve entry point funnels through here,
        // and the fan-out below is what populates the per-tenant caches the
        // sweep evicts, so stamping first keeps a tenant's last-touch coherent
        // with the cache entries it is about to fill.
        self.tenant_activity.lock().insert(*tenant, now_ns);
        let span = tracing::debug_span!(
            "catalog_resolve",
            tenant_hash = %tenant.to_hex(),
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
            segments_pruned = tracing::field::Empty,
        );
        let before = accounting.snapshot();
        let result = self
            .resolve_fanout(
                tenant,
                signal,
                range,
                min_tokens,
                now_ns,
                name_filter,
                accounting,
            )
            .instrument(span.clone())
            .await;
        // The counts this resolve alone added: its LIST/GET fan-out increments
        // `accounting`, so the after-minus-before delta is this call's own S3
        // cost, not the whole query's (a query fetches segments afterwards on
        // the same handle). `segments_pruned` is read straight off the returned
        // snapshot.
        let after = accounting.snapshot();
        span.record(
            "s3_requests",
            after
                .total_s3_requests()
                .saturating_sub(before.total_s3_requests()),
        );
        span.record(
            "s3_bytes",
            after
                .total_s3_bytes()
                .saturating_sub(before.total_s3_bytes()),
        );
        if let Ok((snapshot, _origins, _generations)) = &result {
            span.record("segments_pruned", snapshot.segments_pruned);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    // issue #730: cut the resolve's LIST round trips above the snapshot
    // watermark from three to one (overlap the erasure LIST, one bounded
    // LIST per shard instead of one per (shard, hour)).
    async fn resolve_fanout(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, SegmentOrigins, Vec<ShardGeneration>), CatalogError> {
        // Durable shard_count enforcement on the read path (ADR-0050 section 5): fail before the `0..shard_count` listing loop below if this
        // catalog's configured shard_count disagrees with the (tenant, signal)'s
        // provisioning record, so a lower value never silently drops the shards
        // it omits. A no-op unless enforcement was opted into.
        self.enforce_provisioning_once(tenant, signal, accounting)
            .await?;

        // The generation history for the per-hour scan rule (ADR-0052 section
        // 4), read fresh (see `read_scan_generations`). A corrupt or misfiled
        // record fails the resolve here as a hard `CatalogError`, exactly as
        // `enforce_provisioning_once` above fails on a scalar mismatch: a
        // malformed generation history must never be read as "assume
        // generation 0" and silently under-scan.
        //
        // The original pre-execution refusal (a window whose
        // per-(shard, hour) estimate exceeded `max_catalog_list_requests` was
        // refused here, before any store read) is superseded by the
        // prefix-path routing below (ADR-0056, "INTERACTION 1"): an
        // hour-counting estimate would refuse a wide-but-sparse window whose
        // real cost the prefix scan proves is tiny, the wrong direction to
        // fail closed in. Admission now happens where cost is actually
        // knowable -- routed to the prefix path when the per-bucket estimate
        // would be expensive, and capped at runtime by real LIST count if the
        // prefix scan itself turns out to be too large.
        //
        // Mutable, and returned to the caller at the end of this function: the
        // head-revalidation path below can replace it with a fresher read, and
        // ADR-0103's pushdown eligibility gate must see the very history this
        // resolve computed its scan set from, not a later independent read (a
        // generation appended between the two reads would be invisible to the
        // gate while already visible in the resolved segments).
        let mut generations = self
            .read_scan_generations(tenant, signal, accounting)
            .await?;

        let mut segments: HashMap<String, SegmentRef> = HashMap::new();
        let mut segments_pruned = 0u64;
        // Per-segment origin (ADR-0073 decision 1), keyed the same as
        // `segments` (`data_object_key`). Tagged at each insertion point
        // below, gated the same way `segments.entry(..).or_insert(..)` is:
        // only a genuinely new key gets an origin, so a key already present
        // from an earlier, higher-precedence source (sealed over recent,
        // either over token-resolved) keeps its original tag.
        let mut origin_by_key: HashMap<String, SegmentOrigin> = HashMap::new();

        // The pending-erasure LIST (ADR-0064 decision 2) is started before the
        // listing fan-out and joined with it, so its round trip overlaps the
        // bucket LISTs instead of following them. It still completes before the
        // Snapshot is built below (the join awaits it), so the visibility bound
        // is unchanged: a `.dreq` durable before this resolve began -- hence
        // before this LIST is even issued -- is always observed. `None` here
        // means no window was listed (no fan-out to overlap), so the LIST runs
        // standalone after the token loop, exactly as before.
        let mut pending_erasure: Option<Vec<ErasureRequest>> = None;

        if let Some((window_start_hour, window_end_hour)) = self.window_hour_bounds(range, now_ns) {
            // A snapshot at watermark W serves every window hour <= W
            // straight from its parts; only the suffix above W is listed.
            // No usable snapshot,
            // or a watermark below the window's start, falls back to
            // listing the whole window, unchanged from Phase 1.
            //
            // `name_filter.is_some()` gates the postings GET:
            // the postings object is only ever consulted for an equality
            // `__name__` filter, so a query without one never fetches or
            // decodes it.
            let (window, revalidated_generations) = self
                .resolve_snapshot_window(
                    tenant,
                    signal,
                    now_ns,
                    name_filter.is_some(),
                    // Range-scoped part fetch (ADR-0063): only parts whose hour
                    // range intersects this query window are fetched.
                    window_start_hour,
                    window_end_hour,
                    &generations,
                    accounting,
                )
                .await?;
            // When head validation performed a one-shot record
            // re-read, its fresher generation history (never staler than the
            // one that validated the head) must drive the Phase 1 scan-set
            // computation below too. Otherwise the listing suffix would scan
            // the stale, narrower range the re-read exists to correct, silently
            // omitting data that landed in a wider generation's shards.
            if let Some(revalidated) = revalidated_generations {
                generations = revalidated;
            }
            let listing_start_hour = match &window {
                Some(window) if window.watermark_hour >= window_start_hour => {
                    let snapshot_end_hour = window_end_hour.min(window.watermark_hour);
                    window.extract_into(
                        tenant,
                        signal,
                        window_start_hour,
                        snapshot_end_hour,
                        &range,
                        name_filter,
                        &mut segments_pruned,
                        &mut segments,
                    )?;
                    // `segments` is empty before this call (the first
                    // population in this branch), so every key it holds
                    // afterwards came from `extract_into`: sealed-below-
                    // watermark, postings-pruned (ADR-0073 decision 1).
                    for key in segments.keys() {
                        origin_by_key.insert(key.clone(), SegmentOrigin::SealedBelowWatermark);
                    }
                    window.watermark_hour.saturating_add(1)
                }
                _ => window_start_hour,
            };
            // Traversal choice (ADR-0056), on the listing suffix actually to be
            // scanned (`[listing_start_hour, window_end_hour]`, after any folded
            // snapshot watermark shortened the low end). The per-bucket loop
            // issues one LIST per (shard, hour) bucket -- one per empty bucket
            // included -- so its cost is `shard_count * listing_hours`; the
            // prefix scan issues `O(objects / page_size)` LISTs regardless of
            // width. Switch to the prefix scan once the suffix is wide enough
            // (config crossover), or once the per-bucket loop would exceed the
            // request ceiling. The latter replaces the pre-execution
            // refusal: instead of refusing an over-wide window, route it to the
            // non-amplifying prefix path (runtime-capped inside
            // `list_window_by_prefix`), so a wide-but-sparse window is served
            // rather than refused (ADR-0056 INTERACTION 1). The per-bucket
            // loop is chosen only when the suffix is within the ceiling, so it
            // can never exceed it.
            //
            // The per-bucket estimate must use the same generation-aware shard
            // bound the prefix path will actually scan (ADR-0052 section 4), not the static `self.config.shard_count`: on a
            // shard-count DECREASE the retiring larger generation's higher
            // shard indices stay in scope for `DEFAULT_SCAN_SLACK_HOURS` past
            // the successor's activation, and both this crossover/ceiling
            // decision and `list_window_by_prefix`'s shard loop must agree on
            // that wider bound so the decision stays consistent with what is
            // scanned.
            let suffix_scan_shards = crate::provisioning::max_scan_count_over_range(
                &generations,
                listing_start_hour,
                window_end_hour,
                crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
            );
            let listing_suffix_buckets = u64::from(suffix_scan_shards).saturating_mul(
                u64::from(window_end_hour.saturating_sub(listing_start_hour)).saturating_add(1),
            );
            let use_prefix = listing_start_hour <= window_end_hour
                && (listing_suffix_buckets >= self.config.prefix_list_crossover_requests
                    || listing_suffix_buckets > self.config.max_catalog_list_requests);

            // Overlap the pending-erasure LIST with the listing fan-out
            // (deliverable 1): start it here and `join` it with whichever
            // traversal path runs, so the two round trips share one wave
            // instead of the erasure LIST following the buckets. The joined
            // future is awaited before the Snapshot is built, so ADR-0064's
            // visibility bound is unchanged.
            let erasure_fut = self.list_pending_erasure(tenant, signal, accounting);
            if listing_start_hour > window_end_hour {
                // Empty listing suffix: a folded snapshot's watermark covers
                // the whole window, so there is nothing above it to list. The
                // former per-(shard, hour) loop's `for hour in start..=end`
                // range was empty here and issued no LIST; issue none either,
                // so a fully-folded resolve stays entirely GET-based. Only the
                // erasure LIST runs.
                pending_erasure = Some(erasure_fut.await?);
            } else {
                let listed = if use_prefix {
                    let (listed, erasure) = tokio::join!(
                        self.list_window_by_prefix(
                            tenant,
                            signal,
                            listing_start_hour,
                            window_end_hour,
                            range,
                            &generations,
                            accounting,
                        ),
                        erasure_fut,
                    );
                    pending_erasure = Some(erasure?);
                    listed?
                } else {
                    let (listed, erasure) = tokio::join!(
                        self.list_window_bounded(
                            tenant,
                            signal,
                            listing_start_hour,
                            window_end_hour,
                            range,
                            &generations,
                            accounting,
                        ),
                        erasure_fut,
                    );
                    pending_erasure = Some(erasure?);
                    listed?
                };
                for (key, segment_ref) in listed {
                    if !segments.contains_key(&key) {
                        origin_by_key.insert(key.clone(), SegmentOrigin::Recent);
                    }
                    segments.entry(key).or_insert(segment_ref);
                }
            }
        }

        for token in min_tokens {
            self.resolve_min_token(
                tenant,
                signal,
                token,
                &mut segments,
                &mut origin_by_key,
                accounting,
            )
            .await?;
        }

        // One LIST of `t/<th>/<sig>/del/` per resolve (ADR-0064 decision 2):
        // attach every pending erasure predicate to the snapshot. Runs
        // unconditionally, independent of the segment fan-out and of whether
        // any rewrite pass has physically run yet, so the visibility bound
        // holds -- a `.dreq` durable before this resolve is always seen by it.
        // Empty (the common case) costs exactly one LIST and nothing more. When
        // a window was listed above, this LIST already ran concurrently with
        // the bucket fan-out and is reused here; otherwise it runs now.
        let pending_erasure = match pending_erasure {
            Some(pending) => pending,
            None => {
                self.list_pending_erasure(tenant, signal, accounting)
                    .await?
            }
        };

        let mut segments: Vec<SegmentRef> = segments.into_values().collect();
        // Deterministic total order: the cross-segment dedup provenance order
        // named in docs/catalog-and-mvcc.md (created_unix_ns, writer_epoch,
        // writer_seq), with shard then writer_id as final tiebreaks. writer_id
        // makes the key total over distinct segments: two same-shard segments
        // from different writers can tie on (created_unix_ns, writer_epoch,
        // writer_seq) (seq is monotonic only per (writer_id, epoch, shard),
        // ADR-0010 §3), and without an identity tiebreak the stable sort would
        // otherwise leave them in randomized HashMap iteration order.
        //
        // Mixed L0/L1 levels stay a deterministic total order
        // (docs/catalog-and-mvcc.md "Snapshot resolution"): an L1 part has
        // writer_epoch/seq == 0 and writer_id == nil, so it slots into the
        // same chain by its record's created_unix_ns and gets its
        // input_set_hash then part_index as the final tiebreaks (a level tag
        // separates the two tiebreak families). L0 ordering is unchanged: the
        // appended L1-only key components are constant across L0 refs.
        segments.sort_by_key(segment_sort_key);
        // Every key in `segments` was tagged by the exact same insertion
        // gate that put it there (extract_into's first-population window,
        // or the `!segments.contains_key` checks above), so the lookup
        // below always hits; `SealedBelowWatermark` is the fail-closed
        // default (counted, not exempted) if that invariant is ever wrong.
        let mut origins = SegmentOrigins::default();
        for segment in &segments {
            let origin = origin_by_key
                .remove(&segment.data_object_key)
                .unwrap_or(SegmentOrigin::SealedBelowWatermark);
            origins.push(origin);
        }
        Ok((
            Snapshot {
                segments,
                segments_pruned,
                pending_erasure,
            },
            origins,
            generations,
        ))
    }

    /// Upper bound on the store requests a `resolve`/`resolve_pruned` call
    /// for this `(range, now_ns)` window will issue before it has listed
    /// anything, computed without running any part of resolve itself
    /// (ADR-0044 decision 3, "catalog term": computed before
    /// `Catalog::resolve` runs, from `shard_count` and the number of hour
    /// buckets the padded window spans -- both inputs the planner already
    /// holds).
    ///
    /// Two parts:
    ///
    /// - LIST requests: one per `(shard, hour)` pair, i.e. what
    ///   `resolve_impl` issues with no snapshot watermark to shorten the
    ///   listed suffix (a watermark can only narrow the listed range, never
    ///   widen it, so this bound holds whether or not folding has run).
    /// - `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`: the snapshot-window path
    ///   (`Catalog::resolve_snapshot_window`) that `resolve_impl` tries first, whenever the window is
    ///   non-empty, before any LIST runs. This is not derivable from
    ///   `shard_count`/hour buckets and is folded in as a documented
    ///   constant instead -- see that constant's doc comment for the open
    ///   gap this leaves (ADR-0044 decision 3 does not account for it).
    ///
    /// The record GETs `resolve` also issues for whatever those LISTs turn up
    /// are not included here: which records exist, and how many, is only
    /// knowable after a LIST actually runs, so they cannot be bounded before
    /// resolve starts.
    ///
    /// This number is a true upper envelope of what `resolve` actually issues
    /// on either traversal path (ADR-0044 decision 3, amended for ADR-0056):
    ///
    /// - The per-bucket loop issues exactly this many LISTs (one per bucket,
    ///   under the sparse-bucket assumption of at most `page_size` objects per
    ///   `(shard, hour)` -- the same assumption that lets this formula count
    ///   one LIST per bucket and ignore intra-bucket pagination).
    /// - The prefix scan (ADR-0056) issues `O(objects / page_size)` LISTs,
    ///   strictly fewer than the per-bucket loop's `shard_count * hours` base
    ///   for any window it is chosen for, so this bound still holds over it.
    /// - `resolve` also issues one unconditional `del/` LIST every call
    ///   (ADR-0064 decision 2), regardless of window emptiness. On the
    ///   non-empty-window branch this is not separately counted here -- it
    ///   rides inside [`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`]'s existing
    ///   slack, which still exceeds real usage with it included, so the
    ///   envelope holds without a dedicated term (though with less margin
    ///   than before; a future addition to that path should re-check this).
    ///   On the empty-window branch there is no other slack to absorb it, so
    ///   it is counted explicitly via [`PENDING_ERASURE_LIST_UPPER_BOUND`] --
    ///   without it this branch under-counted (`0` estimated against `1`
    ///   actual), which is the one case where this used to not be a true
    ///   upper envelope.
    ///
    /// It is reported for cost accounting (ADR-0044 decision 3, threaded
    /// through `ravel-query`/`ravel-sql`) and is no longer itself the
    /// admission gate for wide windows: rather than refuse a window whose
    /// per-bucket cost would exceed
    /// [`CatalogConfig::max_catalog_list_requests`](crate::CatalogConfig::max_catalog_list_requests),
    /// `resolve` routes it to the non-amplifying prefix path and caps that
    /// path's LIST count at the same ceiling at runtime (the request
    /// bound preserved). It remains an upper envelope and never a
    /// prediction; a folded tenant's real LIST count is far lower.
    pub fn estimated_catalog_requests(&self, range: TimeRange, now_ns: i64) -> u64 {
        match self.window_hour_bounds(range, now_ns) {
            Some((start_hour, end_hour)) => {
                // One LIST per (shard, hour) pair, but the per-hour shard
                // fan-out is now `scan_count(h)`, not a constant (ADR-0052
                // section 4), so this sums `scan_count(h)` over the window's
                // hours instead of multiplying a single count.
                //
                // This is a pure, I/O-free planner input (ADR-0044 decision 3:
                // computed before resolve runs, from inputs the planner already
                // holds), so it cannot read the provisioning record `resolve`
                // reads fresh. It sums over the process's configured baseline
                // (the single implicit generation 0 at `shard_count`), which
                // equals the pre-ADR-0052 `shard_count * hours`. After a
                // reshard-increase the true per-hour fan-out `resolve` computes
                // can exceed this; the value stays an advisory estimate, not a
                // hard bound (the real fan-out is bounded by resolve's own
                // generation-aware loop, which reads the record).
                let generations = [implicit_generation_zero(self.config.shard_count)];
                let mut list_requests: u64 = 0;
                for hour in start_hour..=end_hour {
                    let scan = crate::provisioning::scan_count(
                        &generations,
                        hour,
                        crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
                    );
                    list_requests = list_requests.saturating_add(u64::from(scan));
                }
                list_requests.saturating_add(SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND)
            }
            None => PENDING_ERASURE_LIST_UPPER_BOUND,
        }
    }

    /// The (start_hour, end_hour) ingest-hour bucket range the listing
    /// window covers, inclusive, or `None` if the window is empty. Applies
    /// to every shard alike; callers cross it with `0..shard_count`
    /// themselves.
    fn window_hour_bounds(&self, range: TimeRange, now_ns: i64) -> Option<(u32, u32)> {
        let window_start_ns = range.start_ns.saturating_sub(self.config.max_ingest_lag_ns);
        let window_end_ns = now_ns.saturating_add(self.config.clock_skew_allowance_ns);
        if window_end_ns < 0 {
            return None;
        }
        let start_hour = window_start_ns.div_euclid(NS_PER_HOUR).max(0);
        let end_hour = window_end_ns.div_euclid(NS_PER_HOUR);
        if start_hour > end_hour {
            return None;
        }
        let start_hour = u32::try_from(start_hour).unwrap_or(0);
        let end_hour = u32::try_from(end_hour).unwrap_or(u32::MAX);
        Some((start_hour, end_hour))
    }

    /// List the commit buckets for the listing suffix
    /// `[listing_start_hour, window_end_hour]` with one bounded LIST per shard
    /// (docs/catalog-and-mvcc.md "Snapshot resolution (Phase 1)"), the
    /// non-prefix traversal path. Replaces the former one-LIST-per-(shard,
    /// hour) loop: each shard's below-watermark history is skipped server-side
    /// by a `list_after` start marker and its tail is drained in one wave, so a
    /// full-window resolve over an 8-shard tenant with a 3-hour unsealed tail
    /// costs 8 LISTs, not 24.
    ///
    /// The per-shard LIST bound is the union scan set over the suffix
    /// ([`crate::provisioning::max_scan_count_over_range`]): a per-shard LIST
    /// cannot vary its shard bound per hour, so it lists up to the max seen
    /// over the range. A bucket whose shard index is outside its OWN hour's
    /// `scan_count` is then dropped before `process_bucket`, so the INCLUDED
    /// set is exactly the per-`(shard, hour)` loop's -- a retiring larger
    /// generation's higher shards stay listed only for the hours inside their
    /// `DEFAULT_SCAN_SLACK_HOURS` window past a decrease, never beyond it
    /// (ADR-0052 section 4). This makes the resolved snapshot byte-identical to
    /// the old per-bucket loop; it is NOT the prefix scan, which is a documented
    /// over-scanning superset (see [`Catalog::list_window_by_prefix`] and
    /// tests/resharding_prefix_traversal.rs).
    #[allow(clippy::too_many_arguments)]
    async fn list_window_bounded(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        listing_start_hour: u32,
        window_end_hour: u32,
        range: TimeRange,
        generations: &[crate::provisioning::ShardGeneration],
        accounting: &QueryAccounting,
    ) -> Result<HashMap<String, SegmentRef>, CatalogError> {
        // Union scan set over the suffix: the widest per-shard bound any hour
        // in the range needs (a per-shard LIST cannot vary per hour).
        let scan_shards = crate::provisioning::max_scan_count_over_range(
            generations,
            listing_start_hour,
            window_end_hour,
            crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
        );
        // One bounded LIST per shard, concurrently under the resolve-wide
        // semaphore (each `list_shard_hours` acquires it per page). Shards
        // partition the key space, so their grouped buckets never collide.
        let shard_groups: Vec<Result<ShardBuckets, CatalogError>> = stream::iter(0..scan_shards)
            .map(|shard| {
                self.list_shard_hours(
                    tenant,
                    signal,
                    shard,
                    listing_start_hour,
                    window_end_hour,
                    accounting,
                )
            })
            .buffered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        let mut grouped: ShardBuckets = HashMap::new();
        for shard_group in shard_groups {
            for ((shard, hour), objs) in shard_group? {
                // Per-hour scan set (ADR-0052 section 4): the union LIST bound
                // over-scans shard indices that THIS hour does not need (a
                // retiring generation's higher shards past their slack window).
                // Drop those buckets so the included set matches the
                // per-(shard, hour) loop exactly, which never listed them.
                let hour_scan = crate::provisioning::scan_count(
                    generations,
                    hour,
                    crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
                );
                if shard >= hour_scan {
                    continue;
                }
                grouped.entry((shard, hour)).or_default().extend(objs);
            }
        }

        // Resolve each surviving bucket through the shared per-bucket path,
        // concurrently. resolve_fanout's final deterministic sort makes the
        // result independent of merge order.
        let coords: Vec<((u32, u32), Vec<ObjectMeta>)> = grouped.into_iter().collect();
        let bucket_maps: Vec<Result<HashMap<String, SegmentRef>, CatalogError>> =
            stream::iter(coords)
                .map(|((shard, hour), objs)| async move {
                    self.process_bucket(tenant, signal, shard, hour, objs, range, accounting)
                        .await
                })
                .buffered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await;
        let mut out: HashMap<String, SegmentRef> = HashMap::new();
        for bucket in bucket_maps {
            for (key, segment_ref) in bucket? {
                out.entry(key).or_insert(segment_ref);
            }
        }
        Ok(out)
    }

    /// List one shard's commit buckets for the hours in
    /// `[listing_start_hour, window_end_hour]` with a single bounded LIST,
    /// grouping the keys by `(shard, hour)` for [`Catalog::process_bucket`].
    ///
    /// The scan starts strictly after
    /// `commit_shard_hour_prefix(.., listing_start_hour)`: that prefix string
    /// sorts before every key under `listing_start_hour`, so `list_after`
    /// skips the shard's whole below-watermark history server-side rather than
    /// paging through and discarding it. Paging stops at the first key whose
    /// hour is past `window_end_hour` -- the `YYYYMMDDTHH` hour strings are
    /// fixed-width and sort chronologically (ravel-commit keys), so once one
    /// past-window key appears every later key in the shard is also past it.
    /// Each page issued is one `AccountedOp::List` in `accounting`, so the
    /// request count stays exact.
    async fn list_shard_hours(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        listing_start_hour: u32,
        window_end_hour: u32,
        accounting: &QueryAccounting,
    ) -> Result<ShardBuckets, CatalogError> {
        let tenant_prefix = format!("t/{}/", tenant.to_hex());
        let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
        let start_after =
            keys::commit_shard_hour_prefix(tenant, signal, shard, listing_start_hour)?;
        let mut grouped: ShardBuckets = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut page_token = None;
        'pages: loop {
            let page = {
                let _permit = self.request_semaphore.acquire().await.map_err(|_| {
                    StoreError::Transient("catalog request semaphore closed".to_string())
                })?;
                let page = self
                    .store
                    .list_after(&prefix, Some(&start_after), page_token)
                    .await;
                accounting.record_s3_request(AccountedOp::List);
                page?
            };
            for meta in page.objects {
                // ADR-0050 §2 isolation assertion, identical to
                // `guarded_list_all`: every returned key is under this tenant's
                // prefix or the scan hard-fails.
                if !meta.key.starts_with(&tenant_prefix) {
                    self.record_isolation_breach();
                    return Err(CatalogError::FieldMismatch {
                        key: prefix.clone(),
                        field: "list_prefix",
                        expected: tenant_prefix,
                        actual: meta.key,
                    });
                }
                // Dedup by key across pages (a key MAY repeat across pages),
                // matching `guarded_list_all` so the grouped key set is
                // identical to the per-bucket loop's.
                if !seen.insert(meta.key.clone()) {
                    continue;
                }
                let (bshard, bhour) = match keys::partition_bucket_entry(&meta.key)? {
                    BucketEntry::CommitRecord(k) => (k.shard, k.ingest_hour_bucket),
                    BucketEntry::CompactionRecord(k) => (k.shard, k.ingest_hour_bucket),
                    BucketEntry::RewriteRecord(k) => (k.shard, k.ingest_hour_bucket),
                    BucketEntry::Tombstone(k) => (k.shard, k.ingest_hour_bucket),
                };
                // Past the window's top hour: every later key in this shard is
                // also past it, so stop paging (do not request the next page).
                if bhour > window_end_hour {
                    break 'pages;
                }
                // `start_after` already excluded everything strictly below
                // `listing_start_hour`; this guard is a belt for a marker key
                // that shares the first page with in-window keys.
                if bhour < listing_start_hour {
                    continue;
                }
                grouped.entry((bshard, bhour)).or_default().push(meta);
            }
            match page.next {
                Some(next) => page_token = Some(next),
                None => break,
            }
        }
        Ok(grouped)
    }

    /// Resolve one `(shard, ingest-hour)` bucket's already-listed keys into its
    /// segment contribution (docs/catalog-and-mvcc.md steps 2-4). Shared
    /// verbatim by both traversal paths (ADR-0056): the per-bucket loop passes
    /// the keys from its per-bucket LIST, the prefix scan passes the keys it
    /// grouped by `(shard, hour)` from its per-shard recursive LIST. The
    /// per-bucket compaction/tombstone/interlock logic is a pure function of
    /// the set of keys in one bucket, so grouping a wider listing by bucket and
    /// running this per group yields the identical snapshot the per-bucket loop
    /// yields -- the property the differential test
    /// pins.
    #[allow(clippy::too_many_arguments)]
    async fn process_bucket(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
        objects: Vec<ObjectMeta>,
        range: TimeRange,
        accounting: &QueryAccounting,
    ) -> Result<HashMap<String, SegmentRef>, CatalogError> {
        let mut out: HashMap<String, SegmentRef> = HashMap::new();
        let prefix = keys::commit_shard_hour_prefix(tenant, signal, shard, hour)?;

        // Partition the listed keys by shape (docs/catalog-and-mvcc.md step
        // 2). An unrecognized shape is a fail-loud error, never a silent
        // skip (fail-loud on layout drift).
        let mut l0_keys: Vec<String> = Vec::new();
        let mut compaction_keys: Vec<String> = Vec::new();
        let mut rewrite_keys: Vec<String> = Vec::new();
        let mut has_tombstone = false;
        for meta in objects {
            match keys::partition_bucket_entry(&meta.key)? {
                BucketEntry::CommitRecord(_) => l0_keys.push(meta.key),
                BucketEntry::CompactionRecord(_) => compaction_keys.push(meta.key),
                BucketEntry::RewriteRecord(_) => rewrite_keys.push(meta.key),
                BucketEntry::Tombstone(_) => has_tombstone = true,
            }
        }

        // Tombstone present: the bucket contributes nothing (ADR-0019,
        // docs/catalog-and-mvcc.md step 3). Observing the tombstone also
        // invalidates this bucket's cached commit and compaction records
        // (ADR-0010 §10's promised trigger), so a later token-fallback GET
        // cannot serve a record the retention sweep is about to remove.
        if has_tombstone {
            self.invalidate_bucket_cache(tenant, &prefix);
            return Ok(out);
        }

        // Warm the record caches for this bucket concurrently before the
        // sequential include logic runs: the includes below
        // then hit the cache instead of each paying a serial GET. A GET
        // failure surfaces here, exactly as it would have from the first
        // sequential load.
        self.prewarm_commit_records(tenant, signal, shard, &l0_keys, accounting)
            .await?;
        self.prewarm_compaction_records(tenant, signal, shard, &compaction_keys, accounting)
            .await?;

        // No compaction AND no rewrite record: Phase 1 behavior, every
        // overlapping L0. A rewrite record supersedes inputs exactly as a
        // compaction record does, so its presence alone (even with no
        // compaction record) rules out this fast path.
        if compaction_keys.is_empty() && rewrite_keys.is_empty() {
            for key in &l0_keys {
                self.include_l0_if_overlaps(
                    tenant, signal, shard, key, &range, &mut out, accounting,
                )
                .await?;
            }
            return Ok(out);
        }

        // Compaction and/or rewrite record(s) present (docs/catalog-and-mvcc.md
        // step 3; ADR-0064 decision 3). Load every compaction record: collect
        // its input identities into the SINGLE `excluded` set, and remember the
        // newest record's created_unix_ns for the interlock check on unlisted
        // L0s. Parts are included below, after the rewrite supersession pass,
        // so a part a rewrite superseded is never included.
        let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
        let mut newest_record_created_ns = i64::MIN;
        let mut input_set_hashes: HashSet<Vec<u8>> = HashSet::new();
        let mut compaction_records: Vec<(String, Arc<CompactionRecord>)> =
            Vec::with_capacity(compaction_keys.len());
        for ckey in &compaction_keys {
            let record = self
                .load_and_validate_compaction(tenant, signal, shard, ckey, accounting)
                .await?;
            input_set_hashes.insert(record.input_set_hash.clone());
            newest_record_created_ns = newest_record_created_ns.max(record.created_unix_ns);
            for input in &record.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
            compaction_records.push((ckey.clone(), record));
        }

        // Load every rewrite record (ADR-0064 decision 3). Same-bucket, key
        // verified against the record's own identity (ADR-0010 §7) inside
        // `load_and_validate_rewrite`.
        let mut rewrite_records: Vec<(String, Arc<RewriteRecord>)> =
            Vec::with_capacity(rewrite_keys.len());
        for rkey in &rewrite_keys {
            let record = self
                .load_and_validate_rewrite(tenant, signal, shard, rkey, accounting)
                .await?;
            newest_record_created_ns = newest_record_created_ns.max(record.created_unix_ns);
            rewrite_records.push((rkey.clone(), record));
        }

        // Resolve every rewrite's supersession into two unified exclusion sets:
        // `excluded` gains the rewrite's effective L0 input identities (its own
        // `inputs`, or those reached by chasing `superseded_record_key`), and
        // `superseded_records` gains the keys of any compaction/rewrite record a
        // rewrite superseded as a whole -- whose output parts must therefore be
        // excluded (overlap harmlessness does NOT hold across a rewrite,
        // ADR-0064 decision 3 point 5). The chase is bounded and cycle-checked;
        // an over-deep or cyclic chain is a typed error, never a hang.
        let mut superseded_records: HashSet<String> = HashSet::new();
        if !rewrite_records.is_empty() {
            let compaction_by_key: HashMap<&str, &CompactionRecord> = compaction_records
                .iter()
                .map(|(k, r)| (k.as_str(), r.as_ref()))
                .collect();
            let rewrite_by_key: HashMap<&str, &RewriteRecord> = rewrite_records
                .iter()
                .map(|(k, r)| (k.as_str(), r.as_ref()))
                .collect();
            for (rkey, record) in &rewrite_records {
                resolve_rewrite_supersession(
                    rkey,
                    record,
                    &prefix,
                    &compaction_by_key,
                    &rewrite_by_key,
                    &mut excluded,
                    &mut superseded_records,
                )?;
            }
        }

        // Compaction parts: include each non-superseded record's parts
        // (event-bound filtered). A record whose whole output a live rewrite
        // superseded is skipped -- its parts would resurrect erased records.
        for (ckey, record) in &compaction_records {
            if superseded_records.contains(ckey) {
                continue;
            }
            for part in &record.parts {
                let segment_ref = build_l1_segment_ref(record, part, ckey)?;
                let event_range = TimeRange {
                    start_ns: segment_ref.min_event_ts_ns,
                    end_ns: segment_ref.max_event_ts_ns,
                };
                if event_range.overlaps(&range) {
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
            }
        }

        // Rewrite output parts: folded in as L1-equivalent entries exactly as
        // compaction parts are (ADR-0064 decision 3 point 5), unless this
        // rewrite is itself superseded by a newer one (a bucket erased twice,
        // ADR-0064 amendment). A rewrite's parts may be empty (a bucket whose
        // every record matched is rewritten to nothing), which contributes
        // nothing here.
        for (rkey, record) in &rewrite_records {
            if superseded_records.contains(rkey) {
                continue;
            }
            for part in &record.parts {
                let segment_ref = build_rewrite_l1_segment_ref(record, part, rkey)?;
                let event_range = TimeRange {
                    start_ns: segment_ref.min_event_ts_ns,
                    end_ns: segment_ref.max_event_ts_ns,
                };
                if event_range.overlaps(&range) {
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
            }
        }

        // Two compaction records with different input_set_hash in one bucket:
        // both parts sets are already included above (harmless overlap); alarm
        // loudly (docs/catalog-and-mvcc.md step 3, §3.6 row 11).
        if input_set_hashes.len() > 1 {
            self.compaction_input_set_conflicts
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                shard,
                hour,
                records = input_set_hashes.len(),
                "compaction interlock breach: sealed bucket holds multiple input sets"
            );
        }

        self.check_rewrite_siblings(shard, hour, &rewrite_records, &superseded_records);

        // L0 records: exclude exactly those named in an input list (a
        // compaction record's inputs, or a rewrite's effective inputs); include
        // any unlisted one normally, raising the interlock metric if it
        // postdates the newest compaction/rewrite record (docs/catalog-and-mvcc.md
        // step 3).
        for key in &l0_keys {
            let record = self
                .load_and_validate(tenant, signal, shard, key, accounting)
                .await?;
            let identity = (
                record.writer_id.clone(),
                record.writer_epoch,
                record.writer_seq,
            );
            if excluded.contains(&identity) {
                continue;
            }
            if record.created_unix_ns > newest_record_created_ns {
                self.interlock_violations.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    shard,
                    hour,
                    key = %key,
                    "writer-interlock violation: L0 record postdates its bucket's compaction record"
                );
            }
            let event_range = TimeRange {
                start_ns: record.min_event_ts_ns,
                end_ns: record.max_event_ts_ns,
            };
            if event_range.overlaps(&range) {
                let segment_ref = build_segment_ref(key, &record)?;
                out.entry(segment_ref.data_object_key.clone())
                    .or_insert(segment_ref);
            }
        }
        Ok(out)
    }

    /// The prefix-scan traversal path (ADR-0056): one drained recursive
    /// `list_after` per shard over [`keys::commit_shard_prefix`], grouped
    /// client-side by `(shard, ingest-hour)` and resolved through
    /// [`Catalog::process_bucket`], for the window buckets in
    /// `[listing_start_hour, window_end_hour]`.
    ///
    /// Cost is `O(objects above the watermark / page_size)`, independent of
    /// window width, versus the pre-ADR-0056 one LIST per `(shard, hour)`
    /// bucket (ADR-0056 measurement). Each shard's scan resumes strictly past
    /// the `listing_start_hour` shard-hour prefix via the `list_after` start
    /// marker, so the shard's below-watermark history is skipped server-side
    /// rather than paged through and discarded, exactly as the non-prefix
    /// path ([`Catalog::list_window_bounded`]) does. What the recursive scan
    /// still over-reads is the other end: keys past `window_end_hour`, which
    /// the client-side filter drops. The range resolved is unchanged, only
    /// the scan method (ADR-0056 INTERACTION 2): a key in any in-range bucket,
    /// however far below the window end, is still grouped and resolved.
    ///
    /// A running LIST count is capped at
    /// [`CatalogConfig::max_catalog_list_requests`](crate::CatalogConfig::max_catalog_list_requests)
    /// and aborts with [`CatalogError::WindowTooWide`] before issuing a page
    /// that would exceed it. This is the request bound, enforced at
    /// runtime on the one path whose cost is not knowable before listing: a
    /// wide-but-sparse window is served, and only a scan whose actual object
    /// volume is unsustainable is refused.
    ///
    /// That page-by-page ceiling, and the `list_after` start marker, are why
    /// this path records its own `AccountedOp::List` per page instead of
    /// calling [`Catalog::guarded_list_all`], which takes no start marker and
    /// drains unconditionally. The permit, the accounting, and the ADR-0050
    /// section 2 tenant-prefix assertion are the same either way
    /// (docs/catalog-and-mvcc.md "Query cost accounting").
    #[allow(clippy::too_many_arguments)]
    async fn list_window_by_prefix(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        listing_start_hour: u32,
        window_end_hour: u32,
        range: TimeRange,
        generations: &[crate::provisioning::ShardGeneration],
        accounting: &QueryAccounting,
    ) -> Result<HashMap<String, SegmentRef>, CatalogError> {
        // One recursive LIST per shard, grouping keys by (shard, hour), each
        // shard's scan bounded below the watermark by a `list_after` start
        // marker (below) so it does not page through the shard's whole history
        // before reaching the listing suffix. The LISTs drain sequentially so
        // the runtime request cap is checked deterministically page by page;
        // the expensive per-bucket record GETs are what run concurrently below,
        // mirroring the per-bucket loop's concurrency model.
        let mut grouped: HashMap<(u32, u32), Vec<ObjectMeta>> = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut lists_issued: u64 = 0;
        let cap = self.config.max_catalog_list_requests;
        let tenant_prefix = format!("t/{}/", tenant.to_hex());
        // Shard bound is the union scan set over every hour in the listing
        // suffix (ADR-0052 section 4), not the static
        // `self.config.shard_count`. `max_scan_count_over_range` keeps a
        // retiring larger generation's higher shard indices in scope for
        // `DEFAULT_SCAN_SLACK_HOURS` past its successor's activation, so on a
        // shard-count decrease a straggler routed under the old count into an
        // early successor hour is still listed. The non-prefix bounded path
        // ([`Catalog::list_window_bounded`]) uses this same bound; a per-shard
        // recursive LIST cannot vary the bound per hour, so it takes the max
        // over the range.
        let scan_shards = crate::provisioning::max_scan_count_over_range(
            generations,
            listing_start_hour,
            window_end_hour,
            crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
        );
        for shard in 0..scan_shards {
            let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
            // Skip the shard's below-watermark history server-side: the
            // shard-hour prefix for `listing_start_hour` sorts before every
            // key at or above it, so `list_after` resumes strictly past it.
            let start_after =
                keys::commit_shard_hour_prefix(tenant, signal, shard, listing_start_hour)?;
            let mut page_token = None;
            loop {
                // Refuse before issuing a page that would exceed the ceiling,
                // so at most `cap` LISTs are ever issued (the request bound).
                if lists_issued >= cap {
                    return Err(CatalogError::WindowTooWide {
                        estimate: lists_issued.saturating_add(1),
                        limit: cap,
                    });
                }
                let page = {
                    let _permit = self.request_semaphore.acquire().await.map_err(|_| {
                        StoreError::Transient("catalog request semaphore closed".to_string())
                    })?;
                    let page = self
                        .store
                        .list_after(&prefix, Some(&start_after), page_token)
                        .await;
                    accounting.record_s3_request(AccountedOp::List);
                    lists_issued += 1;
                    page?
                };
                for meta in page.objects {
                    // ADR-0050 §2 isolation assertion, identical to
                    // `guarded_list_all`: every returned key is under this
                    // tenant's prefix or the scan hard-fails.
                    if !meta.key.starts_with(&tenant_prefix) {
                        self.record_isolation_breach();
                        return Err(CatalogError::FieldMismatch {
                            key: prefix.clone(),
                            field: "list_prefix",
                            expected: tenant_prefix,
                            actual: meta.key,
                        });
                    }
                    // Dedup by key across pages (the cross-page listing
                    // guarantee: a key MAY repeat across pages), matching
                    // `guarded_list_all` so the grouped key set is identical to
                    // the per-bucket loop's.
                    if !seen.insert(meta.key.clone()) {
                        continue;
                    }
                    let (bshard, bhour) = match keys::partition_bucket_entry(&meta.key)? {
                        BucketEntry::CommitRecord(k) => (k.shard, k.ingest_hour_bucket),
                        BucketEntry::CompactionRecord(k) => (k.shard, k.ingest_hour_bucket),
                        BucketEntry::RewriteRecord(k) => (k.shard, k.ingest_hour_bucket),
                        BucketEntry::Tombstone(k) => (k.shard, k.ingest_hour_bucket),
                    };
                    if bhour < listing_start_hour || bhour > window_end_hour {
                        continue;
                    }
                    grouped.entry((bshard, bhour)).or_default().push(meta);
                }
                match page.next {
                    Some(next) => page_token = Some(next),
                    None => break,
                }
            }
        }

        // Resolve each surviving bucket through the shared per-bucket path,
        // concurrently under the resolve-wide semaphore. Bucket keys never
        // collide across buckets, and resolve_impl's final deterministic sort
        // makes the result independent of merge order.
        let coords: Vec<((u32, u32), Vec<ObjectMeta>)> = grouped.into_iter().collect();
        let bucket_maps: Vec<Result<HashMap<String, SegmentRef>, CatalogError>> =
            stream::iter(coords)
                .map(|((shard, hour), objs)| async move {
                    self.process_bucket(tenant, signal, shard, hour, objs, range, accounting)
                        .await
                })
                .buffered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await;
        let mut out: HashMap<String, SegmentRef> = HashMap::new();
        for bucket in bucket_maps {
            for (key, segment_ref) in bucket? {
                out.entry(key).or_insert(segment_ref);
            }
        }
        Ok(out)
    }

    /// Concurrently load and cache every commit record in `keys` under the
    /// resolve-wide semaphore, so a later cache-first read of
    /// each is a hit. Returns the first load error; the sequential include
    /// logic never re-issues a GET a successful prewarm already cached.
    async fn prewarm_commit_records(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        keys: &[String],
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        // Owned key clones, not borrowed slice items: a stream closure that
        // borrows each `&String` makes rustc infer a non-higher-ranked
        // lifetime for the future, which then fails to unify with axum's
        // `Handler` blanket impl at the HTTP router (the same "FnOnce is not
        // general enough" wall the prefetch closure in `ravel-query` hit).
        let loads: Vec<Result<(), CatalogError>> = stream::iter(keys.iter().cloned())
            .map(|key| async move {
                self.load_and_validate(tenant, signal, shard, &key, accounting)
                    .await
                    .map(|_| ())
            })
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        for load in loads {
            load?;
        }
        Ok(())
    }

    /// Compaction-record counterpart to
    /// [`prewarm_commit_records`](Self::prewarm_commit_records).
    async fn prewarm_compaction_records(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        keys: &[String],
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let loads: Vec<Result<(), CatalogError>> = stream::iter(keys.iter().cloned())
            .map(|key| async move {
                self.load_and_validate_compaction(tenant, signal, shard, &key, accounting)
                    .await
                    .map(|_| ())
            })
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        for load in loads {
            load?;
        }
        Ok(())
    }

    /// Load one L0 commit record and, if its event range overlaps `range`,
    /// add its segment ref to `out`. The plain Phase 1 include path, shared
    /// by the no-compaction fast path.
    #[allow(clippy::too_many_arguments)]
    async fn include_l0_if_overlaps(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let record = self
            .load_and_validate(tenant, signal, shard, key, accounting)
            .await?;
        let event_range = TimeRange {
            start_ns: record.min_event_ts_ns,
            end_ns: record.max_event_ts_ns,
        };
        if event_range.overlaps(range) {
            let segment_ref = build_segment_ref(key, &record)?;
            out.entry(segment_ref.data_object_key.clone())
                .or_insert(segment_ref);
        }
        Ok(())
    }

    /// Load, decode, validate, and cache a compaction record, keyed by full
    /// object key, exactly as [`Catalog::load_and_validate`] does for commit
    /// records (docs/catalog-and-mvcc.md step 2). Validation covers
    /// tenant_hash/signal/shard against the (tenant, signal, shard) it was
    /// listed under plus the reconstruct-and-verify of its own key
    /// (ADR-0010 §7).
    pub(crate) async fn load_and_validate_compaction(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Result<Arc<CompactionRecord>, CatalogError> {
        if let Some(cached) = self.compaction_cache.get(tenant, key, accounting) {
            validate_compaction_expected_fields(self, &cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        let bytes = got.data.len() as u64;
        let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
            CatalogError::CompactionRecordDecode {
                key: key.to_string(),
                source: e,
            }
        })?;
        validate_compaction_expected_fields(self, &record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.compaction_cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            bytes,
            self.config.cache_capacity_per_tenant,
        );
        Ok(record)
    }

    /// Load, decode, and validate a rewrite record (ADR-0064 decision 3). Not
    /// cached: rewrite records are rare (one per erasure batch over a bucket),
    /// so a fresh GET+decode per resolve is cheap and avoids a fifth record
    /// cache. `decode_rewrite` already re-verifies the record's own
    /// `input_set_hash` and its `superseded_record_key` bucket-match on decode;
    /// this additionally checks tenant_hash/signal/shard against the (tenant,
    /// signal, shard) it was listed under and verifies its observed key
    /// reconstructs from its own identity fields (ADR-0010 §7), exactly the
    /// discipline every other record type in the bucket loop gets.
    pub(crate) async fn load_and_validate_rewrite(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Result<Arc<RewriteRecord>, CatalogError> {
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        let record = erasure::decode_rewrite(got.data.as_ref()).map_err(|source| {
            CatalogError::RewriteRecordDecode {
                key: key.to_string(),
                source,
            }
        })?;
        validate_rewrite_expected_fields(self, &record, tenant, signal, shard, key)?;
        Ok(Arc::new(record))
    }

    /// List `t/<th>/<sig>/del/` once and decode every pending erasure request
    /// (`.dreq`) into the resolved snapshot's `pending_erasure` (ADR-0064
    /// decision 2). Empty for the common no-erasure case, at the cost of
    /// exactly one LIST and nothing more. `.done` completion records (PII-free
    /// audit evidence) are recognized and skipped; any other shape under `del/`
    /// is layout drift and fails the resolve loudly, never silently dropped.
    ///
    /// Each `.dreq` is decoded and structurally validated (`decode_request`),
    /// and its observed key is verified against the request's own identity
    /// fields (ADR-0010 §7) -- which, since the listed key is already asserted
    /// under this tenant's prefix by `guarded_list_all`, also proves the
    /// request's tenant_hash/signal match this (tenant, signal).
    async fn list_pending_erasure(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        accounting: &QueryAccounting,
    ) -> Result<Vec<ErasureRequest>, CatalogError> {
        let prefix = keys::del_prefix(tenant, signal);
        let objects = self.guarded_list_all(tenant, &prefix, accounting).await?;
        let mut pending = Vec::new();
        for meta in objects {
            // Classify by shape: `.dreq` carries a predicate; `.done` is a
            // completion record with no predicate and is skipped; anything else
            // is layout drift.
            if keys::parse_erasure_request_key(&meta.key).is_ok() {
                let got = self
                    .guarded_get(&meta.key, GetRange::Full, accounting)
                    .await?;
                let record = erasure::decode_request(got.data.as_ref()).map_err(|source| {
                    CatalogError::ErasureRequestDecode {
                        key: meta.key.clone(),
                        source,
                    }
                })?;
                keys::verify_erasure_request_key(&record, &meta.key).map_err(|source| {
                    CatalogError::Reconstruction {
                        key: meta.key.clone(),
                        source,
                    }
                })?;
                pending.push(record);
            } else if keys::parse_erasure_completion_key(&meta.key).is_ok() {
                // `.done`: permanent, PII-free audit evidence, no predicate to
                // attach (ADR-0064 decision 1). Skip.
            } else {
                return Err(CatalogError::Key(keys::KeyError::UnknownBucketEntryShape(
                    meta.key,
                )));
            }
        }
        Ok(pending)
    }

    /// Drop the cached commit and compaction records for one bucket, keyed by
    /// its `c/<shard>/<hour>/` prefix. The single tombstone-observation
    /// invalidation path (ADR-0010 §10): called from both the listing resolve
    /// and the token fallback the moment a tombstone is seen. Both record
    /// caches key by full object key, and every record in a bucket shares this
    /// prefix, so one prefix drop clears the bucket from both.
    fn invalidate_bucket_cache(&self, tenant: &TenantHash, bucket_prefix: &str) {
        self.cache.invalidate_prefix(tenant, bucket_prefix);
        self.compaction_cache
            .invalidate_prefix(tenant, bucket_prefix);
    }

    async fn resolve_min_token(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        token: &CommitToken,
        out: &mut HashMap<String, SegmentRef>,
        origin_by_key: &mut HashMap<String, SegmentOrigin>,
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let key = keys::commit_key_for_token(tenant, signal, token)?;
        if let Some(cached) = self.cache.get(tenant, &key, accounting)
            && validate_expected_fields(self, &cached, tenant, signal, token.shard, &key).is_ok()
        {
            let segment_ref = build_segment_ref(&key, &cached)?;
            if !out.contains_key(&segment_ref.data_object_key) {
                origin_by_key.insert(
                    segment_ref.data_object_key.clone(),
                    SegmentOrigin::TokenResolved,
                );
            }
            out.entry(segment_ref.data_object_key.clone())
                .or_insert(segment_ref);
            return Ok(());
        }

        // Independent retry budgets for the two distinct failure modes. A
        // transient store fault (Throttled/Timeout/Transient) and a NotFound
        // (a real record not yet propagated) are unrelated, so they must not
        // draw from one shared budget: if they did, a transient blip would
        // spend the single NotFound-propagation retry the spec grants
        // (docs/catalog-and-mvcc.md step 4: "Absent after one retry"), so a
        // real but briefly-unpropagated commit would surface as a spurious
        // UnsatisfiableToken, violating read-your-write. NotFound
        // keeps exactly one retry (two probes) as documented; transient
        // errors keep their own independent single retry.
        let mut notfound_retries: u32 = 1;
        let mut transient_retries: u32 = 1;
        loop {
            match self.guarded_get(&key, GetRange::Full, accounting).await {
                Ok(got) => {
                    let bytes = got.data.len() as u64;
                    let record = record::decode(&got.data)?;
                    validate_expected_fields(self, &record, tenant, signal, token.shard, &key)?;
                    let record = Arc::new(record);
                    self.cache.insert(
                        *tenant,
                        key.clone(),
                        record.clone(),
                        bytes,
                        self.config.cache_capacity_per_tenant,
                    );
                    let segment_ref = build_segment_ref(&key, &record)?;
                    if !out.contains_key(&segment_ref.data_object_key) {
                        origin_by_key.insert(
                            segment_ref.data_object_key.clone(),
                            SegmentOrigin::TokenResolved,
                        );
                    }
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                    return Ok(());
                }
                Err(StoreError::NotFound) if notfound_retries > 0 => {
                    notfound_retries -= 1;
                    tokio::time::sleep(MIN_TOKEN_RETRY_DELAY).await;
                }
                Err(StoreError::NotFound) => {
                    // The commit record is absent after one propagation retry.
                    // Before declaring the token unsatisfiable, try the
                    // compaction/tombstone fallback (docs/catalog-and-mvcc.md
                    // step 5): a swept-post-compaction record is served via its
                    // L1 parts, a tombstoned bucket is satisfied with zero
                    // segments.
                    return self
                        .resolve_min_token_fallback(
                            tenant,
                            signal,
                            token,
                            out,
                            origin_by_key,
                            accounting,
                        )
                        .await;
                }
                Err(e) if e.is_retryable() && transient_retries > 0 => {
                    transient_retries -= 1;
                    tokio::time::sleep(MIN_TOKEN_RETRY_DELAY).await;
                }
                Err(e) => return Err(CatalogError::Store(e)),
            }
        }
    }

    /// Token fallback when the exact commit-record GET returned NotFound
    /// after its retry (docs/catalog-and-mvcc.md step 5). The token fully
    /// determines its (shard, ingest_hour) bucket, but not the
    /// `input_set_hash16` a compaction record's key embeds, so this LISTs the
    /// bucket to discover its compaction records and tombstone:
    ///
    /// - Tombstone present: satisfied with zero segments (the data was
    ///   retired, not lost).
    /// - The token's writer identity found in a compaction record's input
    ///   list: satisfied via that record's parts (all of them; the token's
    ///   data is somewhere among them, and read-your-write ignores event
    ///   range just as the L0 exact-token path does).
    /// - Neither: `unsatisfiable token`, unchanged.
    async fn resolve_min_token_fallback(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        token: &CommitToken,
        out: &mut HashMap<String, SegmentRef>,
        origin_by_key: &mut HashMap<String, SegmentOrigin>,
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let prefix =
            keys::commit_shard_hour_prefix(tenant, signal, token.shard, token.ingest_hour_bucket)?;
        let objects = self.guarded_list_all(tenant, &prefix, accounting).await?;
        let mut compaction_keys: Vec<String> = Vec::new();
        let mut rewrite_keys: Vec<String> = Vec::new();
        for meta in objects {
            match keys::partition_bucket_entry(&meta.key)? {
                BucketEntry::Tombstone(_) => {
                    // Satisfied with zero segments (ADR-0019 decision 3), and
                    // observing the tombstone invalidates this bucket's cached
                    // records (ADR-0010 §10), same as the listing path.
                    self.invalidate_bucket_cache(tenant, &prefix);
                    return Ok(());
                }
                BucketEntry::CompactionRecord(_) => compaction_keys.push(meta.key),
                BucketEntry::RewriteRecord(_) => rewrite_keys.push(meta.key),
                BucketEntry::CommitRecord(_) => {}
            }
        }

        let token_identity = (token.writer_id.to_string(), token.epoch, token.seq);

        // Load the bucket's compaction and rewrite records. A rewrite (ADR-0064
        // decision 3) supersedes inputs exactly as compaction does, so
        // read-your-write for a swept L0 whose data a rewrite absorbed must be
        // served from the rewrite's (erased) output, never from a superseded
        // predecessor's parts (which would resurrect erased records).
        let mut compaction_records: Vec<(String, Arc<CompactionRecord>)> = Vec::new();
        for ckey in &compaction_keys {
            let record = self
                .load_and_validate_compaction(tenant, signal, token.shard, ckey, accounting)
                .await?;
            compaction_records.push((ckey.clone(), record));
        }
        let mut rewrite_records: Vec<(String, Arc<RewriteRecord>)> = Vec::new();
        for rkey in &rewrite_keys {
            let record = self
                .load_and_validate_rewrite(tenant, signal, token.shard, rkey, accounting)
                .await?;
            rewrite_records.push((rkey.clone(), record));
        }

        // Which records a live rewrite superseded as a whole (their parts must
        // never be served). No rewrites -> empty set -> exactly the pre-ADR-0064
        // behavior below.
        let compaction_by_key: HashMap<&str, &CompactionRecord> = compaction_records
            .iter()
            .map(|(k, r)| (k.as_str(), r.as_ref()))
            .collect();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> = rewrite_records
            .iter()
            .map(|(k, r)| (k.as_str(), r.as_ref()))
            .collect();
        let mut superseded_records: HashSet<String> = HashSet::new();
        {
            let mut discard: HashSet<(String, u64, u64)> = HashSet::new();
            for (rkey, record) in &rewrite_records {
                resolve_rewrite_supersession(
                    rkey,
                    record,
                    &prefix,
                    &compaction_by_key,
                    &rewrite_by_key,
                    &mut discard,
                    &mut superseded_records,
                )?;
            }
        }

        // Third and last classifier that resolves rewrite supersession; it
        // raises the sibling alarm for the same reason the other two do.
        self.check_rewrite_siblings(
            token.shard,
            token.ingest_hour_bucket,
            &rewrite_records,
            &superseded_records,
        );

        // A live compaction record whose inputs cover the token: serve its parts.
        for (ckey, record) in &compaction_records {
            if superseded_records.contains(ckey) {
                continue;
            }
            let covers = record.inputs.iter().any(|input| {
                (
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ) == token_identity
            });
            if covers {
                for part in &record.parts {
                    let segment_ref = build_l1_segment_ref(record, part, ckey)?;
                    if !out.contains_key(&segment_ref.data_object_key) {
                        origin_by_key.insert(
                            segment_ref.data_object_key.clone(),
                            SegmentOrigin::TokenResolved,
                        );
                    }
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
                return Ok(());
            }
        }

        // A live rewrite record whose effective inputs (its own, or chased
        // through `superseded_record_key`) cover the token: serve its erased
        // output parts. This is the read-your-write answer after an erasure
        // rewrite absorbed the token's flush.
        for (rkey, record) in &rewrite_records {
            if superseded_records.contains(rkey) {
                continue;
            }
            let mut effective_inputs: HashSet<(String, u64, u64)> = HashSet::new();
            let mut discard_superseded: HashSet<String> = HashSet::new();
            resolve_rewrite_supersession(
                rkey,
                record,
                &prefix,
                &compaction_by_key,
                &rewrite_by_key,
                &mut effective_inputs,
                &mut discard_superseded,
            )?;
            if effective_inputs.contains(&token_identity) {
                for part in &record.parts {
                    let segment_ref = build_rewrite_l1_segment_ref(record, part, rkey)?;
                    if !out.contains_key(&segment_ref.data_object_key) {
                        origin_by_key.insert(
                            segment_ref.data_object_key.clone(),
                            SegmentOrigin::TokenResolved,
                        );
                    }
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
                return Ok(());
            }
        }

        Err(unsatisfiable_token(token))
    }

    /// `pub(crate)`: also called by `fold` to load and validate commit records found by bucket
    /// listing, reusing this cache-first GET+decode+validate path.
    pub(crate) async fn load_and_validate(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Result<Arc<CommitRecord>, CatalogError> {
        if let Some(cached) = self.cache.get(tenant, key, accounting) {
            validate_expected_fields(self, &cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        let bytes = got.data.len() as u64;
        let record = record::decode(&got.data)?;
        validate_expected_fields(self, &record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            bytes,
            self.config.cache_capacity_per_tenant,
        );
        Ok(record)
    }
}

/// Deterministic total-order key over a mixed L0/L1 segment set
/// (docs/catalog-and-mvcc.md "Cross-segment duplicate samples" and "Snapshot
/// resolution"). L0 refs keep their exact previous order: the trailing L1
/// discriminator/`input_set_hash`/`part_index` components are constant
/// (0/[0; 32]/0) for every L0 ref, so they never reorder L0-vs-L0. L1 refs
/// carry `writer_epoch`/`writer_seq` == 0 and `writer_id` == nil, so they
/// order by their record's `created_unix_ns` then, past the level tag, by
/// `input_set_hash` then `part_index`.
/// The single implicit generation 0 at `shard_count`, activation hour 0: the
/// generation history of a (tenant, signal) with no reshard, and the fallback
/// a read path uses when provisioning enforcement is off or no provisioning
/// record exists yet (ADR-0052 section 1). `scan_count` over it is
/// `shard_count` for every hour, identical to the pre-ADR-0052 `0..shard_count`
/// fan-out.
fn implicit_generation_zero(shard_count: u32) -> crate::provisioning::ShardGeneration {
    crate::provisioning::ShardGeneration {
        generation: 0,
        shard_count,
        activation_hour: 0,
        appended_unix_ns: 0,
    }
}

fn segment_sort_key(s: &SegmentRef) -> (i64, u64, u64, u32, Uuid, u8, [u8; 32], u32) {
    let (level_tag, input_set_hash, part_index) = match &s.level {
        SegmentLevel::L0 => (0u8, [0u8; 32], 0u32),
        SegmentLevel::L1 {
            input_set_hash,
            part_index,
        } => (1u8, *input_set_hash, *part_index),
    };
    (
        s.created_unix_ns,
        s.writer_epoch,
        s.writer_seq,
        s.shard,
        s.writer_id,
        level_tag,
        input_set_hash,
        part_index,
    )
}

fn unsatisfiable_token(token: &CommitToken) -> CatalogError {
    CatalogError::UnsatisfiableToken {
        shard: token.shard,
        writer_id: token.writer_id.to_string(),
        epoch: token.epoch,
        seq: token.seq,
        ingest_hour_bucket: token.ingest_hour_bucket,
    }
}

/// Validate a decoded record's tenant_hash/signal/shard against the
/// (tenant, signal, shard) it was listed or addressed under (ADR-0010 §10:
/// checked on every cache hit and every fresh decode).
///
/// A `tenant_hash` disagreement is an isolation breach (ADR-0050 §2): a
/// commit record listed or addressed under one tenant's prefix that declares
/// another tenant. It is recorded on `ravel_catalog_isolation_breach_total`
/// before the hard `FieldMismatch` is returned, so an operator sees it on the
/// same counter the HEAD and postings breaches increment. The
/// `catalog` handle is threaded in purely to reach that counter; the
/// rejection itself is unchanged.
fn validate_expected_fields(
    catalog: &Catalog,
    record: &CommitRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        catalog.record_isolation_breach();
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant.to_hex(),
            actual: format!("{:?}", record.tenant_hash),
        });
    }
    if signal::from_proto(record.signal) != Ok(signal) {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "signal",
            expected: format!("{signal:?}"),
            actual: format!("{:?}", record.signal),
        });
    }
    if record.shard != shard {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "shard",
            expected: shard.to_string(),
            actual: record.shard.to_string(),
        });
    }
    Ok(())
}

/// Validate a decoded compaction record's tenant_hash/signal/shard against
/// the (tenant, signal, shard) it was listed or addressed under, and verify
/// its own key reconstructs to the key it was found at (ADR-0010 §7). The
/// compaction-record analog of [`validate_expected_fields`], and like it a
/// `tenant_hash` disagreement is recorded on
/// `ravel_catalog_isolation_breach_total` before the hard `FieldMismatch`
/// (ADR-0050 §2). The `catalog` handle is threaded in purely to reach
/// that counter; the rejection itself is unchanged.
fn validate_compaction_expected_fields(
    catalog: &Catalog,
    record: &CompactionRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        catalog.record_isolation_breach();
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant.to_hex(),
            actual: format!("{:?}", record.tenant_hash),
        });
    }
    if signal::from_proto(record.signal) != Ok(signal) {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "signal",
            expected: format!("{signal:?}"),
            actual: format!("{:?}", record.signal),
        });
    }
    if record.shard != shard {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "shard",
            expected: shard.to_string(),
            actual: record.shard.to_string(),
        });
    }
    keys::verify_compaction_record_key(record, key).map_err(|source| {
        CatalogError::Reconstruction {
            key: key.to_string(),
            source,
        }
    })?;
    Ok(())
}

/// Validate a decoded rewrite record's tenant_hash/signal/shard against the
/// (tenant, signal, shard) it was listed under, and verify its observed key
/// reconstructs from its own identity fields (ADR-0010 §7). The rewrite-record
/// analog of [`validate_compaction_expected_fields`]; a `tenant_hash`
/// disagreement is recorded on `ravel_catalog_isolation_breach_total` before
/// the hard `FieldMismatch` (ADR-0050 §2).
fn validate_rewrite_expected_fields(
    catalog: &Catalog,
    record: &RewriteRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        catalog.record_isolation_breach();
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant.to_hex(),
            actual: format!("{:?}", record.tenant_hash),
        });
    }
    if signal::from_proto(record.signal) != Ok(signal) {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "signal",
            expected: format!("{signal:?}"),
            actual: format!("{:?}", record.signal),
        });
    }
    if record.shard != shard {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "shard",
            expected: shard.to_string(),
            actual: record.shard.to_string(),
        });
    }
    keys::verify_rewrite_record_key(record, key).map_err(|source| {
        CatalogError::Reconstruction {
            key: key.to_string(),
            source,
        }
    })?;
    Ok(())
}

/// Build an L1 [`SegmentRef`] from a compaction record and one of its parts,
/// reconstructing the part key from their identity fields (ADR-0010 §7,
/// never a stored string). `observed_ckey` names the compaction record for
/// error messages. The footer of the part object is later verified against
/// these same fields by the reader.
fn build_l1_segment_ref(
    record: &CompactionRecord,
    part: &CompactionPart,
    observed_ckey: &str,
) -> Result<SegmentRef, CatalogError> {
    let data_object_key = keys::reconstruct_l1_part_key(record, part)?;
    let content_hash: [u8; 32] =
        part.content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_ckey.to_string(),
                field: "part content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", part.content_hash.len()),
            })?;
    let input_set_hash: [u8; 32] =
        record
            .input_set_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_ckey.to_string(),
                field: "input_set_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", record.input_set_hash.len()),
            })?;
    Ok(SegmentRef {
        data_object_key,
        object_size: part.object_size,
        min_event_ts_ns: part.min_event_ts_ns,
        max_event_ts_ns: part.max_event_ts_ns,
        ingest_hour_bucket: record.ingest_hour_bucket,
        sample_count: part.sample_count,
        series_count: part.series_count,
        shard: record.shard,
        content_hash,
        // A part has no writer identity of its own: these are
        // never used for an L1 ref's identity or dedup.
        writer_id: Uuid::nil(),
        writer_epoch: 0,
        writer_seq: 0,
        created_unix_ns: record.created_unix_ns,
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: part.part_index,
        },
        segment_format_version: part.segment_format_version,
    })
}

/// Maximum length of a `superseded_record_key` chase before the resolver gives
/// up with a typed error (ADR-0064 decision 3, amended). Real chains are one
/// link per erasure batch over a bucket and never approach this; a chain this
/// long is corruption or a pathological write pattern, refused rather than
/// looped. Cycles are caught independently by a visited set, so this only
/// bounds acyclic-but-absurd depth.
const MAX_REWRITE_SUPERSESSION_DEPTH: usize = 64;

/// Build an L1-equivalent [`SegmentRef`] from a [`RewriteRecord`] and one of
/// its output parts (ADR-0064 decision 3 point 5: rewrite outputs fold into
/// the snapshot exactly as compaction parts do). The part key is reconstructed
/// from the rewrite's own identity fields (ADR-0010 §7, never a stored
/// string), keyed by the rewrite's `input_set_hash` -- which binds the applied
/// request set (ADR-0064 amendment). The ref carries `SegmentLevel::L1` with
/// that same hash, so it sorts and dedups in the mixed-level snapshot order
/// identically to a compaction part.
fn build_rewrite_l1_segment_ref(
    record: &RewriteRecord,
    part: &CompactionPart,
    observed_rkey: &str,
) -> Result<SegmentRef, CatalogError> {
    let data_object_key = keys::reconstruct_rewrite_part_key(record, part)?;
    let content_hash: [u8; 32] =
        part.content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_rkey.to_string(),
                field: "part content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", part.content_hash.len()),
            })?;
    let input_set_hash: [u8; 32] =
        record
            .input_set_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_rkey.to_string(),
                field: "input_set_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", record.input_set_hash.len()),
            })?;
    Ok(SegmentRef {
        data_object_key,
        object_size: part.object_size,
        min_event_ts_ns: part.min_event_ts_ns,
        max_event_ts_ns: part.max_event_ts_ns,
        ingest_hour_bucket: record.ingest_hour_bucket,
        sample_count: part.sample_count,
        series_count: part.series_count,
        shard: record.shard,
        content_hash,
        // A part has no writer identity of its own; never used for an L1
        // ref's identity or dedup (see `build_l1_segment_ref`).
        writer_id: Uuid::nil(),
        writer_epoch: 0,
        writer_seq: 0,
        created_unix_ns: record.created_unix_ns,
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: part.part_index,
        },
        segment_format_version: part.segment_format_version,
    })
}

/// Resolve one rewrite record's supersession into the two unified exclusion
/// sets shared with the compaction path (ADR-0064 decision 3, amended). Used
/// by both snapshot resolution (`process_bucket`) and the index fold
/// (`fold.rs`), which must derive identical bucket state.
///
/// - `excluded` gains the rewrite's *effective* L0 input identities: its own
///   `inputs` when set, or -- when `superseded_record_key` is set instead --
///   the inputs of the compaction/rewrite record it names, chased through any
///   rewrite-of-a-rewrite chain until a record with `inputs` set directly is
///   reached.
/// - `superseded_records` gains the key of every compaction/rewrite record a
///   rewrite superseded as a whole, so the caller can exclude that record's
///   output parts (overlap harmlessness does not hold across a rewrite).
///
/// The named predecessor is looked up among the bucket's already-loaded
/// records. A predecessor absent from the live listing (already swept) simply
/// ends the chase: its parts and inputs are no longer live, so there is
/// nothing further to exclude. A chain that revisits a key (cycle) or exceeds
/// [`MAX_REWRITE_SUPERSESSION_DEPTH`] is a typed error, never a hang. Both
/// `decode_rewrite` and `validate_rewrite` already guarantee each record has
/// exactly one of `inputs`/`superseded_record_key` set and that the key parses
/// and names this same bucket, so this walk trusts those invariants.
#[allow(clippy::too_many_arguments)]
/// Resolve one rewrite record's supersession chain into the two exclusion sets
/// a snapshot resolve uses (ADR-0064 decision 3, amended). `excluded` gains the
/// rewrite's effective L0 input identities (its own `inputs`, or those reached
/// by chasing `superseded_record_key`); `superseded_records` gains the keys of
/// any compaction/rewrite record a rewrite superseded as a whole, whose output
/// parts must therefore be excluded (overlap harmlessness does NOT hold across
/// a rewrite). The chase is bounded ([`MAX_REWRITE_SUPERSESSION_DEPTH`]) and
/// cycle-checked; an over-deep or cyclic chain is a typed error, never a hang.
///
/// Exposed for the erasure completion check in `ravel-maintain`: ADR-0064 §4
/// requires the rewrite pass to derive "is this
/// bucket's contribution current" through the SAME supersession logic a
/// snapshot resolve and the fold use -- not a bucket LIST resolved in
/// isolation, and not `ravel-maintain`'s own one-hop `resolve_live_record`,
/// which cannot see an L0 input the query still serves because this chain
/// failed to exclude it (the absent-predecessor / partial-input case §4 names).
/// Routing completion through this exact function is what makes a `.done`
/// impossible to write while a resolvable snapshot still serves the subject.
pub fn resolve_rewrite_supersession(
    start_key: &str,
    start_record: &RewriteRecord,
    bucket_prefix: &str,
    compaction_by_key: &HashMap<&str, &CompactionRecord>,
    rewrite_by_key: &HashMap<&str, &RewriteRecord>,
    excluded: &mut HashSet<(String, u64, u64)>,
    superseded_records: &mut HashSet<String>,
) -> Result<(), CatalogError> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut current_key = start_key.to_string();
    let mut current: &RewriteRecord = start_record;
    let mut depth = 0usize;
    loop {
        if !visited.insert(current_key.clone()) {
            return Err(CatalogError::RewriteSupersessionCycle { key: current_key });
        }
        // `>=`, not `>`: at most `MAX_REWRITE_SUPERSESSION_DEPTH` records are
        // ever processed by this loop (depth counts records already visited
        // when this check runs, before the current one), matching the
        // constant's own "maximum length of a chase" doc exactly rather than
        // allowing one extra hop past it.
        if depth >= MAX_REWRITE_SUPERSESSION_DEPTH {
            return Err(CatalogError::RewriteSupersessionChainTooDeep {
                bucket: bucket_prefix.to_string(),
                max: MAX_REWRITE_SUPERSESSION_DEPTH,
            });
        }
        depth += 1;

        // Direct-inputs case: terminal. Exclude the raw L0/L1 identities.
        if !current.inputs.is_empty() {
            for input in &current.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
            return Ok(());
        }

        // Superseded-record case: the whole named record's output is
        // superseded, so its parts must be excluded.
        let superseded_key = current.superseded_record_key.as_str();
        superseded_records.insert(superseded_key.to_string());

        // A compaction record is terminal (it always carries `inputs`): exclude
        // its L0 inputs; its parts are already excluded via `superseded_records`.
        if let Some(comp) = compaction_by_key.get(superseded_key) {
            for input in &comp.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
            return Ok(());
        }

        // A prior rewrite record: chase it (rewrite-of-a-rewrite).
        if let Some(next) = rewrite_by_key.get(superseded_key) {
            current_key = superseded_key.to_string();
            current = next;
            continue;
        }

        // Named predecessor is not live in this bucket (already swept): its
        // inputs and parts are gone, so nothing more to exclude. Stop cleanly.
        return Ok(());
    }
}

fn build_segment_ref(key: &str, record: &CommitRecord) -> Result<SegmentRef, CatalogError> {
    let data_object_key =
        keys::verify_object_key(record).map_err(|source| CatalogError::Reconstruction {
            key: key.to_string(),
            source,
        })?;
    let content_hash: [u8; 32] =
        record
            .content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: key.to_string(),
                field: "content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", record.content_hash.len()),
            })?;
    let writer_id =
        Uuid::parse_str(&record.writer_id).map_err(|_| CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "writer_id",
            expected: "uuid".to_string(),
            actual: record.writer_id.clone(),
        })?;
    Ok(SegmentRef {
        data_object_key,
        object_size: record.object_size,
        min_event_ts_ns: record.min_event_ts_ns,
        max_event_ts_ns: record.max_event_ts_ns,
        ingest_hour_bucket: record.ingest_hour_bucket,
        sample_count: record.sample_count,
        series_count: record.series_count,
        shard: record.shard,
        content_hash,
        writer_id,
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
        created_unix_ns: record.created_unix_ns,
        level: SegmentLevel::L0,
        segment_format_version: record.segment_format_version,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::InstrumentedStore;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    fn tenant() -> TenantHash {
        TenantHash([0xab; 16])
    }

    fn content_hash_for(payload: &[u8]) -> [u8; 32] {
        *blake3::hash(payload).as_bytes()
    }

    fn config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// Build, PUT the data object, and publish a fully self-consistent
    /// commit record for one segment. Each call uses a fresh writer id.
    async fn publish_segment(
        store: &MemoryStore,
        shard: u32,
        seq: u64,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
        min_event_ts_ns: i64,
        max_event_ts_ns: i64,
    ) -> CommitRecord {
        let writer_id = Uuid::new_v4();
        let payload = format!("segment-{shard}-{seq}-{writer_id}").into_bytes();
        let content_hash = content_hash_for(&payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns,
            max_event_ts_ns,
            min_ingest_ts_ns: min_event_ts_ns,
            max_ingest_ts_ns: max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    #[tokio::test]
    async fn publish_then_resolve_returns_the_segment() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let record = publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
        let seg = &snapshot.segments[0];
        assert_eq!(
            seg.data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );
        assert_eq!(seg.shard, 0);
        assert_eq!(seg.content_hash.to_vec(), record.content_hash);
    }

    /// ADR-0046 decision 4, the disk-tier half of the acceptance gate: a
    /// byte-cache hit served from the local disk tier that comes back corrupted
    /// must fall back through this crate's read funnel exactly as a corrupt
    /// store read does -- a typed degrade to full listing, never a wrong result
    /// (compare `snapshot_resolve.rs`'s `corrupt_part_falls_back_to_full_listing`
    /// and `load_one_part`'s hash-mismatch arm, the fallback precedent this
    /// mirrors rather than inventing a new one).
    ///
    /// The hit is forced to be disk-served specifically: the disk tier is seeded
    /// with the part's clean bytes while the RAM tier starts empty and in
    /// corruption mode, so the read falls through RAM into disk, and
    /// `TieredCache` corrupts the disk-served bytes at serve time. The disk
    /// tier's own hit counter proves the corrupt bytes came from disk, not from
    /// an upstream store GET.
    ///
    /// FLIP (pre-fix demonstration): in `fetch_content_addressed`, replace the
    /// `ByteCache::Tiered` arm's `tiered.get_or_fetch(...)` body with a plain
    /// `self.guarded_get(key, GetRange::Full, accounting).await?.data`, so the
    /// funnel never consults the tiered cache. The resolve then reads the clean
    /// part fresh from the store: the disk hit counter does not advance past
    /// `disk_hits_before` (that assertion fails), and the clean part decodes and
    /// is promoted into the decoded part cache (the `part_cache().is_none()`
    /// assertion fails too). Both prove the corrupt disk-served hit only reaches
    /// the funnel when it routes through the tiered handle.
    #[tokio::test]
    async fn corrupt_disk_backed_byte_cache_hit_falls_back_typed() {
        use ravel_cache::DiskCache;

        let store = Arc::new(MemoryStore::new());

        // A sealed ingest hour so `fold` produces a real, content-addressed
        // snapshot part (mirrors `fold.rs`/`cache.rs`'s own seal-time math).
        let hour = 424_242u32;
        let fold_margin = crate::DEFAULT_MAX_FLUSH_LIFETIME_NS
            + crate::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            + crate::DEFAULT_FOLD_SAFETY_MARGIN_NS;
        let now_ns = (i64::from(hour) + 1) * NS_PER_HOUR + fold_margin;
        let created = now_ns - NS_PER_HOUR;
        let record = publish_segment(&store, 0, 1, hour, created, created - 1_000, created).await;

        let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        fold_catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        // The one folded part's key and its real, clean bytes: the exact object
        // the resolve path fetches through `fetch_content_addressed`.
        let part_key = ravel_object_store::list_all(store.as_ref(), "t/")
            .await
            .expect("list")
            .into_iter()
            .map(|meta| meta.key)
            .find(|key| key.contains("/snap/"))
            .expect("fold produced a snapshot part");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let content_hash = *blake3::hash(&part_bytes).as_bytes();
        let cache_key = CacheKey::new(tenant().0, content_hash, 0, part_bytes.len() as u64);

        // Disk tier: seeded with the CLEAN part bytes. RAM tier: empty and in
        // corruption mode, so any hit it serves -- including a disk hit read
        // through it -- comes back corrupted (ADR-0046 decision 4). The read
        // must fall through the empty RAM tier into disk to be served, so the
        // hit is disk-served by construction.
        let limits = CacheLimits::new(64 << 20, 10_000, 16 << 20);
        let disk_dir =
            std::env::temp_dir().join(format!("ravel-catalog-bytecache-{}", Uuid::new_v4()));
        let disk = DiskCache::new(disk_dir.clone(), limits);
        disk.insert(cache_key, part_bytes.as_ref());
        assert!(
            disk.get(&cache_key).is_some(),
            "precondition: the part lives on the disk tier"
        );
        let ram: Cache<Arc<StoreError>> = Cache::with_corruption(limits);
        let tiered = TieredCache::new(ram, disk);
        let disk_metrics = tiered.disk_metrics();

        // A fresh catalog with an empty decoded PartCache, so `load_one_part`
        // must consult the byte cache; the disk-backed tiered handle is the byte
        // cache. Production attaches its disk tier the same way in #97.
        let mut catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        catalog.set_tiered_byte_cache_for_test(tiered);

        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };
        // Baseline the disk hit counter AFTER the precondition `get` above (which
        // already recorded one hit), so the assertion below proves the resolve
        // itself drew from disk, not that any read ever did.
        let disk_hits_before = disk_metrics.snapshot().hits;
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("resolve degrades to listing rather than failing or returning wrong data");

        assert_eq!(
            snapshot.segments.len(),
            1,
            "listing fallback must still find the one published segment"
        );
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("data key"),
            "the fallback resolves the real segment, not the corrupted part's contents"
        );
        assert!(
            disk_metrics.snapshot().hits > disk_hits_before,
            "the resolve's corrupt bytes must have been served from the disk tier through the \
             funnel, not fetched fresh from the store"
        );
        assert!(
            catalog
                .part_cache()
                .get(&tenant(), &part_key, &QueryAccounting::new())
                .is_none(),
            "corrupted bytes must never be promoted into the decoded part cache"
        );

        drop(catalog);
        let _ = std::fs::remove_dir_all(&disk_dir);
    }

    fn synthetic_head(tenant_hash: TenantHash) -> ravel_proto::catalog::v1::SnapshotHead {
        ravel_proto::catalog::v1::SnapshotHead {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as u32,
            shard_count: 1,
            watermark_hour: 1,
            parts: vec![],
            postings: None,
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            shard_generation_count: 1,
            column_stats: None,
            column_stats_part: None,
        }
    }

    /// ADR-0069 decision 2: a tenant idle past the TTL has its
    /// per-tenant catalog cache outer-map entry evicted, the evicted state is
    /// re-derived on the next resolve, and an active tenant's cache entry
    /// survives the same sweep. Deterministic via the injected `now_ns`.
    #[tokio::test]
    async fn idle_tenant_catalog_cache_evicted_and_rederived() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let idle = tenant();
        let active = TenantHash([0x11; 16]);
        let ttl_ns = 100 * NS_PER_HOUR;

        // Publish and resolve a real segment for the idle tenant at t0: the
        // resolve stamps its last-touch and its listing populates the decoded
        // record cache.
        let t0 = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let record = publish_segment(&store, 0, 1, 500_000, t0, t0 - 1_000, t0).await;
        let range = TimeRange {
            start_ns: t0 - 1_000,
            end_ns: t0,
        };
        let cold = catalog
            .resolve(&idle, Signal::Metrics, range, &[], t0)
            .await
            .expect("cold resolve");
        assert_eq!(cold.segments.len(), 1);

        // Seed the decoded-HEAD cache directly for both tenants so each has an
        // observable per-tenant outer-map entry to assert on.
        let acc = QueryAccounting::new();
        catalog.head_cache().insert(
            idle,
            Signal::Metrics,
            Arc::new(synthetic_head(idle)),
            1,
            t0,
            8,
        );
        catalog.head_cache().insert(
            active,
            Signal::Metrics,
            Arc::new(synthetic_head(active)),
            1,
            t0,
            8,
        );

        // The active tenant resolves again right before the sweep, so its
        // last-touch is recent while the idle tenant's is still t0.
        let sweep_ns = t0 + ttl_ns + 1;
        catalog
            .resolve(&active, Signal::Metrics, range, &[], sweep_ns)
            .await
            .expect("active resolve at sweep");

        // Sweep: exactly the idle tenant is evicted.
        let evicted = catalog.evict_idle_tenants(sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "only the idle tenant is evicted");

        // The active tenant's HEAD cache entry survives; the idle tenant's is
        // gone (its whole outer-map entry was dropped).
        assert!(
            catalog
                .head_cache()
                .get(&active, Signal::Metrics, sweep_ns, i64::MAX, &acc)
                .is_some(),
            "the active tenant's cache entry survives the sweep"
        );
        assert!(
            catalog
                .head_cache()
                .get(&idle, Signal::Metrics, sweep_ns, i64::MAX, &acc)
                .is_none(),
            "the idle tenant's cache entry is evicted"
        );

        // Re-derivation: a resolve for the evicted tenant rebuilds its cache
        // from the store and returns the same segment, byte-identical.
        let warm = catalog
            .resolve(&idle, Signal::Metrics, range, &[], sweep_ns)
            .await
            .expect("re-resolve after eviction re-derives from the store");
        assert_eq!(warm.segments.len(), 1);
        assert_eq!(
            warm.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );
    }

    /// A bare `RewriteRecord` naming a `superseded_record_key`, for exercising
    /// `resolve_rewrite_supersession`'s chase guards directly. Field values
    /// other than `superseded_record_key`/`inputs` are irrelevant to the walk,
    /// which trusts the decode-time invariants rather than re-validating.
    fn bare_superseding_rewrite(superseded_key: &str) -> RewriteRecord {
        RewriteRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: 0,
            ingest_hour_bucket: 0,
            inputs: Vec::new(),
            input_set_hash: vec![0u8; 32],
            parts: Vec::new(),
            drops: Vec::new(),
            created_unix_ns: 0,
            superseded_record_key: superseded_key.to_string(),
        }
    }

    #[test]
    fn rewrite_supersession_cycle_is_a_typed_error() {
        // Two rewrite records naming each other: the chase detects the revisit
        // and returns a typed error rather than looping forever. (Honest hash
        // derivation makes a real cycle unconstructable, so this guards the
        // resolver against tampered/corrupt records directly.)
        let k1 = "t/aa/m/c/0000/20260101T00/rw.1111111111111111.cmt".to_string();
        let k2 = "t/aa/m/c/0000/20260101T00/rw.2222222222222222.cmt".to_string();
        let r1 = bare_superseding_rewrite(&k2);
        let r2 = bare_superseding_rewrite(&k1);
        let compaction_by_key: HashMap<&str, &CompactionRecord> = HashMap::new();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> =
            [(k1.as_str(), &r1), (k2.as_str(), &r2)]
                .into_iter()
                .collect();
        let mut excluded = HashSet::new();
        let mut superseded = HashSet::new();
        let err = resolve_rewrite_supersession(
            &k1,
            &r1,
            "bucket",
            &compaction_by_key,
            &rewrite_by_key,
            &mut excluded,
            &mut superseded,
        )
        .expect_err("a supersession cycle must be a typed error");
        assert!(matches!(err, CatalogError::RewriteSupersessionCycle { .. }));
    }

    #[test]
    fn rewrite_supersession_over_deep_chain_is_a_typed_error() {
        // A chain of distinct rewrite records longer than the depth bound is
        // refused rather than walked unboundedly.
        let count = MAX_REWRITE_SUPERSESSION_DEPTH + 5;
        let keys: Vec<String> = (0..=count)
            .map(|i| format!("t/aa/m/c/0000/20260101T00/rw.{i:016x}.cmt"))
            .collect();
        // r[i] supersedes r[i+1]; the last points past the end (absent), but the
        // depth bound trips long before the chase reaches it.
        let records: Vec<RewriteRecord> = (0..count)
            .map(|i| bare_superseding_rewrite(&keys[i + 1]))
            .collect();
        let compaction_by_key: HashMap<&str, &CompactionRecord> = HashMap::new();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> = records
            .iter()
            .enumerate()
            .map(|(i, r)| (keys[i].as_str(), r))
            .collect();
        let mut excluded = HashSet::new();
        let mut superseded = HashSet::new();
        let err = resolve_rewrite_supersession(
            &keys[0],
            &records[0],
            "bucket",
            &compaction_by_key,
            &rewrite_by_key,
            &mut excluded,
            &mut superseded,
        )
        .expect_err("an over-deep supersession chain must be a typed error");
        assert!(matches!(
            err,
            CatalogError::RewriteSupersessionChainTooDeep { .. }
        ));
    }

    /// Pin the depth boundary exactly, not just "deep enough fails": a chain of
    /// `MAX_REWRITE_SUPERSESSION_DEPTH` records is walked to its end, and one
    /// more record is refused. Off by one in either direction would either
    /// reject a legal chain or walk one hop past the documented maximum.
    #[test]
    fn rewrite_supersession_depth_bound_is_exact_at_the_maximum() {
        // A chain of `n` records where r[i] supersedes r[i+1] and the last
        // names an absent predecessor (so the walk terminates cleanly if the
        // depth bound lets it).
        fn chase(n: usize) -> Result<(), CatalogError> {
            let keys: Vec<String> = (0..=n)
                .map(|i| format!("t/aa/m/c/0000/20260101T00/rw.{i:016x}.cmt"))
                .collect();
            let records: Vec<RewriteRecord> = (0..n)
                .map(|i| bare_superseding_rewrite(&keys[i + 1]))
                .collect();
            let compaction_by_key: HashMap<&str, &CompactionRecord> = HashMap::new();
            let rewrite_by_key: HashMap<&str, &RewriteRecord> = records
                .iter()
                .enumerate()
                .map(|(i, r)| (keys[i].as_str(), r))
                .collect();
            let mut excluded = HashSet::new();
            let mut superseded = HashSet::new();
            resolve_rewrite_supersession(
                &keys[0],
                &records[0],
                "bucket",
                &compaction_by_key,
                &rewrite_by_key,
                &mut excluded,
                &mut superseded,
            )
        }

        chase(MAX_REWRITE_SUPERSESSION_DEPTH)
            .expect("a chain of exactly the maximum length must be walked, not refused");
        let err = chase(MAX_REWRITE_SUPERSESSION_DEPTH + 1)
            .expect_err("one record past the maximum must be refused");
        assert!(matches!(
            err,
            CatalogError::RewriteSupersessionChainTooDeep { .. }
        ));
    }

    async fn seed_provisioning_record(store: &MemoryStore, shard_count: u32) {
        use ravel_proto::sys::v1 as sysproto;
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count,
            created_unix_ns: 1,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        store
            .put(
                &crate::provisioning::provisioning_key(&tenant(), Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed provisioning record");
    }

    /// With provisioning enforcement on (as `build_catalog` sets it), a resolve
    /// for a (tenant, signal) whose record disagrees with the configured
    /// shard_count fails with a typed error before the `0..shard_count` listing
    /// loop, so a lower shard_count never serves a truncated shard range
    /// (ADR-0050 section 5, S1-E6 query-path guard).
    #[tokio::test]
    async fn resolve_enforces_provisioning_record_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;
        // The tenant's data was written under shard_count=4.
        seed_provisioning_record(&store, 4).await;

        // A catalog configured for 2 shards, with enforcement on.
        let catalog = Catalog::new(store.clone(), config(2))
            .expect("catalog")
            .with_provisioning_enforcement();
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("a lower configured shard_count must fail the resolve");
        assert!(
            matches!(err, CatalogError::Provisioning(_)),
            "expected a provisioning failure, got: {err}"
        );
    }

    /// Enforcement is opt-in: a catalog built without
    /// `with_provisioning_enforcement` (as every existing direct-construction
    /// caller does) resolves normally even when a disagreeing record exists, so
    /// this change does not alter behavior for those callers.
    #[tokio::test]
    async fn resolve_without_enforcement_ignores_provisioning_record() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;
        seed_provisioning_record(&store, 4).await;

        let catalog = Catalog::new(store.clone(), config(2)).expect("catalog");
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("enforcement off: resolve ignores the provisioning record");
    }

    /// Issue #729: with provisioning enforcement on, the generation-history
    /// read every resolve performs must be an accounted GET through
    /// `guarded_get`, not a raw `store.get`. Both catalogs are warmed with one
    /// resolve (populating the record/HEAD caches, and memoizing the enforced
    /// catalog's one-shot `shard_count` check), then a second resolve is
    /// measured on each so the two run under identical cache warmth. The only
    /// GET that then differs is the always-fresh generation read: enforcement
    /// off synthesizes generation 0 with no store read, enforcement on reads
    /// the record, so the enforced resolve issues exactly one more GET.
    ///
    /// FLIP (pre-fix demonstration): in `Catalog::read_scan_generations`,
    /// replace `read_generations_accounted(&getter, tenant, signal)` with the
    /// raw `read_generations_from_store(self.store.as_ref(), tenant, signal)`.
    /// The generation GET then bypasses `guarded_get`, `on` stays equal to
    /// `off`, and the `on == off + 1` assertion fails.
    #[tokio::test]
    async fn provisioning_generation_read_is_accounted_get() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;
        // A record whose shard_count matches the configured value, so the
        // resolve's scan set (and therefore every non-provisioning GET) is
        // identical with enforcement on and off; the only difference is the
        // provisioning read this change accounts for.
        seed_provisioning_record(&store, 1).await;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        // Measure the second resolve of a catalog, after a warm-up resolve, so
        // the record/HEAD caches are populated identically for both variants.
        async fn measured_gets(catalog: &Catalog, range: TimeRange, now: i64) -> u64 {
            catalog
                .resolve(&tenant(), Signal::Metrics, range, &[], now)
                .await
                .expect("warm-up resolve");
            let acc = QueryAccounting::new();
            catalog
                .resolve_with_accounting(&tenant(), Signal::Metrics, range, &[], now, &acc)
                .await
                .expect("measured resolve");
            acc.snapshot().s3_requests(AccountedOp::Get)
        }

        let off_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let off = measured_gets(&off_catalog, range, now).await;

        let on_catalog = Catalog::new(store.clone(), config(1))
            .expect("catalog")
            .with_provisioning_enforcement();
        let on = measured_gets(&on_catalog, range, now).await;

        assert_eq!(
            off, 1,
            "warm resolve GET count with enforcement off: only the uncached HEAD read"
        );
        assert_eq!(
            on,
            off + 1,
            "enforcement on adds exactly the generation-history GET, routed through guarded_get"
        );
    }

    /// Issue #729: the resolve-path provisioning read is bounded by the resolve
    /// request semaphore because it funnels through `guarded_get`, which holds a
    /// permit around the store call. A FaultStore hold on the
    /// provisioning-record GET blocks the resolve while that permit is held; a
    /// raw `store.get` would issue the same GET but acquire no permit.
    ///
    /// FLIP (pre-fix demonstration): revert `enforce_provisioning_once` to the
    /// raw `validate_or_adopt(self.store.as_ref(), .., CheckOnly)` call (and
    /// `read_scan_generations` to `read_generations_from_store`). The held
    /// provisioning GET then holds no resolve permit, so `available_permits()`
    /// stays at `MAX_CONCURRENT_REQUESTS` and the `== MAX_CONCURRENT_REQUESTS -
    /// 1` assertion fails.
    #[tokio::test]
    async fn provisioning_read_holds_a_resolve_semaphore_permit() {
        let mem = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&mem, 0, 1, 500_000, now, now - 1_000, now).await;
        seed_provisioning_record(&mem, 1).await;
        let store = Arc::new(FaultStore::new(mem, FaultPlan::empty()));

        // Hold every provisioning-record GET inside the store until released.
        // On a fresh enforcement-on catalog the first such GET is the
        // `shard_count` enforcement check (`enforce_provisioning_once`), which
        // runs before the listing fan-out.
        let gate = store.hold(Op::Get, Some("/prov".to_string()), Occurrence::Always);

        let catalog = Arc::new(
            Catalog::new(store.clone(), config(1))
                .expect("catalog")
                .with_provisioning_enforcement(),
        );
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        assert_eq!(
            catalog.request_semaphore.available_permits(),
            MAX_CONCURRENT_REQUESTS,
            "no permit is held before the resolve starts"
        );

        let acc = QueryAccounting::new();
        let acc_task = acc.clone();
        let cat_task = catalog.clone();
        let task = tokio::spawn(async move {
            cat_task
                .resolve_with_accounting(&tenant(), Signal::Metrics, range, &[], now, &acc_task)
                .await
        });

        gate.wait_until_held(1).await;
        assert_eq!(
            gate.held_count(),
            1,
            "the provisioning-record GET is in flight, blocked inside the store"
        );
        assert_eq!(
            catalog.request_semaphore.available_permits(),
            MAX_CONCURRENT_REQUESTS - 1,
            "the held provisioning GET holds a resolve semaphore permit: it routed through \
             guarded_get, not a raw store.get"
        );

        // Release every held provisioning GET (the enforcement check, then the
        // generation-history read) so the resolve runs to completion. Bounded so
        // a resolve that blocks on something other than this gate fails the test
        // instead of hanging the suite.
        let mut spins = 0;
        while !task.is_finished() {
            for id in gate.held() {
                gate.release(id);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            spins += 1;
            assert!(
                spins < 10_000,
                "resolve did not finish after releasing every held provisioning GET"
            );
        }
        let snapshot = task.await.expect("join resolve task").expect("resolve");
        assert_eq!(
            snapshot.segments.len(),
            1,
            "once unblocked the resolve returns the published segment"
        );
    }

    /// End-to-end: an older HEAD (`shard_generation_count` lower
    /// than the reader's history) whose own watermark reaches into hours a
    /// newer, wider generation was already active for must NOT be served
    /// silently. The reader forces one record re-read and, finding the head
    /// still inconsistent, fails closed with a loud `shard_count` mismatch --
    /// never a completed query over the narrower range (which would silently
    /// omit data that landed in the widened shards within the head's watermark).
    #[tokio::test]
    async fn resolve_rejects_older_head_reaching_unknown_generation_hours() {
        let store = Arc::new(MemoryStore::new());
        // gen0 count 4 @ hour 0, then an increase to count 8 @ hour 5.
        crate::provisioning::validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            0,
            crate::provisioning::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        crate::provisioning::append_generation(store.as_ref(), &tenant(), Signal::Metrics, 8, 5, 0)
            .await
            .expect("append generation 1");

        // A HEAD that knew only generation 0 (shard_generation_count 1,
        // shard_count 4) at watermark 10 -- well past gen1's activation at 5.
        let head = ravel_proto::catalog::v1::SnapshotHead {
            format_version: crate::snapshot_format::HEAD_FORMAT_VERSION,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as u32,
            shard_count: 4,
            watermark_hour: 10,
            parts: vec![ravel_proto::catalog::v1::SnapshotPartRef {
                key: "unused-part-never-loaded".to_string(),
                blake3: vec![0u8; 32],
                size: 1,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
            column_stats: None,
            column_stats_part: None,
        };
        let head_bytes = crate::snapshot_format::encode_head(&head).expect("encode head");
        store
            .put(
                &crate::fold::head_object_key(&tenant(), Signal::Metrics),
                Bytes::from(head_bytes),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        let catalog = Catalog::new(store.clone(), config(4))
            .expect("catalog")
            .with_provisioning_enforcement();
        let now = 12 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: 0,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err(
                "an older head reaching into an unknown generation's active hours must fail closed",
            );
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "shard_count"),
            other => panic!("expected a shard_count FieldMismatch, got {other:?}"),
        }
    }

    /// End-to-end: when head validation performs a one-shot record
    /// re-read, the fresher generation history it validated against must drive
    /// the Phase 1 listing scan set, not the stale view the resolve first read.
    /// A `NotFoundBlip` on the resolve's own generation read makes its initial
    /// view the single implicit generation 0 (count 4); the HEAD (folded knowing
    /// the reshard, `shard_generation_count` 2) forces a re-read that recovers
    /// the wide view (count 8). The query must then scan shard 5 -- in the
    /// widened range, at an hour past the watermark -- and return the segment
    /// there, not miss it by listing only `0..4`.
    #[tokio::test]
    async fn resolve_reread_widens_listing_scan_set() {
        let inner = MemoryStore::new();
        // gen0 count 4 @ 0, increase to count 8 @ hour 10.
        crate::provisioning::validate_or_adopt(
            &inner,
            &tenant(),
            Signal::Metrics,
            4,
            0,
            crate::provisioning::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        crate::provisioning::append_generation(&inner, &tenant(), Signal::Metrics, 8, 10, 0)
            .await
            .expect("append generation 1");

        // Data in shard 5 at hour 11 -- reachable only under the widened count 8.
        let hour = 11u32;
        let event_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
        let record = publish_segment(&inner, 5, 1, hour, event_ts, event_ts, event_ts).await;
        let data_key = keys::reconstruct_data_key(&record).expect("data key");

        // A valid, empty snapshot part at watermark 10, and a HEAD that knows
        // both generations (shard_generation_count 2, fan-out ceiling 8).
        let signal_num = signal::to_proto(Signal::Metrics) as u32;
        let part_bytes = crate::snapshot_format::encode_part(tenant().0, signal_num, 8, 10, &[])
            .expect("encode part");
        let part_hash = *blake3::hash(&part_bytes).as_bytes();
        let part_key = format!("t/{}/catalog/m/snap/empty.csnap", tenant().to_hex());
        inner
            .put(
                &part_key,
                Bytes::from(part_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put part");
        let head = ravel_proto::catalog::v1::SnapshotHead {
            format_version: crate::snapshot_format::HEAD_FORMAT_VERSION,
            tenant_hash: tenant().0.to_vec(),
            signal: signal_num,
            shard_count: 8,
            watermark_hour: 10,
            parts: vec![ravel_proto::catalog::v1::SnapshotPartRef {
                key: part_key,
                blake3: part_hash.to_vec(),
                size: part_bytes.len() as u64,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 2,
            column_stats: None,
            column_stats_part: None,
        };
        let head_bytes = crate::snapshot_format::encode_head(&head).expect("encode head");
        inner
            .put(
                &crate::fold::head_object_key(&tenant(), Signal::Metrics),
                Bytes::from(head_bytes),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        // Blip the SECOND `/prov` GET (the resolve's own scan-generations read):
        // GET #1 is enforcement's validate, #2 is scan-generations (blipped to
        // NotFound -> the implicit gen0 view), #3 is the head-validation re-read
        // (recovers the wide record).
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("/prov")
                .with_occurrence(ravel_object_store::fault::Occurrence::Nth(2)),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(4))
            .expect("catalog")
            .with_provisioning_enforcement();

        let now = 12 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: 0,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve must succeed once the re-read recovers the wide view");

        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            1,
            "the blip must have fired, exercising the stale-then-re-read path"
        );
        assert!(
            snapshot
                .segments
                .iter()
                .any(|s| s.data_object_key == data_key),
            "the shard-5 segment (in the widened range, past the watermark) must be \
             returned: the re-read's wide generation view must drive the listing scan set"
        );
    }

    /// Regression: a `FreshNoData` result (no record yet) must not be
    /// cached as validated. A query-only catalog resolves an empty-record tenant
    /// (passes as fresh), then a real record appears written under a higher
    /// shard_count; the next resolve must re-check and surface the mismatch, not
    /// serve a truncated shard range from a stale "already validated" cache
    /// entry (records are immutable, so a wrongly-cached miss never re-checks).
    #[tokio::test]
    async fn resolve_rechecks_after_fresh_no_data_until_record_appears() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;

        // Catalog configured for 2 shards, enforcement on. No provisioning
        // record exists yet: the first resolve sees `FreshNoData` and must pass.
        let catalog = Catalog::new(store.clone(), config(2))
            .expect("catalog")
            .with_provisioning_enforcement();
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("a fresh (no-record) tenant resolves cleanly");
        assert_eq!(snapshot.segments.len(), 1, "first resolve returns the data");

        // A separate higher-shard_count process now writes the real record and
        // (conceptually) lands data across shards 0..4. If the earlier
        // `FreshNoData` had been cached as validated, this resolve would skip the
        // check and silently serve only shards 0..2.
        seed_provisioning_record(&store, 4).await;

        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("once the real record appears the resolve must re-check and refuse");
        assert!(
            matches!(err, CatalogError::Provisioning(_)),
            "expected a provisioning failure from the re-check, got: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_records_list_and_get_requests_into_accounting() {
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let writer_id = Uuid::new_v4();
        let payload = format!("segment-0-1-{writer_id}").into_bytes();
        let content_hash = content_hash_for(&payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 1_000,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 1_000,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        // A separate, uninstrumented catalog over identical data gives the
        // no-accounting baseline: accounting must be pure observation, never
        // a behavior change, so its resolved snapshot must match exactly.
        let plain_store = Arc::new(MemoryStore::new());
        publish::put_data_object(
            plain_store.as_ref(),
            &data_key,
            Bytes::from(payload.clone()),
        )
        .await
        .expect("put data object");
        publish::publish(plain_store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("publish");
        let plain_catalog = Catalog::new(plain_store, config(1)).expect("catalog");
        let baseline = plain_catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("baseline resolve");

        // A fresh catalog and a fresh instrumented backend, seeded with the
        // exact same commit record and data object, isolates the resolve
        // under test from the baseline catalog's own caches.
        let instrumented_inner = MemoryStore::new();
        publish::put_data_object(&instrumented_inner, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(&instrumented_inner, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        let store = Arc::new(InstrumentedStore::new(instrumented_inner));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let before = store.metrics().snapshot();
        let accounting = QueryAccounting::new();
        let snapshot = catalog
            .resolve_with_accounting(&tenant(), Signal::Metrics, range, &[], now, &accounting)
            .await
            .expect("accounted resolve");
        let after = store.metrics().snapshot();

        assert_eq!(snapshot.segments, baseline.segments);
        assert_eq!(snapshot.segments_pruned, baseline.segments_pruned);

        let acc = accounting.snapshot();
        let get_calls_diff = after.get.calls - before.get.calls;
        let list_calls_diff = after.list.calls - before.list.calls;
        let get_bytes_diff = after.get.bytes - before.get.bytes;
        let head_calls_diff = after.head.calls - before.head.calls;
        let list_delimited_calls_diff = after.list_delimited.calls - before.list_delimited.calls;
        let delete_calls_diff = after.delete.calls - before.delete.calls;

        // Catalog's two funnels only ever issue GET and LIST: no HEAD,
        // list_delimited, or DELETE call is on the resolve path.
        assert_eq!(head_calls_diff, 0);
        assert_eq!(list_delimited_calls_diff, 0);
        assert_eq!(delete_calls_diff, 0);

        assert_eq!(acc.s3_requests(AccountedOp::Get), get_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::List), list_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::Head), 0);
        assert_eq!(acc.s3_bytes(AccountedOp::Get), get_bytes_diff);
        assert_eq!(acc.s3_bytes(AccountedOp::List), 0);
        assert_eq!(
            acc.total_s3_requests(),
            get_calls_diff + list_calls_diff + head_calls_diff
        );
        assert_eq!(acc.total_s3_bytes(), get_bytes_diff);

        // At least one LIST (the hour bucket) and one GET (the commit
        // record, or the head probe) actually happened -- otherwise the
        // assertions above would be vacuously true.
        assert!(list_calls_diff >= 1);
        assert!(get_calls_diff >= 1);
    }

    #[tokio::test]
    async fn data_object_without_commit_record_is_never_visible() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let content_hash = content_hash_for(b"orphan");
        let key = keys::data_key(
            &tenant(),
            Signal::Metrics,
            0,
            writer_id,
            1,
            1,
            &content_hash,
        )
        .expect("data key");
        publish::put_data_object(store.as_ref(), &key, Bytes::from_static(b"orphan"))
            .await
            .expect("put data object");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert!(snapshot.segments.is_empty());
    }

    #[tokio::test]
    async fn duplicate_publish_same_content_hash_is_idempotent_single_segment() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let payload = b"identical-bytes";
        let content_hash = content_hash_for(payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store.as_ref(), &data_key, Bytes::from_static(payload))
            .await
            .expect("put data object");
        let token1 = publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("first publish");
        let token2 = publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("retry publish is idempotent");
        assert_eq!(token1, token2);

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
    }

    #[tokio::test]
    async fn different_content_hash_same_identity_is_split_brain() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let base = NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 7,
            object_size: 5,
            content_hash: content_hash_for(b"a"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        };
        let record_a = record::build(base.clone()).expect("valid record a");
        publish::publish(store.as_ref(), &record_a, &RetryPolicy::default())
            .await
            .expect("first publish");

        let mut record_b = record_a.clone();
        record_b.content_hash = content_hash_for(b"b").to_vec();
        let err = publish::publish(store.as_ref(), &record_b, &RetryPolicy::default())
            .await
            .expect_err("must be split-brain");
        assert!(matches!(err, publish::PublishError::SplitBrain { .. }));
    }

    #[tokio::test]
    async fn snapshot_is_stable_after_further_publishes() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let record_1 = publish_segment(&store, 0, 1, 500_000, now, now - 500, now).await;
        let first = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve 1");
        assert_eq!(first.segments.len(), 1);
        let first_key = first.segments[0].data_object_key.clone();
        assert_eq!(
            first_key,
            keys::reconstruct_data_key(&record_1).expect("key")
        );

        publish_segment(&store, 0, 2, 500_000, now, now - 500, now).await;
        let second = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve 2");
        assert_eq!(second.segments.len(), 2);

        // The snapshot returned earlier still names exactly the one segment
        // it was resolved with, unaffected by the later publish (MVCC: a
        // pinned snapshot, not a live view).
        assert_eq!(first.segments.len(), 1);
        assert_eq!(first.segments[0].data_object_key, first_key);
    }

    #[tokio::test]
    async fn event_time_filtering_excludes_out_of_range_segments() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let in_range = publish_segment(&store, 0, 1, 500_000, now, now - 100, now - 50).await;
        // Same ingest hour (so it IS listed), but its event range does not
        // overlap the query range.
        let _out_of_range = publish_segment(
            &store,
            0,
            2,
            500_000,
            now,
            now - 10_000_000_000,
            now - 9_000_000_000,
        )
        .await;

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&in_range).expect("key")
        );
    }

    /// ADR-0010 §2 scenario: a writer with a skewed clock pins an
    /// ingest_hour_bucket well outside the `now`-anchored listing window.
    /// Its commit token still resolves via an exact GET.
    #[tokio::test]
    async fn min_token_resolves_even_when_its_hour_bucket_is_outside_the_listing_window() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // Listing window (defaults: 2h lag, 5m skew) covers roughly hours
        // [499_997, 500_000]; this bucket is far outside it.
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &store,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");

        let without_token = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve without token");
        assert!(
            without_token.segments.is_empty(),
            "must be outside the listing window"
        );

        let with_token = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve with token");
        assert_eq!(with_token.segments.len(), 1);
        assert_eq!(
            with_token.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );
    }

    #[tokio::test]
    async fn min_token_unsatisfiable_when_commit_record_is_missing() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let token = CommitToken {
            shard: 0,
            writer_id: Uuid::new_v4(),
            epoch: 1,
            seq: 1,
            ingest_hour_bucket: 500_000,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[token], now)
            .await
            .expect_err("unsatisfiable");
        assert!(matches!(err, CatalogError::UnsatisfiableToken { .. }));
    }

    #[tokio::test]
    async fn object_key_mismatch_is_fatal_during_resolve() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let mut record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 10,
            content_hash: content_hash_for(b"x"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        // object_key is informational; nothing rejects a record whose field
        // does not match its own reconstructed key at publish time (ADR-0010
        // §7: only readers verify it).
        record.object_key = "t/deliberately/wrong/key".to_string();
        publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("publish corrupted record");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("fatal object_key mismatch");
        assert!(matches!(err, CatalogError::Reconstruction { .. }));
    }

    #[tokio::test]
    async fn shard_field_mismatch_against_listing_path_is_detected() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(2)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 1,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 10,
            content_hash: content_hash_for(b"z"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        // Self-consistent record (shard=1), placed under shard 0's commit
        // path: a cross-wiring bug the per-hit/decode field check must catch
        // (ADR-0010 §10), independent of the object_key check.
        let wrong_shard_key = keys::commit_key(
            &tenant(),
            Signal::Metrics,
            0,
            500_000,
            writer_id,
            record.writer_epoch,
            record.writer_seq,
        )
        .expect("key");
        store
            .put(
                &wrong_shard_key,
                record::encode(&record),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("field mismatch");
        assert!(matches!(
            err,
            CatalogError::FieldMismatch { field: "shard", .. }
        ));
    }

    #[tokio::test]
    async fn pagination_is_exercised_and_all_pages_are_returned() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        for seq in 1..=5u64 {
            publish_segment(&store, 0, seq, 500_000, now, now - 100, now).await;
        }
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 5);
    }

    #[tokio::test]
    async fn listing_window_boundary_hours_are_inclusive() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 1,
                max_ingest_lag_ns: NS_PER_HOUR,
                clock_skew_allowance_ns: 0,
                ..Default::default()
            },
        )
        .expect("catalog");
        let now = 500_000 * NS_PER_HOUR; // exactly on the hour
        let range = TimeRange {
            start_ns: now,
            end_ns: now,
        };
        // window = [range.start - 1h, now + 0] => hours 499_999..=500_000.
        let at_start_hour = publish_segment(&store, 0, 1, 499_999, now, now, now).await;
        let at_end_hour = publish_segment(&store, 0, 2, 500_000, now, now, now).await;
        let just_outside = publish_segment(&store, 0, 3, 499_998, now, now, now).await;

        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        let found: Vec<_> = snapshot
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect();
        assert!(found.contains(&keys::reconstruct_data_key(&at_start_hour).expect("k1")));
        assert!(found.contains(&keys::reconstruct_data_key(&at_end_hour).expect("k2")));
        assert!(!found.contains(&keys::reconstruct_data_key(&just_outside).expect("k3")));
    }

    /// The (shard, hour) listing pass and the per-bucket record
    /// GETs now run concurrently under a semaphore. Concurrency must not drop,
    /// duplicate, or reorder segments: the resolved set must be complete and
    /// its total order deterministic across repeated resolves.
    #[tokio::test]
    async fn parallel_listing_returns_complete_deterministic_snapshot() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(3)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;

        // Many segments across every shard and two in-window hours, each with
        // a distinct created stamp so the ADR-0010 §5 total order is fully
        // determined (no ties left to HashMap iteration order).
        let mut published = Vec::new();
        let mut seq = 1u64;
        for shard in 0..3u32 {
            for hour in [499_999u32, 500_000u32] {
                for _ in 0..4 {
                    let created = i64::from(hour) * NS_PER_HOUR + (seq as i64) * 1_000;
                    let record =
                        publish_segment(&store, shard, seq, hour, created, created, created + 500)
                            .await;
                    published.push(keys::reconstruct_data_key(&record).expect("key"));
                    seq += 1;
                }
            }
        }

        let range = TimeRange {
            start_ns: 499_999 * NS_PER_HOUR,
            end_ns: now,
        };
        let first = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(
            first.segments.len(),
            published.len(),
            "every published segment must be listed exactly once"
        );
        let found: HashSet<String> = first
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect();
        for key in &published {
            assert!(found.contains(key), "missing segment {key}");
        }

        // A second resolve over the same concurrent pass must return the
        // identical order, not a completion-order permutation.
        let second = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        let order1: Vec<&str> = first
            .segments
            .iter()
            .map(|s| s.data_object_key.as_str())
            .collect();
        let order2: Vec<&str> = second
            .segments
            .iter()
            .map(|s| s.data_object_key.as_str())
            .collect();
        assert_eq!(
            order1, order2,
            "concurrent listing must yield a deterministic total order"
        );
    }

    /// A fault on a commit-record GET must still surface as an
    /// error through the concurrent prewarm, never be swallowed into a
    /// silently short snapshot. A permanent GET fault fires on the record read
    /// (the absent HEAD GET degrades to listing as always), and the resolve
    /// fails loudly.
    #[tokio::test]
    async fn a_commit_record_get_fault_surfaces_through_the_concurrent_prewarm() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        for seq in 1..=5u64 {
            publish_segment(&inner, 0, seq, 500_000, now, now - 1_000, now).await;
        }
        let plan = FaultPlan::empty().with_rule(Rule::new(
            Op::Get,
            ScriptedFault::Permanent("record get down".into()),
        ));
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("a record GET fault must surface, not be swallowed");
        assert!(matches!(err, CatalogError::Store(_)), "got {err:?}");
        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the record GET fault must have actually fired"
        );
    }

    /// Regression: the exact-`min_token` GET must give transient
    /// store faults and NotFound propagation blips independent retry
    /// budgets. A transient blip followed by a NotFound blip against a real,
    /// acked commit must still resolve (read-your-write), not surface as
    /// `UnsatisfiableToken`. The `#[92]` `FaultStore` sequencing API scripts
    /// the two distinct faults on one key that a single `Rule` cannot.
    #[tokio::test]
    async fn min_token_transient_then_notfound_still_resolves_the_real_commit() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // Skewed bucket well outside the listing window, so the only GET on
        // the token key comes from the exact-token resolve path (never the
        // listing pass).
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &inner,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");
        let key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("key");

        // Script the token GET: first a transient blip, then a NotFound blip,
        // then a scripted pass-through that returns the real record. Under
        // the shared-budget defect the transient consumed the single retry
        // and the NotFound surfaced as UnsatisfiableToken on the second call.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(key)
                .then_fault(ScriptedFault::Transient("token get blip".into()))
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let snapshot = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("valid token must resolve despite a transient then a NotFound blip");
        assert_eq!(snapshot.segments.len(), 1);
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );

        // All three scripted steps fired: the transient retry and the
        // NotFound retry drew from independent budgets, then the record read
        // succeeded.
        assert_eq!(store.sequence_progress(0), 3, "all three steps consumed");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            1,
            "transient blip fired once"
        );
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            1,
            "NotFound blip fired once"
        );
    }

    /// The NotFound propagation budget stays at exactly one retry (two
    /// probes) as documented (docs/catalog-and-mvcc.md step 4: "Absent after
    /// one retry"). Two scripted NotFound blips exhaust the budget and
    /// surface `UnsatisfiableToken` as its own typed outcome, even though a
    /// third probe would have found the (present) record. This proves the
    /// fix did not widen the NotFound budget.
    #[tokio::test]
    async fn min_token_two_notfound_blips_surface_unsatisfiable_not_over_probing() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &inner,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");
        let key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("key");

        // Two NotFound blips then a pass-through. One retry is documented, so
        // the second NotFound is terminal: the pass-through (step index 2) is
        // never reached even though the record is present.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(key)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let err = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect_err("two NotFound probes must surface UnsatisfiableToken");
        assert!(matches!(err, CatalogError::UnsatisfiableToken { .. }));

        // Exactly two probes (one retry) fired; the third step was not
        // consumed, proving the NotFound budget was not widened.
        assert_eq!(
            store.sequence_progress(0),
            2,
            "only two NotFound probes; pass-through never reached"
        );
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            2,
            "both NotFound blips fired"
        );
    }

    // --- Retention (ADR-0019): tombstone exclusion, token semantics, and the
    // ADR-0010 §10 cache-invalidation-on-tombstone trigger. ---

    /// Write a retention tombstone into a bucket, exactly as ravel-maintain's
    /// retention sweep would.
    async fn write_tombstone(store: &MemoryStore, shard: u32, ingest_hour_bucket: u32) {
        let tombstone = ravel_proto::commit::v1::RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            retired_at_ns: 0,
            retention_window_ns: 0,
            record_count_observed: 0,
        };
        let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
        store
            .put(
                &key,
                tombstone.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put tombstone");
    }

    /// A resolver that lists a tombstone excludes the entire bucket from the
    /// snapshot (ADR-0019 decision 3).
    #[tokio::test]
    async fn tombstoned_bucket_is_excluded_from_resolution() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        publish_segment(&store, 0, 1, 500_000, now, now - 100, now).await;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        // Baseline: the segment resolves.
        let before = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(before.segments.len(), 1);

        // Tombstone the bucket: it now contributes nothing.
        write_tombstone(&store, 0, 500_000).await;
        let after = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert!(after.segments.is_empty(), "tombstoned bucket excluded");
    }

    /// A `min_commit_token` whose bucket is tombstoned resolves as satisfied
    /// with zero segments, not `unsatisfiable token`: the data was retired on
    /// purpose (ADR-0019 decision 3). Uses a skewed bucket outside the listing
    /// window so only the exact-token path is exercised.
    #[tokio::test]
    async fn token_over_tombstoned_bucket_is_satisfied_with_zero_segments() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &store,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");

        // Retire the bucket and remove the commit record (post physical sweep).
        write_tombstone(&store, 0, skewed_bucket).await;
        let commit_key =
            keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store
            .delete(&commit_key)
            .await
            .expect("delete commit record");

        let snapshot = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("tombstoned token is satisfied, not unsatisfiable");
        assert!(snapshot.segments.is_empty(), "satisfied with zero segments");
    }

    /// Observing a tombstone during resolution invalidates that bucket's
    /// cached commit records (ADR-0010 §10). Proven on the token path: a
    /// record cached before the tombstone would otherwise be served straight
    /// from cache (bypassing the tombstone), but the listing pass's
    /// invalidation drops it, so the token then resolves as satisfied-empty.
    #[tokio::test]
    async fn tombstone_observation_invalidates_cached_commit_records() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // In-window bucket, so the listing pass observes the tombstone.
        let bucket_hour = 500_000;
        let record = publish_segment(&store, 0, 1, bucket_hour, now, now - 100, now).await;
        let token = record::token_for(&record).expect("token");

        // First resolve WITH the token: caches the commit record (listing pass
        // and the exact-token path both populate the record cache).
        let first = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve");
        assert_eq!(first.segments.len(), 1);

        // Tombstone the bucket and delete the underlying commit record (a
        // mid-sweep state: record gone, tombstone present). The cache still
        // holds the record.
        write_tombstone(&store, 0, bucket_hour).await;
        let commit_key =
            keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store
            .delete(&commit_key)
            .await
            .expect("delete commit record");

        // Resolve WITH the token again. The listing pass observes the tombstone
        // and invalidates the bucket's cache; the token path then misses the
        // cache, GETs NotFound, falls back, sees the tombstone, and is
        // satisfied with zero segments. Without invalidation, the token path
        // would hit the stale cache and wrongly return the segment.
        let second = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve");
        assert!(
            second.segments.is_empty(),
            "invalidated cache: tombstoned bucket serves nothing on the token path"
        );
    }

    /// Wraps a store, injecting one bogus foreign-tenant key into the final
    /// page of the one `list()` call whose prefix matches `target_prefix`:
    /// simulates a backend or key-layout bug a correctly behaving
    /// `MemoryStore` can never itself produce, to exercise the ADR-0050 §2
    /// LIST-prefix assertion in [`Catalog::guarded_list_all`]. Scoped to a
    /// single target prefix, not every listing call, so the test's
    /// `resolve` (which lists several (shard, hour) buckets to cover its
    /// ingest-lag window) sees exactly one violation.
    struct ForeignKeyInjectingStore<S> {
        inner: S,
        target_prefix: String,
        foreign_key: String,
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for ForeignKeyInjectingStore<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            let mut page = self.inner.list(prefix, page).await?;
            if page.next.is_none() && prefix == self.target_prefix {
                page.objects.push(ObjectMeta {
                    key: self.foreign_key.clone(),
                    size: 0,
                    etag: ravel_object_store::Etag(String::new()),
                    version: ravel_object_store::Version(String::new()),
                    last_modified_unix_ms: 0,
                });
            }
            Ok(page)
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn list_result_outside_tenant_prefix_is_hard_error() {
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        // The resolve now lists one bounded LIST per shard (issue #730), so
        // the injection target is the per-shard prefix. `config(1)` has a
        // single shard, so matching shard 0's prefix injects exactly one
        // out-of-tenant key and the test observes exactly one violation.
        let target_prefix =
            keys::commit_shard_prefix(&tenant(), Signal::Metrics, 0).expect("prefix");
        let store = Arc::new(ForeignKeyInjectingStore {
            inner: MemoryStore::new(),
            target_prefix,
            foreign_key: "t/deadbeefdeadbeefdeadbeefdeadbeef/m/c/s0000/h00500000/foreign-key"
                .to_string(),
        });
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("an out-of-prefix listing result must hard-fail");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "list_prefix"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
    }

    /// ADR-0050 §2: a commit record and a compaction record whose
    /// tenant_hash disagrees with the prefix they were listed under are the
    /// highest-signal breach the metric must reflect. Both validators must
    /// count the breach on `ravel_catalog_isolation_breach_total`, not merely
    /// reject it. tenant_hash is checked before every other field (and before
    /// key reconstruction for the compaction record), so a foreign hash
    /// reaches exactly the breach branch.
    #[tokio::test]
    async fn record_tenant_hash_mismatch_records_isolation_breach() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let listed_under = tenant();
        let foreign = TenantHash([0xff; 16]);
        let now = 500_000 * NS_PER_HOUR;

        // A self-consistent commit record for the FOREIGN tenant, validated as
        // if it were listed under this tenant's prefix.
        let writer_id = Uuid::new_v4();
        let payload = b"payload".to_vec();
        let content_hash = content_hash_for(&payload);
        let commit = record::build(NewCommitRecord {
            tenant_hash: foreign,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 1_000,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 1_000,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");

        let err = validate_expected_fields(
            &catalog,
            &commit,
            &listed_under,
            Signal::Metrics,
            0,
            "listed-under-key",
        )
        .expect_err("a commit record naming a foreign tenant must be rejected");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected tenant_hash FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);

        // A compaction record declaring the foreign tenant. tenant_hash is
        // checked before key reconstruction, so a default-filled record with a
        // foreign hash reaches the breach branch without a reconstructable key.
        let compaction = CompactionRecord {
            tenant_hash: foreign.0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard: 0,
            ..Default::default()
        };
        let err = validate_compaction_expected_fields(
            &catalog,
            &compaction,
            &listed_under,
            Signal::Metrics,
            0,
            "listed-under-ckey",
        )
        .expect_err("a compaction record naming a foreign tenant must be rejected");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected tenant_hash FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 2);
    }

    /// Issue #850, Finding 1: `load_column_stats` routes every GET through the
    /// accounted, semaphore-bounded funnel (`guarded_get`), so the caller's
    /// `QueryAccounting` counts them under `AccountedOp::Get`. The exact charge
    /// is TWO GETs -- the HEAD and the one column-stats object -- and no snapshot
    /// part is fetched (Finding 2). A regression that re-added the raw
    /// `store.get` path would credit zero requests; one that re-added the
    /// per-part GET would credit three. Both fail this assertion.
    #[tokio::test]
    async fn load_column_stats_charges_exactly_two_accounted_gets() {
        let store = Arc::new(MemoryStore::new());
        let signal = Signal::Logs;
        let signal_num = signal::to_proto(signal) as u32;

        // One empty snapshot part.
        let part_bytes =
            crate::snapshot_format::encode_part(tenant().0, signal_num, 8, 10, &[]).expect("part");
        let part_hash = *blake3::hash(&part_bytes).as_bytes();
        let part_key = format!("t/{}/catalog/l/snap/empty.csnap", tenant().to_hex());
        store
            .put(
                &part_key,
                Bytes::from(part_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put part");

        // A consistent single-segment column-stats object bound to that part.
        let segments = vec![ravel_proto::catalog::v1::ColumnStatsSegment {
            ingest_hour_bucket: 1,
            shard: 0,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            columns: vec![ravel_proto::catalog::v1::ColumnStat {
                name: "status".to_string(),
                declared_type: 2,
                non_null_count: 1,
                null_count: 0,
                min: Some(ravel_proto::catalog::v1::ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(1)),
                }),
                max: Some(ravel_proto::catalog::v1::ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(1)),
                }),
                dictionary_present: true,
                dictionary: vec![ravel_proto::catalog::v1::DictEntry {
                    value: Some(ravel_proto::catalog::v1::ColumnValue {
                        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(1)),
                    }),
                    count: 1,
                }],
                sum: Some(1),
            }],
        }];
        let stats_bytes = crate::snapshot_format::encode_column_stats(
            tenant().0,
            signal_num,
            vec![part_hash.to_vec()],
            &segments,
        )
        .expect("encode column stats");
        let stats_hash = *blake3::hash(&stats_bytes).as_bytes();
        let stats_key = format!("t/{}/catalog/l/cstat/one.cstat", tenant().to_hex());
        store
            .put(
                &stats_key,
                Bytes::from(stats_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put stats");

        let head = ravel_proto::catalog::v1::SnapshotHead {
            format_version: crate::snapshot_format::HEAD_FORMAT_VERSION,
            tenant_hash: tenant().0.to_vec(),
            signal: signal_num,
            shard_count: 8,
            watermark_hour: 10,
            parts: vec![ravel_proto::catalog::v1::SnapshotPartRef {
                key: part_key,
                blake3: part_hash.to_vec(),
                size: part_bytes.len() as u64,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
            column_stats: Some(ravel_proto::catalog::v1::SnapshotColumnStatsRef {
                key: stats_key,
                blake3: stats_hash.to_vec(),
                size: stats_bytes.len() as u64,
                segment_count: 1,
                part_blake3: vec![part_hash.to_vec()],
            }),
            column_stats_part: None,
        };
        let head_bytes = crate::snapshot_format::encode_head(&head).expect("encode head");
        store
            .put(
                &crate::fold::head_object_key(&tenant(), signal),
                Bytes::from(head_bytes),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        let catalog = Catalog::new(store.clone(), config(8)).expect("catalog");
        let acc = QueryAccounting::new();
        let loaded = catalog
            .load_column_stats(&tenant(), signal, &acc)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(loaded.segments.len(), 1, "the one segment's stats loaded");

        let snap = acc.snapshot();
        assert_eq!(
            snap.s3_requests(AccountedOp::Get),
            2,
            "exactly the HEAD GET and the column-stats GET, no per-part GET"
        );
        assert_eq!(
            snap.s3_requests(AccountedOp::List),
            0,
            "no listing on this path"
        );
        assert_eq!(
            snap.s3_requests(AccountedOp::Head),
            0,
            "no HEAD op on this path"
        );
        assert_eq!(
            catalog.request_semaphore.available_permits(),
            MAX_CONCURRENT_REQUESTS,
            "every acquired permit was released"
        );
    }

    /// Write a folded HEAD plus its `.cstat` object for `tenant`/`signal`, whose
    /// one segment carries a single I64 `status` column with the exact value
    /// `value` (min == max == `value`, one non-null row). `part_hash` binds the
    /// HEAD's part set to the stats object; reusing the same `part_hash` across
    /// calls keeps the part binding fixed so a re-resolve is driven purely by
    /// the stats object's content hash changing with `value`. The object keys
    /// are namespaced by tenant hex and signal prefix so distinct
    /// `(tenant, signal)` installs never collide in one store.
    async fn install_stats(
        store: &MemoryStore,
        tenant: TenantHash,
        signal: Signal,
        part_hash: [u8; 32],
        value: i64,
    ) {
        let signal_num = signal::to_proto(signal) as u32;
        let prefix = signal.key_prefix();

        let segments = vec![ravel_proto::catalog::v1::ColumnStatsSegment {
            ingest_hour_bucket: 1,
            shard: 0,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            columns: vec![ravel_proto::catalog::v1::ColumnStat {
                name: "status".to_string(),
                declared_type: 2,
                non_null_count: 1,
                null_count: 0,
                min: Some(ravel_proto::catalog::v1::ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(value)),
                }),
                max: Some(ravel_proto::catalog::v1::ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(value)),
                }),
                dictionary_present: true,
                dictionary: vec![ravel_proto::catalog::v1::DictEntry {
                    value: Some(ravel_proto::catalog::v1::ColumnValue {
                        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(value)),
                    }),
                    count: 1,
                }],
                sum: Some(value),
            }],
        }];
        let stats_bytes = crate::snapshot_format::encode_column_stats(
            tenant.0,
            signal_num,
            vec![part_hash.to_vec()],
            &segments,
        )
        .expect("encode column stats");
        let stats_hash = *blake3::hash(&stats_bytes).as_bytes();
        let stats_key = format!("t/{}/catalog/{prefix}/cstat/one.cstat", tenant.to_hex());
        store
            .put(
                &stats_key,
                Bytes::from(stats_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put stats");

        let head = ravel_proto::catalog::v1::SnapshotHead {
            format_version: crate::snapshot_format::HEAD_FORMAT_VERSION,
            tenant_hash: tenant.0.to_vec(),
            signal: signal_num,
            shard_count: 8,
            watermark_hour: 10,
            parts: vec![ravel_proto::catalog::v1::SnapshotPartRef {
                key: format!("t/{}/catalog/{prefix}/snap/empty.csnap", tenant.to_hex()),
                blake3: part_hash.to_vec(),
                size: 1,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
            column_stats: Some(ravel_proto::catalog::v1::SnapshotColumnStatsRef {
                key: stats_key,
                blake3: stats_hash.to_vec(),
                size: stats_bytes.len() as u64,
                segment_count: 1,
                part_blake3: vec![part_hash.to_vec()],
            }),
            column_stats_part: None,
        };
        let head_bytes = crate::snapshot_format::encode_head(&head).expect("encode head");
        store
            .put(
                &crate::fold::head_object_key(&tenant, signal),
                Bytes::from(head_bytes),
                PutOptions::default(),
            )
            .await
            .expect("put head");
    }

    /// [`install_stats`] for the default [`tenant`] on `Signal::Logs`, the
    /// fixture the issue #888 reuse tests were written against.
    async fn install_logs_stats(store: &MemoryStore, part_hash: [u8; 32], value: i64) {
        install_stats(store, tenant(), Signal::Logs, part_hash, value).await;
    }

    /// The exact I64 `status` value carried by the one segment of a loaded
    /// column-stats object built by [`install_logs_stats`].
    fn loaded_value(loaded: &LoadedColumnStats) -> i64 {
        let segment = loaded.segments.values().next().expect("one segment");
        match &segment.columns[0].min.as_ref().expect("min present").kind {
            Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)) => *v,
            other => panic!("expected an I64 min, got {other:?}"),
        }
    }

    /// Issue #888, deliverable 2: two consecutive loads against an UNCHANGED
    /// folded HEAD resolve the stats object exactly once. The first load pays
    /// both GETs (HEAD + `.cstat`); the second re-reads HEAD (one GET, to
    /// detect a fold) and serves the cached object, skipping the second GET.
    /// Both return the same statistics.
    ///
    /// Pre-cache demonstration: without the cache the second load also fetches
    /// the stats object, so its accounted GET count is 2, not 1.
    #[tokio::test]
    async fn load_column_stats_reuses_cache_on_unchanged_head() {
        let store = Arc::new(MemoryStore::new());
        let part_hash = *blake3::hash(b"part-0").as_bytes();
        install_logs_stats(&store, part_hash, 42).await;

        let catalog = Catalog::new(store.clone(), config(8)).expect("catalog");

        let acc1 = QueryAccounting::new();
        let first = catalog
            .load_column_stats(&tenant(), Signal::Logs, &acc1)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            acc1.snapshot().s3_requests(AccountedOp::Get),
            2,
            "first load pays the HEAD GET and the column-stats GET"
        );

        let acc2 = QueryAccounting::new();
        let second = catalog
            .load_column_stats(&tenant(), Signal::Logs, &acc2)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            acc2.snapshot().s3_requests(AccountedOp::Get),
            1,
            "unchanged HEAD: only the HEAD GET, the stats object is served from cache"
        );

        assert_eq!(loaded_value(&first), 42);
        assert_eq!(
            loaded_value(&second),
            42,
            "the cached object is the same one"
        );
    }

    /// Issue #888, deliverable 2: a fold that changes the folded HEAD must
    /// re-resolve, never serve the superseded object. Because the stats object
    /// is keyed by its own content hash (not the pinned snapshot), a rewritten
    /// object gets a new `blake3`, misses the cache, and is re-fetched. The
    /// reload pays both GETs again and returns the NEW statistics.
    ///
    /// A cache keyed by anything a fold does not change (the pinned snapshot,
    /// say) would return the stale `42` here.
    #[tokio::test]
    async fn load_column_stats_rereleases_after_head_change() {
        let store = Arc::new(MemoryStore::new());
        let part_hash = *blake3::hash(b"part-0").as_bytes();
        install_logs_stats(&store, part_hash, 42).await;

        let catalog = Catalog::new(store.clone(), config(8)).expect("catalog");

        let acc1 = QueryAccounting::new();
        let first = catalog
            .load_column_stats(&tenant(), Signal::Logs, &acc1)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(loaded_value(&first), 42);

        // A new fold: same part set, but the stats object's content changes, so
        // HEAD now references a different `blake3`.
        install_logs_stats(&store, part_hash, 99).await;

        let acc2 = QueryAccounting::new();
        let second = catalog
            .load_column_stats(&tenant(), Signal::Logs, &acc2)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            acc2.snapshot().s3_requests(AccountedOp::Get),
            2,
            "a changed HEAD re-resolves: HEAD GET plus a fresh column-stats GET"
        );
        assert_eq!(
            loaded_value(&second),
            99,
            "the reload returns the new object, never the cached stale one"
        );
    }

    fn tenant_n(n: u8) -> TenantHash {
        TenantHash([n; 16])
    }

    /// A `LoadedColumnStats` carrying one segment and `parts` covered-part
    /// hashes, for driving [`ColumnStatsCache`] directly with a known byte
    /// weight. The segment's content is fixed, so every call produces an object
    /// of identical [`LoadedColumnStats::heap_bytes`].
    fn make_loaded(parts: usize) -> Arc<LoadedColumnStats> {
        let segment = ravel_proto::catalog::v1::ColumnStatsSegment {
            ingest_hour_bucket: 1,
            shard: 0,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            columns: Vec::new(),
        };
        let identity: crate::EntryIdentity = (1, 0, [0xAA; 16], 1, 1);
        let mut segments = HashMap::new();
        segments.insert(identity, segment);
        let part_blake3 = (0..parts).map(|i| [i as u8; 32]).collect();
        Arc::new(LoadedColumnStats {
            segments,
            part_blake3,
        })
    }

    /// Issue #905, exact byte accounting: the cache reports the bytes actually
    /// held as the exact sum of each segment's encoded protobuf length plus 32
    /// bytes per covered part hash, not `> 0` and not an entry count.
    ///
    /// Flip to watch it fail: change `heap_bytes` to return the entry count
    /// (`self.segments.len() as u64`) and the `held_bytes` assertion drops to
    /// `1`.
    #[test]
    fn column_stats_cache_accounts_exact_held_bytes() {
        let loaded = make_loaded(3);
        // Independently summed, not via heap_bytes: the payload the budget bounds.
        let expected: u64 = loaded
            .segments
            .values()
            .map(|segment| segment.encoded_len() as u64)
            .sum::<u64>()
            + 3 * 32;
        assert_eq!(
            loaded.heap_bytes(),
            expected,
            "heap_bytes is the summed segment encoded length plus 32 bytes per part"
        );

        let cache = ColumnStatsCache::new(1 << 20);
        cache.insert((tenant(), Signal::Logs), [1u8; 32], Arc::clone(&loaded));
        assert_eq!(
            cache.held_bytes(),
            expected,
            "held bytes is the exact payload, not an entry count"
        );
    }

    /// Issue #905, the budget evicts at the boundary: a budget of exactly two
    /// entries holds two with no eviction, and the third insert evicts exactly
    /// the least-recently-used one, leaving the exact surviving set.
    ///
    /// Flip to watch it fail: change the eviction guard from `held_bytes + bytes
    /// > self.max_bytes` to `>=` and the two-entry boundary evicts one early, so
    /// `evictions()` reads `1` after the second insert.
    #[test]
    fn column_stats_cache_evicts_lru_at_byte_boundary() {
        let entry = make_loaded(1);
        let b = entry.heap_bytes();
        let cache = ColumnStatsCache::new(2 * b);

        let ka = (tenant_n(1), Signal::Logs);
        let kb = (tenant_n(2), Signal::Logs);
        let kc = (tenant_n(3), Signal::Logs);
        let parts = entry.part_blake3.clone();

        cache.insert(ka, [1u8; 32], Arc::clone(&entry));
        cache.insert(kb, [1u8; 32], Arc::clone(&entry));
        assert_eq!(cache.held_bytes(), 2 * b, "two entries fit exactly");
        assert_eq!(cache.evictions(), 0, "no eviction at the boundary");

        // One over the boundary: evicts the least-recently-used entry (ka).
        cache.insert(kc, [1u8; 32], Arc::clone(&entry));
        assert_eq!(cache.evictions(), 1, "exactly one eviction");
        assert_eq!(
            cache.held_bytes(),
            2 * b,
            "still exactly two entries' bytes"
        );

        assert!(
            cache.get(ka, &[1u8; 32], &parts).is_none(),
            "the least-recently-used entry was evicted"
        );
        assert!(cache.get(kb, &[1u8; 32], &parts).is_some(), "kb survives");
        assert!(
            cache.get(kc, &[1u8; 32], &parts).is_some(),
            "the just-inserted kc survives"
        );
    }

    /// Issue #905, an eviction increments the observability counter by exactly
    /// one. A single insert past a one-entry budget evicts one entry and bumps
    /// `evictions` by exactly one; an object larger than the whole budget is
    /// refused (not evicted) and bumps `refusals` instead.
    ///
    /// Flip to watch it fail: drop the `self.evictions.fetch_add(1, ...)` in
    /// `insert` and the eviction assertion reads `0`.
    #[test]
    fn column_stats_cache_eviction_counter_increments_by_one() {
        let entry = make_loaded(1);
        let b = entry.heap_bytes();
        let parts = entry.part_blake3.clone();

        let cache = ColumnStatsCache::new(b);
        cache.insert((tenant_n(1), Signal::Logs), [1u8; 32], Arc::clone(&entry));
        cache.insert((tenant_n(2), Signal::Logs), [1u8; 32], Arc::clone(&entry));
        assert_eq!(cache.evictions(), 1, "one eviction, counted once");
        assert_eq!(cache.refusals(), 0, "an eviction is not a refusal");

        // An object larger than the whole budget: refused, not evicted.
        let refuse_cache = ColumnStatsCache::new(b - 1);
        refuse_cache.insert((tenant_n(1), Signal::Logs), [1u8; 32], Arc::clone(&entry));
        assert_eq!(refuse_cache.refusals(), 1, "oversized object refused once");
        assert_eq!(
            refuse_cache.evictions(),
            0,
            "nothing to evict for a refusal"
        );
        assert_eq!(
            refuse_cache.held_bytes(),
            0,
            "the oversized object is not cached"
        );
    }

    /// Issue #905, deliverable 4: a query whose statistics were evicted returns
    /// the SAME rows as one that never had them. An evicted entry is a cache
    /// miss that re-fetches the object; no partial or stale statistic reaches
    /// the caller.
    ///
    /// Flip to watch it fail: make `ColumnStatsCache::get` ignore the
    /// `part_blake3`/`stats_blake3` binding and return a still-resident stale
    /// entry, and the `segments` equality against the never-cached load breaks.
    #[tokio::test]
    async fn evicted_column_stats_reload_returns_same_rows() {
        let store = Arc::new(MemoryStore::new());
        let part_a = *blake3::hash(b"part-a").as_bytes();
        let part_b = *blake3::hash(b"part-b").as_bytes();
        let ta = tenant_n(1);
        let tb = tenant_n(2);
        install_stats(&store, ta, Signal::Logs, part_a, 42).await;
        install_stats(&store, tb, Signal::Logs, part_b, 7).await;

        // Control: the cache disabled, so tenant A "never had" cached stats.
        let disabled = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 8,
                column_stats_cache_max_bytes: 0,
                ..Default::default()
            },
        )
        .expect("catalog");
        let fresh_a = disabled
            .load_column_stats(&ta, Signal::Logs, &QueryAccounting::new())
            .await
            .expect("load ok")
            .expect("stats present");
        let entry_bytes = fresh_a.heap_bytes();
        assert_eq!(
            disabled.column_stats_cache_evictions(),
            0,
            "a disabled cache never evicts"
        );

        // A budget that holds exactly one entry: loading tenant B evicts A's.
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 8,
                column_stats_cache_max_bytes: entry_bytes,
                ..Default::default()
            },
        )
        .expect("catalog");

        catalog
            .load_column_stats(&ta, Signal::Logs, &QueryAccounting::new())
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            catalog.column_stats_cache_held_bytes(),
            entry_bytes,
            "tenant A's stats are cached"
        );

        catalog
            .load_column_stats(&tb, Signal::Logs, &QueryAccounting::new())
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            catalog.column_stats_cache_evictions(),
            1,
            "loading tenant B evicted tenant A's entry"
        );

        // Reload A: its entry was evicted, so this re-fetches (two GETs).
        let acc_reload = QueryAccounting::new();
        let reloaded_a = catalog
            .load_column_stats(&ta, Signal::Logs, &acc_reload)
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            acc_reload.snapshot().s3_requests(AccountedOp::Get),
            2,
            "the evicted entry re-fetches: HEAD GET plus a fresh column-stats GET"
        );

        assert_eq!(
            reloaded_a.segments, fresh_a.segments,
            "the evicted-then-reloaded stats equal the never-cached stats, row for row"
        );
        assert_eq!(
            reloaded_a.part_blake3, fresh_a.part_blake3,
            "the covered part set is unchanged by eviction"
        );
        assert_eq!(loaded_value(&reloaded_a), 42, "and carry tenant A's value");
    }
}
