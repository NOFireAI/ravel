//! Bounded per-tenant cache of decoded commit records, keyed by full object
//! key (docs/catalog-and-mvcc.md step 2, ADR-0010 §10).
//!
//! LRU eviction, the other of the two strategies the ADR allows ("simple LRU
//! or capacity cap per tenant"). The cache outlives any one resolve, so the
//! hot ingest hours a query keeps returning to must not be evicted by a
//! one-off wide scan or a fold's sweep of older buckets, which is exactly
//! what insertion-order eviction does to them. Commit records are immutable
//! once published (Phase 1 has no deletion), so eviction only costs a
//! re-GET+decode on the next miss, never correctness. Field validation
//! against the expected (tenant, signal, shard) happens in the caller on
//! every hit and every fresh decode, not here: the cache only stores and
//! evicts.
//!
//! Both record caches are held to a byte budget as well as an entry cap,
//! because neither record type has a bounded size: `CommitRecord` and
//! `CompactionPart` both carry a `declared_column_stats` list that the format
//! does not cap, and a `CompactionRecord` carries one input identity per
//! compacted L0 segment. Each entry is charged an estimate of its live heap
//! ([`commit_entry_resident_bytes`], [`compaction_entry_resident_bytes`]) and
//! eviction runs until the tenant's summed charge is inside its budget.
//!
//! A `capacity` of 0 is the disabled sentinel (matching
//! [`CatalogConfig::byte_cache_max_bytes`](crate::CatalogConfig::byte_cache_max_bytes)):
//! nothing is admitted at all, so every read falls through to a store GET.
//!
//! Every `get()` below also records a hit or a miss, and a hit's stored byte
//! size, into the caller's [`QueryAccounting`] (ADR-0044): the
//! byte count is the size of the raw object the cached value was originally
//! decoded from, captured once at `insert()` time so a hit never re-derives
//! it. This is the only place cache bytes are counted; a miss falls through
//! to a funnel GET, which counts its own `s3_bytes` instead, so a cached
//! object's bytes are never counted twice.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ravel_proto::catalog::v1::SnapshotHead;
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash};

use crate::snapshot_format::{DecodedPart, DecodedPostings};

/// Charged cost of one cached commit entry beyond its variable-length
/// members: the `CommitRecord` struct (about 224 bytes of scalars and
/// `Vec`/`String` headers), its `Arc` allocation, the [`RecordEntry`] wrapper's
/// own fields, the key `String` header held twice (the entry map and the
/// recency index) and the two collections' slot overhead. The members that
/// vary in length are charged separately by [`commit_entry_resident_bytes`].
const COMMIT_ENTRY_FIXED_BYTES: u64 = 416;

/// Estimated live heap one cached commit entry holds, the figure
/// [`TenantCache`] evicts against.
///
/// `CommitRecord.declared_column_stats` is a repeated field capped neither by
/// the format (proto/ravel/commit.proto) nor by
/// `ravel_commit::record::validate`, and the tenant-config declared-column path
/// does not bound the count either
/// (`ravel_commit::declared_stats`'s own
/// `an_entry_count_above_any_declared_column_list_is_read_in_full` decodes
/// 4,096 entries in one record), so an entry-count bound does not bound this
/// cache's memory. Each statistic is charged on the same basis a compaction
/// part's is ([`DECLARED_COLUMN_STAT_FIXED_BYTES`] plus its column name), since
/// it is the same `DeclaredColumnMinMax` struct. An estimate, not a
/// measurement of the allocator.
fn commit_entry_resident_bytes(key: &str, record: &CommitRecord) -> u64 {
    let stats: u64 = record
        .declared_column_stats
        .iter()
        .map(|stat| DECLARED_COLUMN_STAT_FIXED_BYTES + stat.name.len() as u64)
        .sum();
    COMMIT_ENTRY_FIXED_BYTES
        + 2 * key.len() as u64
        + record.tenant_hash.len() as u64
        + record.writer_id.len() as u64
        + record.object_key.len() as u64
        + record.content_hash.len() as u64
        + stats
}

/// One cached commit record plus the recency stamp that positions it in
/// [`TenantCache::by_use`].
struct RecordEntry {
    record: Arc<CommitRecord>,
    /// Size of the raw object this was decoded from, captured at insert so a
    /// hit never re-derives it (see the module docs on accounting).
    bytes: u64,
    /// What this entry is charged against the tenant's byte budget
    /// ([`commit_entry_resident_bytes`]); not what a hit reports.
    resident_bytes: u64,
    use_tick: u64,
}

#[derive(Default)]
struct TenantCache {
    entries: HashMap<String, RecordEntry>,
    /// Recency index, least-recently-used first: `use_tick -> key`, in step
    /// with `entries`. A monotonic tick keeps a touch O(log n) rather than
    /// the O(n) a queue would cost, which matters because a single resolve
    /// over a hot ingest hour touches every entry twice (the concurrent
    /// prewarm, then the sequential include pass).
    by_use: std::collections::BTreeMap<u64, String>,
    next_tick: u64,
    /// Summed [`RecordEntry::resident_bytes`] of everything in `entries`.
    charged_bytes: u64,
}

impl TenantCache {
    /// Return the entry for `key` and mark it most-recently-used, or `None`
    /// when absent (an absent key leaves the recency order untouched).
    fn touch(&mut self, key: &str) -> Option<(Arc<CommitRecord>, u64)> {
        let tick = self.next_tick;
        let entry = self.entries.get_mut(key)?;
        let previous = entry.use_tick;
        entry.use_tick = tick;
        let hit = (entry.record.clone(), entry.bytes);
        self.by_use.remove(&previous);
        self.by_use.insert(tick, key.to_string());
        self.next_tick += 1;
        Some(hit)
    }

    /// Admit `record` and evict least-recently-used until the cache is inside
    /// BOTH bounds: `max_bytes` of charged residency and `capacity` entries.
    /// The byte bound is the one that holds this cache to its share of
    /// `MAX_RECORD_CACHE_BYTES_PER_TENANT`, because `CommitRecord` carries an
    /// uncapped `declared_column_stats` list and so has no bounded per-entry
    /// size (see [`commit_entry_resident_bytes`]).
    fn insert(
        &mut self,
        key: String,
        record: Arc<CommitRecord>,
        bytes: u64,
        capacity: usize,
        max_bytes: u64,
    ) {
        if capacity == 0 {
            return;
        }
        // Already held: records are immutable, so keep the stored value and
        // only refresh its recency.
        if self.touch(&key).is_some() {
            return;
        }
        let tick = self.next_tick;
        self.next_tick += 1;
        let resident_bytes = commit_entry_resident_bytes(&key, &record);
        self.charged_bytes = self.charged_bytes.saturating_add(resident_bytes);
        self.entries.insert(
            key.clone(),
            RecordEntry {
                record,
                bytes,
                resident_bytes,
                use_tick: tick,
            },
        );
        self.by_use.insert(tick, key);
        while self.entries.len() > capacity || self.charged_bytes > max_bytes {
            let Some((_, evicted)) = self.by_use.pop_first() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&evicted) {
                self.charged_bytes = self.charged_bytes.saturating_sub(evicted.resident_bytes);
            }
        }
    }
}

/// Decoded-record cache, partitioned by tenant. Process-wide: one `Catalog`
/// is built per process and held for its lifetime, so an entry admitted by one
/// resolve is served to every later resolve over the same object key, which is
/// what makes a repeated resolve over an unsealed ingest hour cost its LISTs
/// and nothing else (issue #783).
#[derive(Default)]
pub(crate) struct RecordCache {
    tenants: Mutex<HashMap<TenantHash, TenantCache>>,
}

impl RecordCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Option<Arc<CommitRecord>> {
        let hit = self
            .tenants
            .lock()
            .get_mut(tenant)
            .and_then(|c| c.touch(key));
        match hit {
            Some((record, bytes)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes);
                Some(record)
            }
            None => {
                accounting.record_cache_miss();
                None
            }
        }
    }

    /// True if `key` is currently resident for `tenant`, without recording a
    /// cache hit or miss and without changing recency. The resolve path uses
    /// this at its single commit-record warming site (the prewarm pass) to
    /// decide "served from cache" for the exact `commit_record_cache_hits`
    /// count, separately from the pooled hit/miss accounting [`Self::get`]
    /// performs on every touch; peeking here rather than counting a second
    /// [`Self::get`] is what keeps the pooled counters untouched.
    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        record: Arc<CommitRecord>,
        bytes: u64,
        capacity: usize,
        max_bytes: u64,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, bytes, capacity, max_bytes);
    }

    /// Charged residency currently held for `tenant`, the figure eviction
    /// holds inside
    /// [`CatalogConfig::commit_cache_max_bytes_per_tenant`](crate::CatalogConfig::commit_cache_max_bytes_per_tenant).
    #[cfg(test)]
    pub(crate) fn charged_bytes(&self, tenant: &TenantHash) -> u64 {
        self.tenants
            .lock()
            .get(tenant)
            .map_or(0, |cache| cache.charged_bytes)
    }

    /// Number of entries currently resident for `tenant`.
    #[cfg(test)]
    pub(crate) fn entry_count(&self, tenant: &TenantHash) -> usize {
        self.tenants
            .lock()
            .get(tenant)
            .map_or(0, |cache| cache.entries.len())
    }

    /// Drop every cached record for `tenant` whose key starts with `prefix`.
    /// The tombstone-observation invalidation trigger ADR-0010 §10 promises:
    /// when a resolver lists a bucket's retention tombstone, the bucket's
    /// cached commit records are dropped so a later token-fallback GET cannot
    /// serve a record the sweep is about to physically remove.
    pub(crate) fn invalidate_prefix(&self, tenant: &TenantHash, prefix: &str) {
        if let Some(cache) = self.tenants.lock().get_mut(tenant) {
            let mut dropped: u64 = 0;
            cache.entries.retain(|k, entry| {
                if k.starts_with(prefix) {
                    dropped = dropped.saturating_add(entry.resident_bytes);
                    false
                } else {
                    true
                }
            });
            cache.charged_bytes = cache.charged_bytes.saturating_sub(dropped);
            cache.by_use.retain(|_, k| !k.starts_with(prefix));
        }
    }

    /// Drop the whole per-tenant outer-map entry for `tenant` (ADR-0069
    /// decision 2, idle-tenant state eviction). Returns whether an entry was
    /// present. Safe because commit records are immutable and re-derivable: a
    /// later access re-GETs and re-decodes on a miss, never wrong data.
    pub(crate) fn evict_tenant(&self, tenant: &TenantHash) -> bool {
        self.tenants.lock().remove(tenant).is_some()
    }
}

/// Charged cost of one cached compaction entry beyond its variable-length
/// members: the `CompactionRecord` struct, its `Arc` allocation, the key
/// `String` header held twice (map key and order queue) and the two
/// collections' slot overhead. The members that vary in length are charged
/// separately by [`compaction_entry_resident_bytes`].
const COMPACTION_ENTRY_FIXED_BYTES: u64 = 320;
/// Charged cost of one decoded `CompactionInputIdentity`: the struct itself
/// (a `String` header and two `u64`s) as it sits in the `inputs` vector. Its
/// `writer_id` heap allocation is charged on top, per input.
const COMPACTION_INPUT_FIXED_BYTES: u64 = 40;
/// Charged cost of one decoded `CompactionPart` struct as it sits in the
/// `parts` vector: three `Vec<u8>` headers (72), four `u64`s (32), two `i64`s
/// (16), two `u32`s (8) and the `declared_column_stats` vector header (24),
/// which is 152 rounded up to the next multiple of 16. Its byte vectors and
/// statistics are charged on top.
const COMPACTION_PART_FIXED_BYTES: u64 = 160;
/// Charged cost of one decoded `DeclaredColumnMinMax` struct: a `String`
/// header, a `u32`, two optional message-typed extrema and a `u64`. The
/// column name's heap allocation is charged on top. Both record types carry
/// this same struct, so both charge it the same way: on a `CompactionPart` via
/// [`compaction_entry_resident_bytes`], on a `CommitRecord` via
/// [`commit_entry_resident_bytes`].
const DECLARED_COLUMN_STAT_FIXED_BYTES: u64 = 80;

/// Estimated live heap one cached compaction entry holds, the figure
/// [`CompactionTenantCache`] evicts against.
///
/// A `CompactionRecord` carries one `CompactionInputIdentity` per compacted L0
/// segment and one `CompactionPart` per output object, neither capped by the
/// format (proto/ravel/commit.proto) nor by
/// `ravel_commit::record::validate_compaction`, so its size is not a constant
/// and an entry-count bound does not bound this cache's memory. Same basis as
/// the column-statistics cache's own
/// [`heap_bytes`](crate::column_stats_resolve::LoadedColumnStats::heap_bytes):
/// every struct that is held, plus every heap allocation hanging off it. An
/// estimate, not a measurement of the allocator.
fn compaction_entry_resident_bytes(key: &str, record: &CompactionRecord) -> u64 {
    let inputs: u64 = record
        .inputs
        .iter()
        .map(|input| COMPACTION_INPUT_FIXED_BYTES + input.writer_id.len() as u64)
        .sum();
    let parts: u64 = record
        .parts
        .iter()
        .map(|part| {
            let stats: u64 = part
                .declared_column_stats
                .iter()
                .map(|stat| DECLARED_COLUMN_STAT_FIXED_BYTES + stat.name.len() as u64)
                .sum();
            COMPACTION_PART_FIXED_BYTES
                + part.first_series_id.len() as u64
                + part.last_series_id.len() as u64
                + part.content_hash.len() as u64
                + stats
        })
        .sum();
    COMPACTION_ENTRY_FIXED_BYTES
        + 2 * key.len() as u64
        + record.tenant_hash.len() as u64
        + record.input_set_hash.len() as u64
        + inputs
        + parts
}

struct CompactionEntry {
    record: Arc<CompactionRecord>,
    /// Size of the raw object this was decoded from, for the accounting a hit
    /// records (see the module docs); not what eviction charges.
    bytes: u64,
    /// What this entry is charged against the tenant's byte budget
    /// ([`compaction_entry_resident_bytes`]).
    resident_bytes: u64,
}

#[derive(Default)]
struct CompactionTenantCache {
    entries: HashMap<String, CompactionEntry>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
    /// Summed [`CompactionEntry::resident_bytes`] of everything in `entries`.
    charged_bytes: u64,
}

impl CompactionTenantCache {
    /// Admit `record` and evict oldest-first until the cache is inside BOTH
    /// bounds: `max_bytes` of charged residency and `capacity` entries. The
    /// byte bound is the one that holds this cache to its share of
    /// `MAX_RECORD_CACHE_BYTES_PER_TENANT`, because a single compaction record
    /// can hold thousands of inputs and so cost hundreds of times what an
    /// ordinary commit-record entry does (see
    /// [`compaction_entry_resident_bytes`]). A
    /// `max_bytes` of `0` is the disabled sentinel `cache_capacity_per_tenant
    /// == 0` resolves to: the entry is admitted and immediately evicted, so
    /// nothing is ever resident.
    fn insert(
        &mut self,
        key: String,
        record: Arc<CompactionRecord>,
        bytes: u64,
        capacity: usize,
        max_bytes: u64,
    ) {
        if self.entries.contains_key(&key) {
            return;
        }
        let resident_bytes = compaction_entry_resident_bytes(&key, &record);
        self.charged_bytes = self.charged_bytes.saturating_add(resident_bytes);
        self.entries.insert(
            key.clone(),
            CompactionEntry {
                record,
                bytes,
                resident_bytes,
            },
        );
        self.order.push_back(key);
        while self.charged_bytes > max_bytes || self.order.len() > capacity.max(1) {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.charged_bytes = self.charged_bytes.saturating_sub(evicted.resident_bytes);
            }
        }
    }
}

/// Decoded compaction-record cache, partitioned by tenant and keyed by full
/// object key, like [`RecordCache`] (docs/catalog-and-mvcc.md step 2:
/// compaction records are cached the same way commit records are), bounded in
/// BYTES as well as in entries like [`RecordCache`]: a compaction record's size
/// grows with its input list, so an entry count does not bound its memory (see
/// [`compaction_entry_resident_bytes`] and
/// [`CatalogConfig::compaction_cache_max_bytes_per_tenant`](crate::CatalogConfig::compaction_cache_max_bytes_per_tenant)).
/// Compaction records are immutable once published, so entries are only ever
/// budget evicted, except for one trigger: when a resolver observes a
/// bucket's retention tombstone it drops that bucket's cached compaction
/// records via [`CompactionRecordCache::invalidate_prefix`] (ADR-0010 §10,
/// the same tombstone-observation invalidation as [`RecordCache`]). Exclusion
/// itself already happens at resolution before the cache matters, so this is a
/// hygiene trigger, not a correctness dependency; it keeps a later
/// token-fallback GET from serving a record the sweep is about to remove.
#[derive(Default)]
pub(crate) struct CompactionRecordCache {
    tenants: Mutex<HashMap<TenantHash, CompactionTenantCache>>,
}

impl CompactionRecordCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Option<Arc<CompactionRecord>> {
        let hit = self
            .tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key))
            .map(|entry| (entry.record.clone(), entry.bytes));
        match hit {
            Some((record, bytes)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes);
                Some(record)
            }
            None => {
                accounting.record_cache_miss();
                None
            }
        }
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        record: Arc<CompactionRecord>,
        bytes: u64,
        capacity: usize,
        max_bytes: u64,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, bytes, capacity, max_bytes);
    }

    /// Drop every cached compaction record for `tenant` whose key starts with
    /// `prefix` (ADR-0010 §10 tombstone-observation invalidation).
    pub(crate) fn invalidate_prefix(&self, tenant: &TenantHash, prefix: &str) {
        if let Some(cache) = self.tenants.lock().get_mut(tenant) {
            let mut dropped: u64 = 0;
            cache.entries.retain(|k, entry| {
                if k.starts_with(prefix) {
                    dropped = dropped.saturating_add(entry.resident_bytes);
                    false
                } else {
                    true
                }
            });
            cache.charged_bytes = cache.charged_bytes.saturating_sub(dropped);
            cache.order.retain(|k| !k.starts_with(prefix));
        }
    }

    /// Charged residency currently held for `tenant`, the figure eviction
    /// holds inside
    /// [`CatalogConfig::compaction_cache_max_bytes_per_tenant`](crate::CatalogConfig::compaction_cache_max_bytes_per_tenant).
    #[cfg(test)]
    pub(crate) fn charged_bytes(&self, tenant: &TenantHash) -> u64 {
        self.tenants
            .lock()
            .get(tenant)
            .map_or(0, |cache| cache.charged_bytes)
    }

    /// Number of entries currently resident for `tenant`.
    #[cfg(test)]
    pub(crate) fn entry_count(&self, tenant: &TenantHash) -> usize {
        self.tenants
            .lock()
            .get(tenant)
            .map_or(0, |cache| cache.entries.len())
    }

    /// Drop the whole per-tenant outer-map entry for `tenant` (ADR-0069
    /// decision 2). Returns whether an entry was present. Compaction records
    /// are immutable and re-derivable, so a later miss re-GETs and re-decodes.
    pub(crate) fn evict_tenant(&self, tenant: &TenantHash) -> bool {
        self.tenants.lock().remove(tenant).is_some()
    }
}

struct HeadCacheEntry {
    head: Arc<SnapshotHead>,
    bytes: u64,
    cached_at_ns: i64,
}

/// State behind [`HeadCache`]'s single lock: the entry map plus its
/// insertion order, so capacity-cap eviction can pop the oldest (tenant, signal) pair
/// without a second, separately-lockable structure racing the first.
#[derive(Default)]
struct HeadCacheState {
    entries: HashMap<(TenantHash, Signal), HeadCacheEntry>,
    order: std::collections::VecDeque<(TenantHash, Signal)>,
}

/// Decoded-HEAD cache, one entry per (tenant, signal), with a caller-checked
/// TTL (`head_cache_ttl`, default 30s) and a
/// capacity-cap bound on the number of (tenant, signal) pairs held at once. `now_ns` is always caller-supplied: this cache never reads
/// a clock.
#[derive(Default)]
pub(crate) struct HeadCache {
    state: Mutex<HeadCacheState>,
}

impl HeadCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        now_ns: i64,
        ttl_ns: i64,
        accounting: &QueryAccounting,
    ) -> Option<Arc<SnapshotHead>> {
        let state = self.state.lock();
        let fresh = state.entries.get(&(*tenant, signal)).and_then(|entry| {
            if now_ns.saturating_sub(entry.cached_at_ns) <= ttl_ns {
                Some((entry.head.clone(), entry.bytes))
            } else {
                None
            }
        });
        drop(state);
        match fresh {
            Some((head, bytes)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes);
                Some(head)
            }
            None => {
                accounting.record_cache_miss();
                None
            }
        }
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        signal: Signal,
        head: Arc<SnapshotHead>,
        bytes: u64,
        now_ns: i64,
        capacity: usize,
    ) {
        let mut state = self.state.lock();
        let entry_key = (tenant, signal);
        if !state.entries.contains_key(&entry_key) {
            state.order.push_back(entry_key);
        }
        state.entries.insert(
            entry_key,
            HeadCacheEntry {
                head,
                bytes,
                cached_at_ns: now_ns,
            },
        );
        while state.order.len() > capacity.max(1) {
            if let Some(oldest) = state.order.pop_front() {
                state.entries.remove(&oldest);
            }
        }
    }

    /// Drop every `(tenant, signal)` entry for `tenant` (ADR-0069 decision 2,
    /// idle-tenant state eviction). Returns the number of entries removed (one
    /// per signal held). A decoded HEAD is TTL-revalidated and re-read on a
    /// miss, so eviction only costs a re-read, never correctness.
    pub(crate) fn evict_tenant(&self, tenant: &TenantHash) -> usize {
        let mut state = self.state.lock();
        let before = state.entries.len();
        state.entries.retain(|(t, _), _| t != tenant);
        state.order.retain(|(t, _)| t != tenant);
        before - state.entries.len()
    }
}

#[derive(Default)]
struct PartTenantCache {
    entries: HashMap<String, (Arc<DecodedPart>, u64)>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl PartTenantCache {
    fn insert(&mut self, key: String, part: Arc<DecodedPart>, bytes: u64, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), (part, bytes));
        self.order.push_back(key);
        while self.order.len() > capacity.max(1) {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// Decoded snapshot-part cache, partitioned by tenant. Parts are
/// content-addressed and immutable, so entries never need invalidating,
/// only capacity-cap eviction (`snapshot_cache_parts`).
#[derive(Default)]
pub(crate) struct PartCache {
    tenants: Mutex<HashMap<TenantHash, PartTenantCache>>,
}

impl PartCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Option<Arc<DecodedPart>> {
        let hit = self
            .tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned());
        match hit {
            Some((part, bytes)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes);
                Some(part)
            }
            None => {
                accounting.record_cache_miss();
                None
            }
        }
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        part: Arc<DecodedPart>,
        bytes: u64,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, part, bytes, capacity);
    }

    /// Drop the whole per-tenant outer-map entry for `tenant` (ADR-0069
    /// decision 2). Returns whether an entry was present. Parts are
    /// content-addressed and immutable, so a later miss re-fetches and
    /// re-decodes.
    pub(crate) fn evict_tenant(&self, tenant: &TenantHash) -> bool {
        self.tenants.lock().remove(tenant).is_some()
    }
}

#[derive(Default)]
struct PostingsTenantCache {
    entries: HashMap<String, (Arc<DecodedPostings>, u64)>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl PostingsTenantCache {
    fn insert(&mut self, key: String, postings: Arc<DecodedPostings>, bytes: u64, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), (postings, bytes));
        self.order.push_back(key);
        while self.order.len() > capacity.max(1) {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// Decoded name-postings cache, partitioned by tenant. Postings objects are content-addressed
/// and immutable, so entries never need invalidating, only capacity-cap
/// eviction, mirroring [`PartCache`].
#[derive(Default)]
pub(crate) struct PostingsCache {
    tenants: Mutex<HashMap<TenantHash, PostingsTenantCache>>,
}

impl PostingsCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Option<Arc<DecodedPostings>> {
        let hit = self
            .tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned());
        match hit {
            Some((postings, bytes)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes);
                Some(postings)
            }
            None => {
                accounting.record_cache_miss();
                None
            }
        }
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        postings: Arc<DecodedPostings>,
        bytes: u64,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, postings, bytes, capacity);
    }

    /// Drop the whole per-tenant outer-map entry for `tenant` (ADR-0069
    /// decision 2). Returns whether an entry was present. Postings objects are
    /// content-addressed and immutable, so a later miss re-fetches and
    /// re-decodes.
    pub(crate) fn evict_tenant(&self, tenant: &TenantHash) -> bool {
        self.tenants.lock().remove(tenant).is_some()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_cache::CacheKey;
    use ravel_commit::keys;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend};
    use ravel_types::TimeRange;
    use ravel_types::accounting::AccountedOp;

    use super::*;
    use crate::{Catalog, CatalogConfig};

    fn record(tenant_hash: [u8; 16], shard: u32) -> CommitRecord {
        CommitRecord {
            format_version: 1,
            tenant_hash: tenant_hash.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as i32,
            shard,
            writer_id: uuid::Uuid::new_v4().to_string(),
            writer_epoch: 0,
            writer_seq: 0,
            object_key: String::new(),
            object_size: 0,
            content_hash: vec![0; 32],
            sample_count: 0,
            series_count: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
            declared_column_stats: Vec::new(),
        }
    }

    /// A commit record carrying `columns` declared column statistics, the shape
    /// whose size the entry-count bound could not see: each statistic is a
    /// `DeclaredColumnMinMax` with its own name, and neither
    /// proto/ravel/commit.proto nor `ravel_commit::record::validate` caps how
    /// many a record carries.
    fn record_with_declared_columns(
        tenant_hash: [u8; 16],
        shard: u32,
        columns: usize,
    ) -> CommitRecord {
        CommitRecord {
            declared_column_stats: (0..columns)
                .map(|i| ravel_proto::commit::v1::DeclaredColumnMinMax {
                    name: format!("attr.column_{i:04}"),
                    declared_type: 1,
                    min: None,
                    max: None,
                    null_count: 0,
                })
                .collect(),
            ..record(tenant_hash, shard)
        }
    }

    /// A budget wide enough that the entry cap is what binds, for the tests
    /// that are about the entry cap.
    const UNBOUNDED_BYTES: u64 = u64::MAX;

    #[test]
    fn miss_then_hit() {
        let cache = RecordCache::default();
        let tenant = TenantHash([1; 16]);
        let accounting = QueryAccounting::new();
        assert!(cache.get(&tenant, "k", &accounting).is_none());
        cache.insert(
            tenant,
            "k".to_string(),
            Arc::new(record([1; 16], 0)),
            42,
            10,
            UNBOUNDED_BYTES,
        );
        assert!(cache.get(&tenant, "k", &accounting).is_some());

        let snap = accounting.snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_bytes, 42, "bytes counted once, on the hit only");
    }

    #[test]
    fn capacity_cap_evicts_oldest() {
        let cache = RecordCache::default();
        let tenant = TenantHash([2; 16]);
        let accounting = QueryAccounting::new();
        for i in 0..5 {
            cache.insert(
                tenant,
                format!("k{i}"),
                Arc::new(record([2; 16], 0)),
                1,
                3,
                UNBOUNDED_BYTES,
            );
        }
        // Oldest two evicted, most recent three retained.
        assert!(cache.get(&tenant, "k0", &accounting).is_none());
        assert!(cache.get(&tenant, "k1", &accounting).is_none());
        assert!(cache.get(&tenant, "k2", &accounting).is_some());
        assert!(cache.get(&tenant, "k3", &accounting).is_some());
        assert!(cache.get(&tenant, "k4", &accounting).is_some());
    }

    /// The one case that separates LRU from insertion-order eviction: a read
    /// of the oldest entry protects it, and the next-oldest goes instead. A
    /// hot ingest hour queries keep returning to must survive a wider scan
    /// admitted after it (issue #783), which insertion-order eviction cannot
    /// give.
    #[test]
    fn lru_evicts_the_least_recently_used() {
        let cache = RecordCache::default();
        let tenant = TenantHash([16; 16]);
        let accounting = QueryAccounting::new();
        for i in 0..3 {
            cache.insert(
                tenant,
                format!("k{i}"),
                Arc::new(record([16; 16], 0)),
                1,
                3,
                UNBOUNDED_BYTES,
            );
        }
        // Read the oldest-inserted entry: now the least-recently-used is k1.
        assert!(cache.get(&tenant, "k0", &accounting).is_some());
        cache.insert(
            tenant,
            "k3".to_string(),
            Arc::new(record([16; 16], 0)),
            1,
            3,
            UNBOUNDED_BYTES,
        );

        assert!(
            cache.get(&tenant, "k0", &accounting).is_some(),
            "the re-read entry must survive; insertion-order eviction would have dropped it"
        );
        assert!(
            cache.get(&tenant, "k1", &accounting).is_none(),
            "the least-recently-used entry must be the one evicted"
        );
        assert!(cache.get(&tenant, "k2", &accounting).is_some());
        assert!(cache.get(&tenant, "k3", &accounting).is_some());
    }

    /// A re-insert of a key already held refreshes its recency rather than
    /// being ignored: the resolve's prewarm and include passes both go
    /// through `insert`/`get` for the same key, so an ignored re-insert would
    /// leave a record the resolve just used looking stale to eviction.
    #[test]
    fn reinsert_refreshes_recency() {
        let cache = RecordCache::default();
        let tenant = TenantHash([17; 16]);
        let accounting = QueryAccounting::new();
        for i in 0..3 {
            cache.insert(
                tenant,
                format!("k{i}"),
                Arc::new(record([17; 16], 0)),
                1,
                3,
                UNBOUNDED_BYTES,
            );
        }
        cache.insert(
            tenant,
            "k0".to_string(),
            Arc::new(record([17; 16], 0)),
            1,
            3,
            UNBOUNDED_BYTES,
        );
        cache.insert(
            tenant,
            "k3".to_string(),
            Arc::new(record([17; 16], 0)),
            1,
            3,
            UNBOUNDED_BYTES,
        );

        assert!(cache.get(&tenant, "k0", &accounting).is_some());
        assert!(cache.get(&tenant, "k1", &accounting).is_none());
    }

    /// `0` is the disabled sentinel: nothing is admitted, so every read is a
    /// miss and falls through to a store GET.
    #[test]
    fn zero_capacity_admits_nothing() {
        let cache = RecordCache::default();
        let tenant = TenantHash([18; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(
            tenant,
            "k".to_string(),
            Arc::new(record([18; 16], 0)),
            1,
            0,
            0,
        );
        assert!(cache.get(&tenant, "k", &accounting).is_none());
    }

    #[test]
    fn tenants_are_isolated() {
        let cache = RecordCache::default();
        let a = TenantHash([3; 16]);
        let b = TenantHash([4; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(
            a,
            "k".to_string(),
            Arc::new(record([3; 16], 0)),
            1,
            10,
            UNBOUNDED_BYTES,
        );
        assert!(cache.get(&a, "k", &accounting).is_some());
        assert!(cache.get(&b, "k", &accounting).is_none());
    }

    /// `RECORD_CACHE_ENTRY_BYTES` is the planning rate the derived capacity is
    /// sized against, so it has to bound what an ordinary entry actually
    /// charges. Pins the 863-byte breakdown
    /// `DEFAULT_CACHE_CAPACITY_PER_TENANT`'s documentation states, against real
    /// keys rather than round numbers: a 119-byte commit key held twice, the
    /// fixed struct cost, and the record's own heap (16-byte tenant hash,
    /// 36-byte writer uuid, 125-byte data object key, 32-byte content hash).
    #[test]
    fn the_per_entry_planning_figure_bounds_an_ordinary_records_charge() {
        let tenant = TenantHash([25; 16]);
        let writer_id = uuid::Uuid::from_u128(1);
        let content_hash = [7u8; 32];
        let object_key = keys::data_key(
            &tenant,
            Signal::Metrics,
            0,
            writer_id,
            1,
            0,
            &content_hash,
        )
        .expect("data key");
        let key = keys::commit_key(&tenant, Signal::Metrics, 0, 0, writer_id, 1, 0)
            .expect("commit key");
        assert_eq!(key.len(), 119, "the commit key the breakdown is stated for");
        assert_eq!(
            object_key.len(),
            125,
            "the data object key the breakdown is stated for"
        );

        let ordinary = CommitRecord {
            writer_id: writer_id.to_string(),
            object_key,
            content_hash: content_hash.to_vec(),
            ..record([25; 16], 0)
        };
        let charge = commit_entry_resident_bytes(&key, &ordinary);
        assert_eq!(
            charge,
            COMMIT_ENTRY_FIXED_BYTES + 2 * 119 + 16 + 36 + 125 + 32,
            "the charge is the sum of the entry's parts, not a constant"
        );
        assert_eq!(charge, 863);
        assert!(
            charge <= crate::config::RECORD_CACHE_ENTRY_BYTES,
            "the planning rate ({}) must bound an ordinary entry's charge ({charge}), or the \
             derived capacity would promise residency the byte budget cannot give",
            crate::config::RECORD_CACHE_ENTRY_BYTES
        );
    }

    /// The charge is the sum of what the entry actually holds, so a record
    /// carrying declared column statistics is charged for them. Pins the
    /// arithmetic: a flat per-entry constant would make all three of these
    /// equal.
    #[test]
    fn commit_entry_charge_counts_the_declared_column_list() {
        let key = "t/ab/m/c/0000/0/w.1.0.cmt";
        let base = record_with_declared_columns([26; 16], 0, 0);
        let empty = commit_entry_resident_bytes(key, &base);
        assert_eq!(
            empty,
            COMMIT_ENTRY_FIXED_BYTES + 2 * key.len() as u64 + 16 + 36 + 32,
            "a stats-free record is the fixed cost, the key twice, and its own heap"
        );

        // Every generated name is `attr.column_NNNN`, 16 characters.
        let per_stat = DECLARED_COLUMN_STAT_FIXED_BYTES + 16;
        for columns in [1usize, 100, 200] {
            let with = record_with_declared_columns([26; 16], 0, columns);
            assert_eq!(
                commit_entry_resident_bytes(key, &with),
                empty + columns as u64 * per_stat,
                "{columns} declared columns must be charged {columns} times over"
            );
        }
        assert_eq!(
            200 * per_stat,
            19_200,
            "a record with 200 declared columns carries about 19 KB of statistics alone, not \
             the 900 bytes the planning rate assumes"
        );
    }

    /// Issue #1735 fix round. The commit-record cache is bounded in BYTES, not
    /// in entries: a few dozen records whose declared-column lists make each of
    /// them more than twenty times the planning rate must evict down to the
    /// budget even though their count stays far below the entry capacity.
    ///
    /// Rules out two wrong implementations. An eviction that compares the entry
    /// COUNT against a byte-derived cap leaves all sixty resident (60 << 1,000)
    /// and the assertion on `charged_bytes` fails at sixty times the per-entry
    /// charge, far over the budget; the assertion is on summed bytes rather
    /// than on length for exactly that reason. A charge that is the flat
    /// `RECORD_CACHE_ENTRY_BYTES` constant instead of a measurement of
    /// `declared_column_stats` also leaves all sixty resident (60 * 900 is
    /// inside a 900,000-byte budget), and fails the proportionality block
    /// below, which pins three different declared-column counts to three
    /// charges that differ by exactly the per-statistic cost.
    #[test]
    fn commit_records_with_large_declared_column_lists_evict_on_bytes() {
        let cache = RecordCache::default();
        let tenant = TenantHash([27; 16]);
        let capacity = 1_000;
        let budget = capacity as u64 * crate::config::RECORD_CACHE_ENTRY_BYTES;
        assert_eq!(budget, 900_000);

        let record = Arc::new(record_with_declared_columns([27; 16], 0, 200));
        let charge = commit_entry_resident_bytes("k0", &record);
        assert!(
            charge > 19_000,
            "sanity: one such record is charged {charge} bytes, more than twenty times the 900 \
             an entry cap assumes"
        );
        let expected_resident = (budget / charge) as usize;
        assert_eq!(
            expected_resident, 45,
            "sanity: the budget holds exactly forty-five of these"
        );

        for i in 0..60 {
            cache.insert(
                tenant,
                format!("k{i}"),
                record.clone(),
                1,
                capacity,
                budget,
            );
            assert!(
                cache.charged_bytes(&tenant) <= budget,
                "the charged total must stay inside the budget after every insert"
            );
        }

        assert_eq!(
            cache.charged_bytes(&tenant),
            expected_resident as u64 * charge,
            "residency is pinned to exactly the entries the budget holds, not to \
             'something was evicted'"
        );
        assert_eq!(cache.entry_count(&tenant), expected_resident);
        assert!(
            60 < capacity,
            "the entry cap is never reached here: only the byte bound can have evicted"
        );

        // Least-recently-used first: the fifteen earliest keys are gone.
        let accounting = QueryAccounting::new();
        for i in 0..15 {
            assert!(
                cache.get(&tenant, &format!("k{i}"), &accounting).is_none(),
                "k{i} must have been evicted"
            );
        }
        for i in 15..60 {
            assert!(
                cache.get(&tenant, &format!("k{i}"), &accounting).is_some(),
                "k{i} must still be resident"
            );
        }

        // The charge is measured per entry, not applied as one flat constant:
        // records with different declared-column counts charge proportionally
        // different amounts.
        let none = commit_entry_resident_bytes("k", &record_with_declared_columns([27; 16], 0, 0));
        let half =
            commit_entry_resident_bytes("k", &record_with_declared_columns([27; 16], 0, 100));
        let full =
            commit_entry_resident_bytes("k", &record_with_declared_columns([27; 16], 0, 200));
        let per_stat = DECLARED_COLUMN_STAT_FIXED_BYTES + 16;
        assert_eq!(half - none, 100 * per_stat);
        assert_eq!(full - none, 200 * per_stat);
        assert_eq!(
            full - none,
            2 * (half - none),
            "twice the declared columns is twice the declared-column charge"
        );
    }

    /// The commit cache's per-tenant budget is the derived capacity times the
    /// per-entry planning rate, the figure docs/catalog-and-mvcc.md,
    /// docs/guides/caching.md and docs/guides/operations.md quote: 22.5 MB at
    /// the cap and 9 MB at the floor `--disable-cache` holds.
    #[test]
    fn the_commit_budget_is_the_stated_per_cache_share() {
        let at_cap = CatalogConfig {
            cache_capacity_per_tenant: crate::config::MAX_CACHE_CAPACITY_PER_TENANT,
            ..Default::default()
        };
        assert_eq!(at_cap.commit_cache_max_bytes_per_tenant(), 22_500_000);
        assert_eq!(
            at_cap.commit_cache_max_bytes_per_tenant(),
            at_cap.compaction_cache_max_bytes_per_tenant(),
            "the per-tenant budget is split equally between the two record caches"
        );
        assert_eq!(
            at_cap.commit_cache_max_bytes_per_tenant() * crate::config::RECORD_CACHES_PER_TENANT,
            crate::config::MAX_RECORD_CACHE_BYTES_PER_TENANT,
            "both caches together are the stated 45 MB"
        );

        let at_floor = CatalogConfig {
            cache_capacity_per_tenant: crate::config::DEFAULT_CACHE_CAPACITY_PER_TENANT,
            ..Default::default()
        };
        assert_eq!(at_floor.commit_cache_max_bytes_per_tenant(), 9_000_000);
    }

    /// A tenant whose commit records carry no declared columns is still bounded
    /// by the entry capacity, so the byte bound replaces nothing: both hold.
    #[test]
    fn small_commit_entries_still_evict_on_the_entry_cap() {
        let cache = RecordCache::default();
        let tenant = TenantHash([28; 16]);
        let record = Arc::new(record_with_declared_columns([28; 16], 0, 0));
        let charge = commit_entry_resident_bytes("k0", &record);
        let budget = 10_000 * charge;

        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), record.clone(), 1, 3, budget);
        }

        assert_eq!(
            cache.entry_count(&tenant),
            3,
            "the entry cap binds when the entries are small"
        );
        assert_eq!(cache.charged_bytes(&tenant), 3 * charge);
    }

    /// Tombstone-observation invalidation drops the commit cache's charge with
    /// the entries, so a later insert is not evicted against bytes that are no
    /// longer held.
    #[test]
    fn commit_invalidate_prefix_releases_the_charged_bytes() {
        let cache = RecordCache::default();
        let tenant = TenantHash([29; 16]);
        let record = Arc::new(record_with_declared_columns([29; 16], 0, 10));
        let budget = 10_000_000;
        cache.insert(
            tenant,
            "m/c/0/1/a.cmt".to_string(),
            record.clone(),
            1,
            100,
            budget,
        );
        cache.insert(
            tenant,
            "m/c/0/2/b.cmt".to_string(),
            record.clone(),
            1,
            100,
            budget,
        );
        let both = cache.charged_bytes(&tenant);

        cache.invalidate_prefix(&tenant, "m/c/0/1/");

        assert_eq!(cache.entry_count(&tenant), 1);
        assert_eq!(
            cache.charged_bytes(&tenant),
            commit_entry_resident_bytes("m/c/0/2/b.cmt", &record),
            "the dropped entry's charge is released, not left on the tenant's total"
        );
        assert!(cache.charged_bytes(&tenant) < both);
    }

    /// A compaction record over `inputs` L0 segments, the shape whose size the
    /// entry-count bound could not see: each input is a 36-character uuid
    /// string plus two integers, and nothing in the format or in
    /// `validate_compaction` caps how many a record carries.
    fn compaction_record(tenant_hash: [u8; 16], inputs: usize) -> CompactionRecord {
        CompactionRecord {
            format_version: 1,
            tenant_hash: tenant_hash.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as i32,
            shard: 0,
            ingest_hour_bucket: 0,
            level: 1,
            inputs: (0..inputs)
                .map(|i| ravel_proto::commit::v1::CompactionInputIdentity {
                    writer_id: uuid::Uuid::from_u128(i as u128).to_string(),
                    writer_epoch: 1,
                    writer_seq: i as u64,
                })
                .collect(),
            input_set_hash: vec![0; 32],
            parts: Vec::new(),
            created_unix_ns: 0,
        }
    }

    /// The charge is the sum of what the entry actually holds, so a record
    /// with an input list is charged for it. Pins the arithmetic:
    /// fixed overhead, the key held twice, the two byte vectors, and per
    /// input its struct plus its 36-character uuid.
    #[test]
    fn compaction_entry_charge_counts_the_input_list() {
        let key = "m/c/0/0/abc.cmp";
        let empty = compaction_entry_resident_bytes(key, &compaction_record([20; 16], 0));
        assert_eq!(
            empty,
            COMPACTION_ENTRY_FIXED_BYTES + 2 * key.len() as u64 + 16 + 32
        );
        let per_input = COMPACTION_INPUT_FIXED_BYTES + 36;
        assert_eq!(
            compaction_entry_resident_bytes(key, &compaction_record([20; 16], 1_800)),
            empty + 1_800 * per_input
        );
        assert_eq!(
            per_input * 1_800,
            136_800,
            "one L1 record over a shard-hour of 1,800 L0 segments is charged about 137 KB, \
             not the 900 bytes an entry-count bound assumes"
        );
    }

    /// Issue #1735 fix round. The compaction cache is bounded in BYTES, not in
    /// entries: a handful of records whose input lists make each of them
    /// hundreds of times the assumed per-entry cost must evict down to the
    /// budget even though their count stays far below the entry capacity.
    ///
    /// Rules out two wrong implementations. An eviction that compares the
    /// entry COUNT against a byte-derived cap leaves all eight resident
    /// (8 << 1,000), so the charged total lands at eight times the per-entry
    /// charge, well over the budget. An eviction applied to the commit-record
    /// cache only leaves this cache on its entry cap, with the same result.
    /// Both fail the exact assertions below, which are on charged bytes.
    #[test]
    fn oversized_compaction_entries_evict_on_bytes_not_count() {
        let cache = CompactionRecordCache::default();
        let tenant = TenantHash([21; 16]);
        let capacity = 1_000;
        let budget = capacity as u64 * crate::config::RECORD_CACHE_ENTRY_BYTES;
        assert_eq!(budget, 900_000);

        let record = Arc::new(compaction_record([21; 16], 1_800));
        let charge = compaction_entry_resident_bytes("c0", &record);
        assert!(
            charge > 100_000,
            "sanity: one such record is charged {charge} bytes, far above the 900 an entry \
             cap assumes"
        );
        let expected_resident = (budget / charge) as usize;
        assert_eq!(
            expected_resident, 6,
            "sanity: the budget holds exactly six of these"
        );

        for i in 0..8 {
            cache.insert(tenant, format!("c{i}"), record.clone(), 1, capacity, budget);
            assert!(
                cache.charged_bytes(&tenant) <= budget,
                "the charged total must stay inside the budget after every insert"
            );
        }

        assert_eq!(
            cache.charged_bytes(&tenant),
            expected_resident as u64 * charge,
            "residency is pinned to exactly the entries the budget holds, not to \
             'something was evicted'"
        );
        assert_eq!(cache.entry_count(&tenant), expected_resident);
        assert!(
            8 < capacity,
            "the entry cap is never reached here: only the byte bound can have evicted"
        );

        // Oldest-first: the two earliest keys are gone, the six newest stay.
        let accounting = QueryAccounting::new();
        for i in 0..2 {
            assert!(
                cache.get(&tenant, &format!("c{i}"), &accounting).is_none(),
                "c{i} must have been evicted"
            );
        }
        for i in 2..8 {
            assert!(
                cache.get(&tenant, &format!("c{i}"), &accounting).is_some(),
                "c{i} must still be resident"
            );
        }
    }

    /// A tenant whose compaction records are small is still bounded by the
    /// entry capacity, so the byte bound replaces nothing: both hold.
    #[test]
    fn small_compaction_entries_still_evict_on_the_entry_cap() {
        let cache = CompactionRecordCache::default();
        let tenant = TenantHash([22; 16]);
        let record = Arc::new(compaction_record([22; 16], 0));
        let charge = compaction_entry_resident_bytes("c0", &record);
        let budget = 10_000 * charge;

        for i in 0..5 {
            cache.insert(tenant, format!("c{i}"), record.clone(), 1, 3, budget);
        }

        assert_eq!(
            cache.entry_count(&tenant),
            3,
            "the entry cap binds when the entries are small"
        );
        assert_eq!(cache.charged_bytes(&tenant), 3 * charge);
    }

    /// `cache_capacity_per_tenant == 0` resolves to a `0` budget, and nothing
    /// stays resident under it.
    #[test]
    fn a_zero_budget_holds_no_compaction_entry() {
        let cache = CompactionRecordCache::default();
        let tenant = TenantHash([23; 16]);
        let accounting = QueryAccounting::new();
        let config = CatalogConfig {
            cache_capacity_per_tenant: 0,
            ..Default::default()
        };
        assert_eq!(config.compaction_cache_max_bytes_per_tenant(), 0);

        cache.insert(
            tenant,
            "c0".to_string(),
            Arc::new(compaction_record([23; 16], 0)),
            1,
            config.cache_capacity_per_tenant,
            config.compaction_cache_max_bytes_per_tenant(),
        );

        assert_eq!(cache.charged_bytes(&tenant), 0);
        assert!(cache.get(&tenant, "c0", &accounting).is_none());
    }

    /// Tombstone-observation invalidation drops the charge with the entries,
    /// so a later insert is not evicted against bytes that are no longer held.
    #[test]
    fn invalidate_prefix_releases_the_charged_bytes() {
        let cache = CompactionRecordCache::default();
        let tenant = TenantHash([24; 16]);
        let record = Arc::new(compaction_record([24; 16], 10));
        let budget = 10_000_000;
        cache.insert(
            tenant,
            "m/c/0/1/a.cmp".to_string(),
            record.clone(),
            1,
            100,
            budget,
        );
        cache.insert(
            tenant,
            "m/c/0/2/b.cmp".to_string(),
            record.clone(),
            1,
            100,
            budget,
        );
        let both = cache.charged_bytes(&tenant);

        cache.invalidate_prefix(&tenant, "m/c/0/1/");

        assert_eq!(cache.entry_count(&tenant), 1);
        assert_eq!(
            cache.charged_bytes(&tenant),
            compaction_entry_resident_bytes("m/c/0/2/b.cmp", &record),
            "the dropped entry's charge is released, not left on the tenant's total"
        );
        assert!(cache.charged_bytes(&tenant) < both);
    }

    /// The per-tenant budget is the derived capacity times the per-entry cost,
    /// the figure docs/guides/caching.md and docs/guides/operations.md quote:
    /// 22.5 MB at the cap and 9 MB at the floor `--disable-cache` holds.
    #[test]
    fn the_compaction_budget_is_the_stated_per_cache_share() {
        let at_cap = CatalogConfig {
            cache_capacity_per_tenant: crate::config::MAX_CACHE_CAPACITY_PER_TENANT,
            ..Default::default()
        };
        assert_eq!(at_cap.compaction_cache_max_bytes_per_tenant(), 22_500_000);
        assert_eq!(
            at_cap.compaction_cache_max_bytes_per_tenant()
                * crate::config::RECORD_CACHES_PER_TENANT,
            crate::config::MAX_RECORD_CACHE_BYTES_PER_TENANT,
            "both caches together are the stated 45 MB"
        );

        let at_floor = CatalogConfig {
            cache_capacity_per_tenant: crate::config::DEFAULT_CACHE_CAPACITY_PER_TENANT,
            ..Default::default()
        };
        assert_eq!(at_floor.compaction_cache_max_bytes_per_tenant(), 9_000_000);
    }

    fn head(tenant_hash: [u8; 16], watermark_hour: u32) -> SnapshotHead {
        SnapshotHead {
            format_version: 1,
            tenant_hash: tenant_hash.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as u32,
            shard_count: 1,
            watermark_hour,
            parts: vec![],
            postings: None,
            folder_id: uuid::Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            shard_generation_count: 1,
        }
    }

    #[test]
    fn head_cache_miss_then_hit() {
        let cache = HeadCache::default();
        let tenant = TenantHash([5; 16]);
        let accounting = QueryAccounting::new();
        assert!(
            cache
                .get(&tenant, Signal::Metrics, 1_000, 500, &accounting)
                .is_none()
        );
        cache.insert(
            tenant,
            Signal::Metrics,
            Arc::new(head([5; 16], 10)),
            77,
            1_000,
            10,
        );
        let cached = cache
            .get(&tenant, Signal::Metrics, 1_000, 500, &accounting)
            .expect("hit");
        assert_eq!(cached.watermark_hour, 10);

        let snap = accounting.snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_bytes, 77);
    }

    #[test]
    fn head_cache_expires_after_ttl() {
        let cache = HeadCache::default();
        let tenant = TenantHash([6; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(
            tenant,
            Signal::Metrics,
            Arc::new(head([6; 16], 1)),
            1,
            0,
            10,
        );
        assert!(
            cache
                .get(&tenant, Signal::Metrics, 500, 500, &accounting)
                .is_some()
        );
        assert!(
            cache
                .get(&tenant, Signal::Metrics, 501, 500, &accounting)
                .is_none()
        );
    }

    #[test]
    fn head_cache_is_keyed_by_signal_too() {
        let cache = HeadCache::default();
        let tenant = TenantHash([7; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(
            tenant,
            Signal::Metrics,
            Arc::new(head([7; 16], 1)),
            1,
            0,
            10,
        );
        assert!(
            cache
                .get(&tenant, Signal::Logs, 0, 500, &accounting)
                .is_none()
        );
    }

    /// `HeadCache` holds a bounded number of (tenant, signal) entries.
    /// Inserting more tenants than the
    /// configured cap must evict the oldest rather than growing without
    /// limit.
    #[test]
    fn head_cache_capacity_bound_evicts_oldest_tenant() {
        let cache = HeadCache::default();
        let accounting = QueryAccounting::new();
        for i in 0..5u8 {
            let tenant = TenantHash([i; 16]);
            cache.insert(
                tenant,
                Signal::Metrics,
                Arc::new(head([i; 16], u32::from(i))),
                1,
                0,
                3,
            );
        }
        for i in 0..2u8 {
            let tenant = TenantHash([i; 16]);
            assert!(
                cache
                    .get(&tenant, Signal::Metrics, 0, 500, &accounting)
                    .is_none(),
                "tenant {i} should have been evicted"
            );
        }
        for i in 2..5u8 {
            let tenant = TenantHash([i; 16]);
            assert!(
                cache
                    .get(&tenant, Signal::Metrics, 0, 500, &accounting)
                    .is_some(),
                "tenant {i} should still be cached"
            );
        }
    }

    fn decoded_part(watermark_hour: u32) -> DecodedPart {
        DecodedPart {
            header: ravel_proto::catalog::v1::SnapshotPartHeader {
                format_version: 1,
                tenant_hash: vec![0; 16],
                signal: ravel_proto::commit::v1::Signal::Metrics as u32,
                shard_count: 1,
                watermark_hour,
                entry_count: 0,
                entries_uncompressed_len: 0,
                min_hour: 0,
            },
            entries: vec![],
        }
    }

    #[test]
    fn part_cache_miss_then_hit() {
        let cache = PartCache::default();
        let tenant = TenantHash([8; 16]);
        let accounting = QueryAccounting::new();
        assert!(cache.get(&tenant, "k", &accounting).is_none());
        cache.insert(tenant, "k".to_string(), Arc::new(decoded_part(1)), 9, 10);
        assert!(cache.get(&tenant, "k", &accounting).is_some());

        let snap = accounting.snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_bytes, 9);
    }

    #[test]
    fn part_cache_capacity_cap_evicts_oldest() {
        let cache = PartCache::default();
        let tenant = TenantHash([9; 16]);
        let accounting = QueryAccounting::new();
        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), Arc::new(decoded_part(1)), 1, 3);
        }
        assert!(cache.get(&tenant, "k0", &accounting).is_none());
        assert!(cache.get(&tenant, "k1", &accounting).is_none());
        assert!(cache.get(&tenant, "k2", &accounting).is_some());
        assert!(cache.get(&tenant, "k3", &accounting).is_some());
        assert!(cache.get(&tenant, "k4", &accounting).is_some());
    }

    #[test]
    fn part_cache_tenants_are_isolated() {
        let cache = PartCache::default();
        let a = TenantHash([10; 16]);
        let b = TenantHash([11; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(a, "k".to_string(), Arc::new(decoded_part(1)), 1, 10);
        assert!(cache.get(&a, "k", &accounting).is_some());
        assert!(cache.get(&b, "k", &accounting).is_none());
    }

    fn decoded_postings() -> DecodedPostings {
        DecodedPostings {
            header: ravel_proto::catalog::v1::SnapshotPostingsHeader {
                format_version: 1,
                tenant_hash: vec![0; 16],
                signal: ravel_proto::commit::v1::Signal::Metrics as u32,
                part_blake3: vec![vec![0; 32]],
                entry_count: 0,
                name_count: 0,
                body_uncompressed_len: 0,
            },
            names: vec![],
        }
    }

    #[test]
    fn postings_cache_miss_then_hit() {
        let cache = PostingsCache::default();
        let tenant = TenantHash([12; 16]);
        let accounting = QueryAccounting::new();
        assert!(cache.get(&tenant, "k", &accounting).is_none());
        cache.insert(
            tenant,
            "k".to_string(),
            Arc::new(decoded_postings()),
            13,
            10,
        );
        assert!(cache.get(&tenant, "k", &accounting).is_some());

        let snap = accounting.snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_bytes, 13);
    }

    #[test]
    fn postings_cache_capacity_cap_evicts_oldest() {
        let cache = PostingsCache::default();
        let tenant = TenantHash([13; 16]);
        let accounting = QueryAccounting::new();
        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), Arc::new(decoded_postings()), 1, 3);
        }
        assert!(cache.get(&tenant, "k0", &accounting).is_none());
        assert!(cache.get(&tenant, "k1", &accounting).is_none());
        assert!(cache.get(&tenant, "k2", &accounting).is_some());
        assert!(cache.get(&tenant, "k3", &accounting).is_some());
        assert!(cache.get(&tenant, "k4", &accounting).is_some());
    }

    #[test]
    fn postings_cache_tenants_are_isolated() {
        let cache = PostingsCache::default();
        let a = TenantHash([14; 16]);
        let b = TenantHash([15; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(a, "k".to_string(), Arc::new(decoded_postings()), 1, 10);
        assert!(cache.get(&a, "k", &accounting).is_some());
        assert!(cache.get(&b, "k", &accounting).is_none());
    }

    // -----------------------------------------------------------------
    // Byte cache (ADR-0046): Catalog::fetch_content_addressed,
    // consulted from load_one_part/load_snapshot_postings ahead of the
    // decoded PartCache/PostingsCache above.
    // -----------------------------------------------------------------

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const FOLD_MARGIN_NS: i64 = crate::DEFAULT_MAX_FLUSH_LIFETIME_NS
        + crate::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + crate::DEFAULT_FOLD_SAFETY_MARGIN_NS;

    fn byte_cache_tenant() -> TenantHash {
        TenantHash([0x42; 16])
    }

    fn byte_cache_catalog_config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// `now_ns` at which ingest hour `hour` has just sealed under default
    /// margins (mirrors `fold.rs`'s own `now_at_seal`, not reachable from
    /// here since it is private to that module's test mod).
    fn now_at_seal(hour: u32) -> i64 {
        (i64::from(hour) + 1) * NS_PER_HOUR + FOLD_MARGIN_NS
    }

    /// Build, PUT the data object, and publish a fully self-consistent
    /// commit record for one L0 segment, so `Catalog::fold` has something
    /// to compact into a real, content-addressed snapshot part.
    async fn publish_one_segment(
        store: &MemoryStore,
        hour: u32,
        created_unix_ns: i64,
    ) -> CommitRecord {
        let writer_id = uuid::Uuid::new_v4();
        let payload = format!("byte-cache-seg-{writer_id}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let record = record::build(NewCommitRecord {
            tenant_hash: byte_cache_tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket: hour,
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

    /// Find the one snapshot part object a fold produced, by its key
    /// convention (`fold::part_object_key`'s `/snap/` segment; not
    /// reachable directly since it is private to that module).
    async fn find_part_key(store: &MemoryStore) -> String {
        ravel_object_store::list_all(store, "t/")
            .await
            .expect("list")
            .into_iter()
            .map(|meta| meta.key)
            .find(|key| key.contains("/snap/"))
            .expect("fold produced a snapshot part")
    }

    #[tokio::test]
    async fn resolve_is_byte_identical_with_and_without_the_byte_cache() {
        let store = Arc::new(MemoryStore::new());
        let hour = 900_000u32;
        let now_ns = now_at_seal(hour);
        publish_one_segment(&store, hour, now_ns - NS_PER_HOUR).await;

        let fold_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        fold_catalog
            .fold(
                &byte_cache_tenant(),
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        let catalog = Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };

        let cold = catalog
            .resolve(&byte_cache_tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("cold resolve");
        assert_eq!(cold.segments.len(), 1);
        assert!(
            !catalog.byte_cache().is_empty(),
            "cold resolve must have admitted the part into the byte cache"
        );

        let warm = catalog
            .resolve(&byte_cache_tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("warm resolve");

        assert_eq!(
            cold, warm,
            "resolve's contract is unchanged: byte-identical, including segment ordering, \
             cached or not"
        );
    }

    /// ADR-0046: the catalog HEAD is CAS-written and must never be admitted
    /// to the byte cache. Structurally, `read_head` never has a
    /// `content_hash` to pass `Catalog::fetch_content_addressed` (see that
    /// method's doc comment) so it cannot reach the byte cache at all; this
    /// is the runtime regression backstop for that argument. Every resolve
    /// below re-reads (or TTL-revalidates) HEAD through the same
    /// `guarded_get` funnel the byte cache sits behind; if HEAD bytes ever
    /// reached the byte cache, its entry count would grow past the single
    /// content-addressed part across repeated resolves.
    #[tokio::test]
    async fn head_reads_never_populate_the_byte_cache() {
        let store = Arc::new(MemoryStore::new());
        let hour = 900_001u32;
        let now_ns = now_at_seal(hour);
        publish_one_segment(&store, hour, now_ns - NS_PER_HOUR).await;

        let fold_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        fold_catalog
            .fold(
                &byte_cache_tenant(),
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        let catalog = Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };

        for _ in 0..3 {
            catalog
                .resolve(&byte_cache_tenant(), Signal::Metrics, range, &[], now_ns)
                .await
                .expect("resolve");
        }

        assert_eq!(
            catalog.byte_cache().len(),
            1,
            "only the content-addressed part may ever be admitted; HEAD is fetched on the \
             path above but must never appear in the byte cache"
        );
    }

    #[tokio::test]
    async fn corrupt_byte_cache_entry_falls_back_to_listing_like_a_corrupt_store_read() {
        let store = Arc::new(MemoryStore::new());
        let hour = 900_002u32;
        let now_ns = now_at_seal(hour);
        let record = publish_one_segment(&store, hour, now_ns - NS_PER_HOUR).await;

        let fold_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        fold_catalog
            .fold(
                &byte_cache_tenant(),
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        let part_key = find_part_key(&store).await;
        let good_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let content_hash = *blake3::hash(&good_bytes).as_bytes();
        let cache_key = CacheKey::new(
            byte_cache_tenant().0,
            content_hash,
            0,
            good_bytes.len() as u64,
        );
        let mut corrupted = good_bytes.to_vec();
        corrupted[0] ^= 0xFF;

        // A fresh catalog: empty decoded PartCache, so load_one_part must
        // consult the byte cache; seeded directly with a corrupt entry at
        // the part's real cache key, bypassing a real store GET entirely.
        let catalog = Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        catalog
            .byte_cache()
            .insert(cache_key, Bytes::from(corrupted));

        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };
        let snapshot = catalog
            .resolve(&byte_cache_tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("resolve degrades to listing rather than failing or returning wrong data");

        assert_eq!(
            snapshot.segments.len(),
            1,
            "listing fallback must still find the one published segment"
        );
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("data key")
        );
        assert!(
            catalog
                .part_cache()
                .get(&byte_cache_tenant(), &part_key, &QueryAccounting::new())
                .is_none(),
            "corrupted bytes must never be promoted into the decoded part cache"
        );
    }

    #[tokio::test]
    async fn byte_cache_hit_is_counted_and_does_not_also_count_an_s3_request() {
        let store = Arc::new(MemoryStore::new());
        let hour = 900_003u32;
        let now_ns = now_at_seal(hour);
        publish_one_segment(&store, hour, now_ns - NS_PER_HOUR).await;

        let fold_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        fold_catalog
            .fold(
                &byte_cache_tenant(),
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        let part_key = find_part_key(&store).await;
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let content_hash = *blake3::hash(&part_bytes).as_bytes();
        let cache_key = CacheKey::new(
            byte_cache_tenant().0,
            content_hash,
            0,
            part_bytes.len() as u64,
        );

        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };

        // Cold: fresh catalog, byte cache empty. HEAD and the part both
        // come from the store.
        let cold_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        let cold_accounting = QueryAccounting::new();
        cold_catalog
            .resolve_with_accounting(
                &byte_cache_tenant(),
                Signal::Metrics,
                range,
                &[],
                now_ns,
                &cold_accounting,
            )
            .await
            .expect("cold resolve");
        let cold_snap = cold_accounting.snapshot();

        // Warm: another fresh catalog (empty decoded caches too, so this
        // exercises the byte cache specifically, not PartCache), with the
        // byte cache pre-seeded with the part's real bytes under its real
        // key.
        let warm_catalog =
            Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        warm_catalog.byte_cache().insert(cache_key, part_bytes);
        let warm_accounting = QueryAccounting::new();
        let warm_snapshot = warm_catalog
            .resolve_with_accounting(
                &byte_cache_tenant(),
                Signal::Metrics,
                range,
                &[],
                now_ns,
                &warm_accounting,
            )
            .await
            .expect("warm resolve");
        let warm_snap = warm_accounting.snapshot();

        assert_eq!(warm_snapshot.segments.len(), 1);
        assert_eq!(
            warm_snap.cache_hits,
            cold_snap.cache_hits + 1,
            "the byte cache hit must be counted"
        );
        assert_eq!(
            warm_snap.s3_requests(AccountedOp::Get),
            cold_snap.s3_requests(AccountedOp::Get) - 1,
            "a byte cache hit must skip the store GET the cold path made for the part"
        );
    }

    /// `byte_cache_max_bytes == 0` (the config `--disable-cache`
    /// resolves to) builds a catalog with no byte cache constructed at all,
    /// not a zero-capacity one. Asserts on the absence of the cache handle,
    /// the same style as the fetcher cache's "--disable-cache leaves no cache
    /// constructed" invariant, rather than on a zero hit count. A resolve over
    /// such a catalog still returns the published segment: the byte cache is
    /// an optimization only, so its absence never changes a result.
    #[tokio::test]
    async fn disabled_byte_cache_config_constructs_no_byte_cache() {
        let store = Arc::new(MemoryStore::new());
        let hour = 900_004u32;
        let now_ns = now_at_seal(hour);
        publish_one_segment(&store, hour, now_ns - NS_PER_HOUR).await;

        let disabled_config = CatalogConfig {
            byte_cache_max_bytes: 0,
            ..byte_cache_catalog_config(1)
        };

        let fold_catalog = Catalog::new(store.clone(), disabled_config).expect("catalog");
        fold_catalog
            .fold(
                &byte_cache_tenant(),
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a snapshot part");

        let catalog = Catalog::new(store.clone(), disabled_config).expect("catalog");
        assert!(
            catalog.byte_cache_metrics().is_none(),
            "byte_cache_max_bytes == 0 must construct no byte cache, so there is no \
             counters handle to expose"
        );

        // An enabled catalog over the same store must, by contrast, expose the
        // handle: the assertion above is proving a disable, not a permanently
        // absent feature.
        let enabled = Catalog::new(store.clone(), byte_cache_catalog_config(1)).expect("catalog");
        assert!(
            enabled.byte_cache_metrics().is_some(),
            "a non-zero byte_cache_max_bytes must construct the byte cache and expose \
             its counters handle"
        );

        let range = TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: now_ns,
        };
        let snapshot = catalog
            .resolve(&byte_cache_tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("resolve succeeds with the byte cache disabled");
        assert_eq!(
            snapshot.segments.len(),
            1,
            "the resolve must still find the one published segment with no byte cache"
        );
    }
}
