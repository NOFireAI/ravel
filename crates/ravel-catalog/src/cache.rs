//! Bounded per-tenant cache of decoded commit records, keyed by full object
//! key (docs/catalog-and-mvcc.md step 2, ADR-0010 §10).
//!
//! Capacity-cap eviction (FIFO): the simpler of the two strategies the ADR
//! explicitly allows ("simple LRU or capacity cap per tenant"). Commit
//! records are immutable once published (Phase 1 has no deletion), so
//! eviction only costs a re-GET+decode on the next miss, never
//! correctness. Field validation against the expected (tenant, signal,
//! shard) happens in the caller on every hit and every fresh decode, not
//! here: the cache only stores and evicts.
//!
//! Every `get()` below also records a hit or a miss, and a hit's stored byte
//! size, into the caller's [`QueryAccounting`] (ADR-0044, issue #421): the
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

#[derive(Default)]
struct TenantCache {
    entries: HashMap<String, (Arc<CommitRecord>, u64)>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl TenantCache {
    fn insert(&mut self, key: String, record: Arc<CommitRecord>, bytes: u64, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), (record, bytes));
        self.order.push_back(key);
        while self.order.len() > capacity.max(1) {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// Decoded-record cache, partitioned by tenant.
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
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned());
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
        record: Arc<CommitRecord>,
        bytes: u64,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, bytes, capacity);
    }

    /// Drop every cached record for `tenant` whose key starts with `prefix`.
    /// The tombstone-observation invalidation trigger ADR-0010 §10 promises:
    /// when a resolver lists a bucket's retention tombstone, the bucket's
    /// cached commit records are dropped so a later token-fallback GET cannot
    /// serve a record the sweep is about to physically remove.
    pub(crate) fn invalidate_prefix(&self, tenant: &TenantHash, prefix: &str) {
        if let Some(cache) = self.tenants.lock().get_mut(tenant) {
            cache.entries.retain(|k, _| !k.starts_with(prefix));
            cache.order.retain(|k| !k.starts_with(prefix));
        }
    }
}

#[derive(Default)]
struct CompactionTenantCache {
    entries: HashMap<String, (Arc<CompactionRecord>, u64)>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl CompactionTenantCache {
    fn insert(&mut self, key: String, record: Arc<CompactionRecord>, bytes: u64, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), (record, bytes));
        self.order.push_back(key);
        while self.order.len() > capacity.max(1) {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// Decoded compaction-record cache, partitioned by tenant and keyed by full
/// object key, exactly like [`RecordCache`] (docs/catalog-and-mvcc.md step
/// 2: compaction records are cached the same way commit records are).
/// Compaction records are immutable once published, so entries are only ever
/// capacity-cap evicted, except for one trigger: when a resolver observes a
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
            .and_then(|c| c.entries.get(key).cloned());
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
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, bytes, capacity);
    }

    /// Drop every cached compaction record for `tenant` whose key starts with
    /// `prefix` (ADR-0010 §10 tombstone-observation invalidation).
    pub(crate) fn invalidate_prefix(&self, tenant: &TenantHash, prefix: &str) {
        if let Some(cache) = self.tenants.lock().get_mut(tenant) {
            cache.entries.retain(|k, _| !k.starts_with(prefix));
            cache.order.retain(|k| !k.starts_with(prefix));
        }
    }
}

struct HeadCacheEntry {
    head: Arc<SnapshotHead>,
    bytes: u64,
    cached_at_ns: i64,
}

/// State behind [`HeadCache`]'s single lock: the entry map plus its
/// insertion order, so capacity-cap eviction (issue #421: this cache had a
/// TTL but no capacity bound) can pop the oldest (tenant, signal) pair
/// without a second, separately-lockable structure racing the first.
#[derive(Default)]
struct HeadCacheState {
    entries: HashMap<(TenantHash, Signal), HeadCacheEntry>,
    order: std::collections::VecDeque<(TenantHash, Signal)>,
}

/// Decoded-HEAD cache, one entry per (tenant, signal), with a caller-checked
/// TTL (docs/metric-index-plan.md 5.1: `head_cache_ttl`, default 30s) and a
/// capacity-cap bound on the number of (tenant, signal) pairs held at once
/// (issue #421). `now_ns` is always caller-supplied: this cache never reads
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
/// only capacity-cap eviction (docs/metric-index-plan.md 5.1:
/// `snapshot_cache_parts`).
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

/// Decoded name-postings cache, partitioned by tenant (P5b,
/// docs/metric-index-plan.md 5.4). Postings objects are content-addressed
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
        }
    }

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
            cache.insert(tenant, format!("k{i}"), Arc::new(record([2; 16], 0)), 1, 3);
        }
        // Oldest two evicted, most recent three retained.
        assert!(cache.get(&tenant, "k0", &accounting).is_none());
        assert!(cache.get(&tenant, "k1", &accounting).is_none());
        assert!(cache.get(&tenant, "k2", &accounting).is_some());
        assert!(cache.get(&tenant, "k3", &accounting).is_some());
        assert!(cache.get(&tenant, "k4", &accounting).is_some());
    }

    #[test]
    fn tenants_are_isolated() {
        let cache = RecordCache::default();
        let a = TenantHash([3; 16]);
        let b = TenantHash([4; 16]);
        let accounting = QueryAccounting::new();
        cache.insert(a, "k".to_string(), Arc::new(record([3; 16], 0)), 1, 10);
        assert!(cache.get(&a, "k", &accounting).is_some());
        assert!(cache.get(&b, "k", &accounting).is_none());
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

    /// Issue #421: `HeadCache` used to hold one entry per (tenant, signal)
    /// with no capacity bound at all. Inserting more tenants than the
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
    // Byte cache (ADR-0046, issue #443): Catalog::fetch_content_addressed,
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

    /// Issue #553: `byte_cache_max_bytes == 0` (the config `--disable-cache`
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
            )
            .await
            .expect("fold produces a snapshot part");

        let catalog = Catalog::new(store.clone(), disabled_config).expect("catalog");
        assert!(
            catalog.byte_cache_metrics().is_none(),
            "byte_cache_max_bytes == 0 must construct no byte cache, so there is no \
             counters handle to expose (issue #553)"
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
