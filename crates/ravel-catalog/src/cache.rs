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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ravel_proto::catalog::v1::SnapshotHead;
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord};
use ravel_types::{Signal, TenantHash};

use crate::snapshot_format::{DecodedPart, DecodedPostings};

#[derive(Default)]
struct TenantCache {
    entries: HashMap<String, Arc<CommitRecord>>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl TenantCache {
    fn insert(&mut self, key: String, record: Arc<CommitRecord>, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), record);
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
    pub(crate) fn get(&self, tenant: &TenantHash, key: &str) -> Option<Arc<CommitRecord>> {
        self.tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned())
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        record: Arc<CommitRecord>,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, capacity);
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
    entries: HashMap<String, Arc<CompactionRecord>>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl CompactionTenantCache {
    fn insert(&mut self, key: String, record: Arc<CompactionRecord>, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), record);
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
    pub(crate) fn get(&self, tenant: &TenantHash, key: &str) -> Option<Arc<CompactionRecord>> {
        self.tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned())
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        record: Arc<CompactionRecord>,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, record, capacity);
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
    cached_at_ns: i64,
}

/// Decoded-HEAD cache, one entry per (tenant, signal), with a caller-checked
/// TTL (docs/metric-index-plan.md 5.1: `head_cache_ttl`, default 30s).
/// `now_ns` is always caller-supplied: this cache never reads a clock.
#[derive(Default)]
pub(crate) struct HeadCache {
    entries: Mutex<HashMap<(TenantHash, Signal), HeadCacheEntry>>,
}

impl HeadCache {
    pub(crate) fn get(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        now_ns: i64,
        ttl_ns: i64,
    ) -> Option<Arc<SnapshotHead>> {
        let entries = self.entries.lock();
        let entry = entries.get(&(*tenant, signal))?;
        if now_ns.saturating_sub(entry.cached_at_ns) > ttl_ns {
            return None;
        }
        Some(entry.head.clone())
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        signal: Signal,
        head: Arc<SnapshotHead>,
        now_ns: i64,
    ) {
        self.entries.lock().insert(
            (tenant, signal),
            HeadCacheEntry {
                head,
                cached_at_ns: now_ns,
            },
        );
    }
}

#[derive(Default)]
struct PartTenantCache {
    entries: HashMap<String, Arc<DecodedPart>>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl PartTenantCache {
    fn insert(&mut self, key: String, part: Arc<DecodedPart>, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), part);
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
    pub(crate) fn get(&self, tenant: &TenantHash, key: &str) -> Option<Arc<DecodedPart>> {
        self.tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned())
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        part: Arc<DecodedPart>,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, part, capacity);
    }
}

#[derive(Default)]
struct PostingsTenantCache {
    entries: HashMap<String, Arc<DecodedPostings>>,
    /// Insertion order, oldest first, for capacity-cap eviction.
    order: std::collections::VecDeque<String>,
}

impl PostingsTenantCache {
    fn insert(&mut self, key: String, postings: Arc<DecodedPostings>, capacity: usize) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), postings);
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
    pub(crate) fn get(&self, tenant: &TenantHash, key: &str) -> Option<Arc<DecodedPostings>> {
        self.tenants
            .lock()
            .get(tenant)
            .and_then(|c| c.entries.get(key).cloned())
    }

    pub(crate) fn insert(
        &self,
        tenant: TenantHash,
        key: String,
        postings: Arc<DecodedPostings>,
        capacity: usize,
    ) {
        self.tenants
            .lock()
            .entry(tenant)
            .or_default()
            .insert(key, postings, capacity);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

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
        assert!(cache.get(&tenant, "k").is_none());
        cache.insert(tenant, "k".to_string(), Arc::new(record([1; 16], 0)), 10);
        assert!(cache.get(&tenant, "k").is_some());
    }

    #[test]
    fn capacity_cap_evicts_oldest() {
        let cache = RecordCache::default();
        let tenant = TenantHash([2; 16]);
        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), Arc::new(record([2; 16], 0)), 3);
        }
        // Oldest two evicted, most recent three retained.
        assert!(cache.get(&tenant, "k0").is_none());
        assert!(cache.get(&tenant, "k1").is_none());
        assert!(cache.get(&tenant, "k2").is_some());
        assert!(cache.get(&tenant, "k3").is_some());
        assert!(cache.get(&tenant, "k4").is_some());
    }

    #[test]
    fn tenants_are_isolated() {
        let cache = RecordCache::default();
        let a = TenantHash([3; 16]);
        let b = TenantHash([4; 16]);
        cache.insert(a, "k".to_string(), Arc::new(record([3; 16], 0)), 10);
        assert!(cache.get(&a, "k").is_some());
        assert!(cache.get(&b, "k").is_none());
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
        }
    }

    #[test]
    fn head_cache_miss_then_hit() {
        let cache = HeadCache::default();
        let tenant = TenantHash([5; 16]);
        assert!(cache.get(&tenant, Signal::Metrics, 1_000, 500).is_none());
        cache.insert(tenant, Signal::Metrics, Arc::new(head([5; 16], 10)), 1_000);
        let cached = cache
            .get(&tenant, Signal::Metrics, 1_000, 500)
            .expect("hit");
        assert_eq!(cached.watermark_hour, 10);
    }

    #[test]
    fn head_cache_expires_after_ttl() {
        let cache = HeadCache::default();
        let tenant = TenantHash([6; 16]);
        cache.insert(tenant, Signal::Metrics, Arc::new(head([6; 16], 1)), 0);
        assert!(cache.get(&tenant, Signal::Metrics, 500, 500).is_some());
        assert!(cache.get(&tenant, Signal::Metrics, 501, 500).is_none());
    }

    #[test]
    fn head_cache_is_keyed_by_signal_too() {
        let cache = HeadCache::default();
        let tenant = TenantHash([7; 16]);
        cache.insert(tenant, Signal::Metrics, Arc::new(head([7; 16], 1)), 0);
        assert!(cache.get(&tenant, Signal::Logs, 0, 500).is_none());
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
            },
            entries: vec![],
        }
    }

    #[test]
    fn part_cache_miss_then_hit() {
        let cache = PartCache::default();
        let tenant = TenantHash([8; 16]);
        assert!(cache.get(&tenant, "k").is_none());
        cache.insert(tenant, "k".to_string(), Arc::new(decoded_part(1)), 10);
        assert!(cache.get(&tenant, "k").is_some());
    }

    #[test]
    fn part_cache_capacity_cap_evicts_oldest() {
        let cache = PartCache::default();
        let tenant = TenantHash([9; 16]);
        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), Arc::new(decoded_part(1)), 3);
        }
        assert!(cache.get(&tenant, "k0").is_none());
        assert!(cache.get(&tenant, "k1").is_none());
        assert!(cache.get(&tenant, "k2").is_some());
        assert!(cache.get(&tenant, "k3").is_some());
        assert!(cache.get(&tenant, "k4").is_some());
    }

    #[test]
    fn part_cache_tenants_are_isolated() {
        let cache = PartCache::default();
        let a = TenantHash([10; 16]);
        let b = TenantHash([11; 16]);
        cache.insert(a, "k".to_string(), Arc::new(decoded_part(1)), 10);
        assert!(cache.get(&a, "k").is_some());
        assert!(cache.get(&b, "k").is_none());
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
        assert!(cache.get(&tenant, "k").is_none());
        cache.insert(tenant, "k".to_string(), Arc::new(decoded_postings()), 10);
        assert!(cache.get(&tenant, "k").is_some());
    }

    #[test]
    fn postings_cache_capacity_cap_evicts_oldest() {
        let cache = PostingsCache::default();
        let tenant = TenantHash([13; 16]);
        for i in 0..5 {
            cache.insert(tenant, format!("k{i}"), Arc::new(decoded_postings()), 3);
        }
        assert!(cache.get(&tenant, "k0").is_none());
        assert!(cache.get(&tenant, "k1").is_none());
        assert!(cache.get(&tenant, "k2").is_some());
        assert!(cache.get(&tenant, "k3").is_some());
        assert!(cache.get(&tenant, "k4").is_some());
    }

    #[test]
    fn postings_cache_tenants_are_isolated() {
        let cache = PostingsCache::default();
        let a = TenantHash([14; 16]);
        let b = TenantHash([15; 16]);
        cache.insert(a, "k".to_string(), Arc::new(decoded_postings()), 10);
        assert!(cache.get(&a, "k").is_some());
        assert!(cache.get(&b, "k").is_none());
    }
}
