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
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::TenantHash;

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
}
