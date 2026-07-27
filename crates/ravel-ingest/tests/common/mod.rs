//! Shared test support: a deterministic injected clock, point/label builders,
//! and a stalling store wrapper for backpressure tests. Not part of the
//! crate's public API; used only by integration tests under `tests/`.
#![allow(clippy::expect_used, dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_otlp::NormalizedPoint;
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
use tokio::sync::Notify;

/// Clock whose reading is set explicitly by the test, so flush identity
/// (ingest_hour_bucket, created_unix_ns) is deterministic and can be advanced
/// mid-retry to exercise hour-boundary pinning.
pub struct TestClock(AtomicI64);

impl TestClock {
    pub fn new(start_ns: i64) -> Arc<Self> {
        Arc::new(TestClock(AtomicI64::new(start_ns)))
    }

    pub fn set_ns(&self, ns: i64) {
        self.0.store(ns, Ordering::SeqCst);
    }

    pub fn advance_ns(&self, delta_ns: i64) {
        self.0.fetch_add(delta_ns, Ordering::SeqCst);
    }

    pub fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub fn tenant(id: &str) -> TenantId {
    TenantId::new(id)
}

pub fn build_labels(pairs: &[(&str, &str)]) -> LabelSet {
    let labels = pairs
        .iter()
        .map(|(name, value)| Label {
            name: (*name).to_string(),
            value: (*value).to_string(),
        })
        .collect();
    LabelSet::new(labels).expect("valid labels")
}

/// Builds one `NormalizedPoint` for `metric` with the given extra labels
/// (beyond `__name__`), timestamp, and value.
pub fn make_point(
    tenant: &TenantId,
    metric: &str,
    extra_labels: &[(&str, &str)],
    ts_ns: i64,
    value: f64,
) -> NormalizedPoint {
    let mut pairs = vec![(METRIC_NAME_LABEL, metric)];
    pairs.extend_from_slice(extra_labels);
    let labels = build_labels(&pairs);
    let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
    NormalizedPoint {
        series_id,
        labels,
        sample: Sample { ts_ns, value },
        is_monotonic_sum: false,
    }
}

/// Wraps a `MemoryStore` and stalls the `stall_on`-th `put` whose key
/// contains `key_contains` until `release()` is called. Used to
/// deterministically hold a shard actor mid-flush so backpressure on its
/// mpsc channel can be observed without racing on real time.
pub struct StallingStore {
    inner: MemoryStore,
    key_contains: String,
    stall_on: u64,
    hits: AtomicU64,
    gate: Notify,
    released: AtomicBool,
}

impl StallingStore {
    pub fn new(inner: MemoryStore, key_contains: impl Into<String>, stall_on: u64) -> Self {
        StallingStore {
            inner,
            key_contains: key_contains.into(),
            stall_on,
            hits: AtomicU64::new(0),
            gate: Notify::new(),
            released: AtomicBool::new(false),
        }
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.gate.notify_waiters();
    }

    async fn wait_for_release(&self) {
        loop {
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            let notified = self.gate.notified();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for StallingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.contains(self.key_contains.as_str()) {
            let hit = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
            if hit == self.stall_on {
                self.wait_for_release().await;
            }
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}
