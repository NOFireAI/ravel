//! Shared test support: a deterministic injected clock, point/label builders,
//! and a stalling store wrapper for backpressure tests. Not part of the
//! crate's public API; used only by integration tests under `tests/`.
#![allow(clippy::expect_used, dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::{Clock, SEGMENT_FORMAT_VERSION};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_otlp::NormalizedPoint;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use tokio::sync::{Notify, watch};
use uuid::Uuid;

/// Clock whose reading is set explicitly by the test, so flush identity
/// (ingest_hour_bucket, created_unix_ns) is deterministic and can be advanced
/// mid-retry to exercise hour-boundary pinning.
///
/// `Clock::sleep` (the shard actor's flush tick) is driven off `wake_tx`:
/// every `set_ns`/`advance_ns` sends on the watch channel, waking any
/// sleeping actor to re-check its deadline. A `watch` (not a `Notify`) is
/// used so an advance that lands before the sleeper registers is never lost:
/// the version bump is durable and `changed()` observes it on the next poll.
pub struct TestClock {
    now_ns: AtomicI64,
    wake_tx: watch::Sender<()>,
}

impl TestClock {
    pub fn new(start_ns: i64) -> Arc<Self> {
        let (wake_tx, _rx) = watch::channel(());
        Arc::new(TestClock {
            now_ns: AtomicI64::new(start_ns),
            wake_tx,
        })
    }

    pub fn set_ns(&self, ns: i64) {
        self.now_ns.store(ns, Ordering::SeqCst);
        let _ = self.wake_tx.send(());
    }

    pub fn advance_ns(&self, delta_ns: i64) {
        self.now_ns.fetch_add(delta_ns, Ordering::SeqCst);
        let _ = self.wake_tx.send(());
    }

    pub fn now(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let deadline = self
            .now_ns()
            .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
        // Subscribe before the first deadline check: any advance after this
        // point bumps the watch version and is seen by `changed()`, and any
        // advance already reflected in `now_ns()` is caught by the check, so
        // the wakeup can be neither duplicated into a spin nor lost.
        let mut rx = self.wake_tx.subscribe();
        Box::pin(async move {
            loop {
                if self.now_ns() >= deadline {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The clock (and its sender) is gone; nothing will advance
                    // time again, so let the sleeper proceed rather than hang.
                    return;
                }
            }
        })
    }
}

/// Wraps a `MemoryStore` and makes every `put` whose key contains
/// `key_contains` sleep for `delay` on the injected `Clock` before
/// delegating to the inner store. Models a backend call that is merely slow
/// -- it always eventually succeeds, never errors -- so a test can drive
/// the shard actor's `max_flush_lifetime` deadline past `delay` and assert
/// the flush is abandoned rather than published, without any
/// real wall-clock wait: the delay only ever elapses if the test itself
/// advances `clock` that far.
pub struct SlowStore {
    inner: MemoryStore,
    clock: Arc<dyn Clock>,
    delay: Duration,
    key_contains: String,
    hits: AtomicU64,
}

impl SlowStore {
    pub fn new(
        inner: MemoryStore,
        clock: Arc<dyn Clock>,
        key_contains: impl Into<String>,
        delay: Duration,
    ) -> Self {
        SlowStore {
            inner,
            clock,
            delay,
            key_contains: key_contains.into(),
            hits: AtomicU64::new(0),
        }
    }

    /// Number of `put` calls so far whose key matched `key_contains` (and so
    /// entered the artificial delay), regardless of whether the delay has
    /// finished yet.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for SlowStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.contains(self.key_contains.as_str()) {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.clock.sleep(self.delay).await;
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
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
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
        labels: Arc::new(labels),
        sample: Sample { ts_ns, value },
        is_monotonic_sum: false,
    }
}

/// Builds one `SeriesInput` for hand-assembled RSEG segments (tests that
/// bypass `IngestRouter` to control commit identity, RSEG version, or exact
/// sample timestamps directly).
pub fn series_input(
    tenant_id: &TenantId,
    metric: &str,
    extra: &[(&str, &str)],
    samples: &[(i64, f64)],
) -> SeriesInput {
    let mut pairs = vec![(METRIC_NAME_LABEL, metric)];
    pairs.extend_from_slice(extra);
    let label_set = build_labels(&pairs);
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }
}

/// Writes a real RSEG v5 segment (`SegmentWriter::write`, the raw-sample
/// adapter) and publishes its commit record directly, bypassing
/// `IngestRouter`. Used by tests that need to pin exact commit identity
/// (writer_seq, ingest_hour_bucket, created_unix_ns) that a single
/// `IngestRouter` cannot produce in one flush. ADR-0027: v5 is the only
/// version, so there is no version to choose.
#[allow(clippy::too_many_arguments)]
pub async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    shard: u32,
    writer_id: Uuid,
    writer_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    series: Vec<SeriesInput>,
) -> (CommitToken, String) {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write v5 segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(SEGMENT_FORMAT_VERSION),
        created_unix_ns,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

pub fn catalog(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config valid")
}

pub fn engine(store: Arc<dyn ObjectStoreBackend>, catalog: Catalog) -> QueryEngine {
    QueryEngine::new(Arc::new(catalog), store, EngineConfig::default())
}

/// Unwraps a range-query result this suite expects to be a plain range
/// matrix, panicking with the actual variant otherwise.
pub fn expect_matrix(value: ravel_promql::Value) -> ravel_promql::RangeMatrix {
    match value {
        ravel_promql::Value::Matrix(m) => m,
        other => panic!("expected range matrix, got {other:?}"),
    }
}

/// Fetches the commit record a token addresses, decodes it, then fetches
/// and opens the exact data object the record's own `object_key` names, and
/// returns `(commit record's segment_format_version, object's own trailer
/// version)`. Chains commit record -> `object_key` -> data object rather
/// than guessing the data key from a prefix listing, so the two returned
/// values are guaranteed to describe the very same object, not merely "the
/// only object present."
pub async fn commit_and_trailer_versions(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    token: &CommitToken,
) -> (u32, u16) {
    let commit_key = keys::commit_key_for_token(tenant_hash, signal, token).expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let record = record::decode(&commit_bytes).expect("decode commit record");
    let data_bytes = store
        .get(&record.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data;
    let loc =
        ravel_segment::open_from_full(&data_bytes, ReaderLimits::default()).expect("open segment");
    (record.segment_format_version, loc.version)
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
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}
