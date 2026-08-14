//! ADR-0056: the per-shard recursive prefix LIST is a pure
//! traversal change. These tests prove it against the per-bucket loop.
//!
//! The traversal path is config-driven, so each path is forced directly:
//! `prefix_list_crossover_requests = u64::MAX` (with an unbounded ceiling)
//! pins the per-bucket loop; `prefix_list_crossover_requests = 0` pins the
//! prefix scan. Both are run over the same corpus and compared.
//!
//! Deliverables: (a) differential byte-identity, (b) request-count bound,
//! (c) late data below the window end still resolves, (d) the estimate stays
//! an upper envelope, (e) pagination across a page boundary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig, CatalogError};
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn tenant() -> TenantHash {
    TenantHash([0x5a; 16])
}

/// Base config: zero listing padding so hour math is exact, and a chosen
/// `shard_count`. Callers override the two traversal-selecting fields.
fn base_config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        max_ingest_lag_ns: 0,
        clock_skew_allowance_ns: 0,
        ..Default::default()
    }
}

/// Force the per-bucket loop: never reach the crossover, never trip the
/// ceiling.
fn per_bucket_config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        prefix_list_crossover_requests: u64::MAX,
        max_catalog_list_requests: u64::MAX,
        ..base_config(shard_count)
    }
}

/// Force the prefix scan: crossover 0 means any non-empty listing suffix takes
/// it. Ceiling unbounded so the runtime cap never trips.
fn prefix_config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        prefix_list_crossover_requests: 0,
        max_catalog_list_requests: u64::MAX,
        ..base_config(shard_count)
    }
}

/// Publish one L0 commit record (and its data object) with a distinct
/// `writer_id`, in `(shard, ingest_hour_bucket)`, with the given event ts.
async fn publish_at(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    ingest_hour_bucket: u32,
    event_ts_ns: i64,
) -> CommitRecord {
    let payload = format!("seg-{shard}-{ingest_hour_bucket}-{event_ts_ns}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns,
        min_ingest_ts_ns: event_ts_ns,
        max_ingest_ts_ns: event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: event_ts_ns,
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

async fn write_tombstone(store: &dyn ObjectStoreBackend, shard: u32, ingest_hour_bucket: u32) {
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

#[derive(Clone, Default)]
struct ListLog {
    n: Arc<Mutex<usize>>,
}
impl ListLog {
    fn count(&self) -> usize {
        *self.n.lock().unwrap()
    }
}
struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: ListLog,
}
impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, ListLog) {
        let log = ListLog::default();
        (
            Arc::new(CountingStore {
                inner,
                log: log.clone(),
            }),
            log,
        )
    }
}
#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(k, r).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        *self.log.n.lock().unwrap() += 1;
        self.inner.list(p, t).await
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        *self.log.n.lock().unwrap() += 1;
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn win(start_hour: i64, now_hour: i64) -> (TimeRange, i64) {
    (
        TimeRange {
            start_ns: start_hour * NS_PER_HOUR,
            end_ns: now_hour * NS_PER_HOUR,
        },
        now_hour * NS_PER_HOUR,
    )
}

fn data_keys(snap: &ravel_catalog::Snapshot) -> Vec<String> {
    snap.segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect()
}

/// (a) Differential: for a corpus spanning several ingest hours and shards,
/// including a tombstoned bucket and late data, the per-bucket loop and the
/// prefix scan return byte-identical snapshots, and both match an
/// independently reconstructed expected set.
#[tokio::test]
async fn differential_paths_return_identical_snapshots() {
    let store = Arc::new(MemoryStore::new());
    let shard_count = 2u32;

    // Corpus: several hours per shard, some hours with two writers, one late
    // record at hour 3, and a tombstoned bucket (shard 1, hour 5) that also
    // holds an L0 record which must therefore be excluded.
    let mut kept: BTreeSet<String> = BTreeSet::new();
    let mut push_kept = |rec: &CommitRecord| {
        kept.insert(keys::reconstruct_data_key(rec).expect("dk"));
    };

    for shard in 0..shard_count {
        for hour in [3u32, 10, 11, 42, 100] {
            let ev = i64::from(hour) * NS_PER_HOUR + 5 * 60_000_000_000;
            let r1 = publish_at(store.as_ref(), shard, hour, ev).await;
            push_kept(&r1);
            if hour == 11 {
                let r2 = publish_at(store.as_ref(), shard, hour, ev + 60_000_000_000).await;
                push_kept(&r2);
            }
        }
    }
    // Tombstoned bucket (shard 1, hour 5): its L0 record is NOT kept.
    let _tombstoned = publish_at(store.as_ref(), 1, 5, 5 * NS_PER_HOUR + 60_000_000_000).await;
    write_tombstone(store.as_ref(), 1, 5).await;

    // Window [0, 200h] covers every bucket above.
    let (range, now_ns) = win(0, 200);

    let per_bucket = Catalog::new(store.clone(), per_bucket_config(shard_count)).expect("cat");
    let prefix = Catalog::new(store.clone(), prefix_config(shard_count)).expect("cat");

    let snap_pb = per_bucket
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("per-bucket resolve");
    let snap_px = prefix
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix resolve");

    assert_eq!(
        snap_pb, snap_px,
        "the two traversals must return byte-identical snapshots"
    );

    // Independent reconstruction: exactly the kept records' data keys.
    let expected: Vec<String> = kept.into_iter().collect();
    let mut got = data_keys(&snap_px);
    got.sort();
    assert_eq!(
        got, expected,
        "snapshot must equal the independently built set"
    );
    assert_eq!(
        snap_px.segments.len(),
        expected.len(),
        "no tombstoned-bucket record leaked in"
    );
}

/// (b) An epoch-width window issues O(objects) requests on the prefix path,
/// not one-per-hour. The per-bucket loop over the same window issues one LIST
/// per bucket. Numbers stated so a reader sees the point.
#[tokio::test]
async fn epoch_window_issues_object_bounded_requests() {
    // Sparse corpus: 50 hours each with one shard-0 record => 50 commit
    // objects. A very wide window (5000 hours) has 4950 empty buckets.
    let inner = Arc::new(MemoryStore::new());
    for hour in 0..50u32 {
        publish_at(
            inner.as_ref(),
            0,
            hour,
            i64::from(hour) * NS_PER_HOUR + 60_000_000_000,
        )
        .await;
    }
    let (range, now_ns) = win(0, 4999);

    // Per-bucket path.
    let (c_pb, log_pb) = CountingStore::new(inner.clone());
    let pb = Catalog::new(c_pb, per_bucket_config(1)).expect("cat");
    let snap_pb = pb
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("per-bucket");

    // Prefix path.
    let (c_px, log_px) = CountingStore::new(inner.clone());
    let px = Catalog::new(c_px, prefix_config(1)).expect("cat");
    let snap_px = px
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix");

    assert_eq!(snap_pb, snap_px, "same result on both paths");
    assert_eq!(snap_px.segments.len(), 50);

    // Per-bucket = one LIST per (shard, hour) bucket = 5000 for a 5000-hour
    // window. Prefix = one page per page_size(1000) objects + terminal =
    // floor(50/1000)+1 = 1. Independent of the 4950 empty buckets. Each resolve
    // also issues exactly one `del/` LIST for pending erasure (ADR-0064
    // decision 2), so both paths carry a +1.
    assert_eq!(
        log_pb.count(),
        5001,
        "per-bucket loop issues one LIST per bucket (window width in hours) plus the del/ LIST"
    );
    assert_eq!(
        log_px.count(),
        2,
        "prefix scan issues O(objects/page_size) LISTs plus the del/ LIST; here 50 objects at \
         page_size 1000 = 1 page + 1 del/ LIST, versus the per-bucket loop's {}",
        log_pb.count()
    );
}

/// (c) Late-arriving data written into a bucket well below the window end is
/// still found by the prefix scan. This is what protects INTERACTION 2.
#[tokio::test]
async fn late_data_below_window_end_still_resolves() {
    let inner = Arc::new(MemoryStore::new());
    // One record at ingest hour 2, thousands of hours below the window end.
    let late = publish_at(inner.as_ref(), 0, 2, 2 * NS_PER_HOUR + 60_000_000_000).await;
    let (range, now_ns) = win(0, 9000);

    let px = Catalog::new(inner.clone(), prefix_config(1)).expect("cat");
    let snap = px
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix resolve");
    assert_eq!(snap.segments.len(), 1, "the low-bucket record is found");
    assert_eq!(
        snap.segments[0].data_object_key,
        keys::reconstruct_data_key(&late).expect("dk"),
    );
}

/// (d) `estimated_catalog_requests` remains an upper envelope of the requests
/// the prefix scan actually issues, for several window shapes. This is the
/// property the request ceiling rests on.
#[tokio::test]
async fn estimate_is_an_upper_envelope() {
    let inner = Arc::new(MemoryStore::new());
    // A denser-than-one-per-hour corpus (three writers per hour over 40 hours)
    // so the prefix scan issues a couple of pages, not a trivial one.
    for hour in 0..40u32 {
        for w in 0..3 {
            let ev = i64::from(hour) * NS_PER_HOUR + (w as i64 + 1) * 60_000_000_000;
            publish_at(inner.as_ref(), 0, hour, ev).await;
        }
    }
    for &now_hour in &[10i64, 100, 1000, 5000] {
        let (range, now_ns) = win(0, now_hour);
        let (counting, log) = CountingStore::new(inner.clone());
        let cat = Catalog::new(counting, prefix_config(1)).expect("cat");
        let estimate = cat.estimated_catalog_requests(range, now_ns);
        cat.resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
            .await
            .expect("resolve");
        assert!(
            estimate >= log.count() as u64,
            "estimate ({estimate}) must be >= actual LISTs ({}) for window \
             now={now_hour}h",
            log.count()
        );
    }
}

/// (e) The prefix scan drains every page across a page boundary. With
/// `page_size = 2` and five commit objects in the shard subtree, the scan
/// paginates and still returns all overlapping segments.
#[tokio::test]
async fn prefix_scan_paginates_across_page_boundaries() {
    let inner = Arc::new(MemoryStore::with_page_size(2));
    // Five records across three hours (one hour holds three writers), so the
    // shard subtree spans multiple pages of size 2.
    publish_at(inner.as_ref(), 0, 1, NS_PER_HOUR + 60_000_000_000).await;
    publish_at(inner.as_ref(), 0, 2, 2 * NS_PER_HOUR + 60_000_000_000).await;
    publish_at(inner.as_ref(), 0, 2, 2 * NS_PER_HOUR + 120_000_000_000).await;
    publish_at(inner.as_ref(), 0, 2, 2 * NS_PER_HOUR + 180_000_000_000).await;
    publish_at(inner.as_ref(), 0, 3, 3 * NS_PER_HOUR + 60_000_000_000).await;

    let (range, now_ns) = win(0, 50);

    // Cross-check: per-bucket loop over the same corpus and page size.
    let per_bucket = Catalog::new(inner.clone(), per_bucket_config(1)).expect("cat");
    let (counting, log) = CountingStore::new(inner.clone());
    let prefix = Catalog::new(counting, prefix_config(1)).expect("cat");

    let snap_pb = per_bucket
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("per-bucket");
    let snap_px = prefix
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix");

    assert_eq!(snap_pb, snap_px, "pagination must not change the result");
    assert_eq!(snap_px.segments.len(), 5, "all five records returned");
    // 5 commit objects at page_size 2: floor(5/2)+1 = 3 pages for the one
    // shard subtree, plus one `del/` LIST per resolve for pending erasure
    // (ADR-0064 decision 2) = 4.
    assert_eq!(
        log.count(),
        4,
        "prefix scan drained the shard subtree across page boundaries, plus the del/ LIST"
    );
}

/// Sanity: a `CatalogError` type is in scope (keeps the import meaningful even
/// if the happy-path tests never construct one).
#[allow(dead_code)]
fn _assert_error_in_scope(e: CatalogError) -> CatalogError {
    e
}
