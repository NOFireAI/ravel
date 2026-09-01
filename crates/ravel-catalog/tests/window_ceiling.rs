//! Admission and the catalog-request ceiling (ADR-0044 decision 3;
//! amended per ADR-0056).
//!
//! Under ADR-0056 a wide window is no longer refused before any LIST purely
//! because its per-bucket estimate `shard_count * hours + SNAPSHOT_...` exceeds
//! `CatalogConfig::max_catalog_list_requests`. Instead it is routed to the
//! single per-shard recursive prefix LIST, whose cost is `O(objects /
//! page_size)` and independent of window width. That path carries a runtime
//! LIST cap at the same ceiling, so the request bound ("a resolve never issues more
//! than the ceiling of catalog LISTs") is preserved -- now enforced at runtime
//! on the one path whose cost is not knowable before listing. The net effect:
//!
//! - a wide-but-sparse window that the request ceiling refused is now *served* cheaply
//!   (`wide_sparse_window_is_served_by_the_prefix_scan`);
//! - a scan whose actual object volume would exceed the ceiling is refused at
//!   runtime, having issued at most `ceiling` LISTs
//!   (`oversized_corpus_trips_the_runtime_cap`);
//! - `estimated_catalog_requests` keeps its formula and its upper-envelope role
//!   for cost accounting (`estimate_formula_is_unchanged`).
//!
//! The guard lives in `Catalog::resolve` itself, the one funnel every public
//! resolve entry point delegates to, so these catalog-level tests cover every
//! caller (SQL, PromQL, exemplars, analytics) at the choke point. The
//! HTTP-status mapping of the typed error is tested at each boundary
//! (crates/ravel-sql/src/error.rs, crates/ravel-query/src/http/error.rs).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig, CatalogError};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
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
    TenantHash([0xef; 16])
}

/// A catalog config with zero listing padding (`max_ingest_lag`,
/// `clock_skew_allowance`), so the pre-execution estimate is exactly
/// `shard_count * hour_buckets + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` and the
/// boundary can be pinned by arithmetic rather than approximated.
fn ceiling_config(shard_count: u32, ceiling: u64) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        max_ingest_lag_ns: 0,
        clock_skew_allowance_ns: 0,
        max_catalog_list_requests: ceiling,
        // Keep the crossover out of the way: these tests drive the prefix path
        // through wide windows (`suffix_buckets > ceiling`), not the crossover.
        prefix_list_crossover_requests: 720,
        ..Default::default()
    }
}

/// Build, PUT the data object, and publish one self-consistent commit record.
/// Distinct `writer_id` per call, so repeated calls at one hour populate one
/// bucket with several commit objects.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
) -> CommitRecord {
    let payload = format!("seg-{ingest_hour_bucket}-{created_unix_ns}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
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
        min_event_ts_ns: created_unix_ns,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns,
        max_ingest_ts_ns: created_unix_ns,
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

// ---------------------------------------------------------------------------
// Counting store: records every LIST call, so a test can bound the LIST count
// a resolve issues rather than infer it from timing.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct ListLog {
    lists: Arc<Mutex<Vec<String>>>,
}

impl ListLog {
    fn list_count(&self) -> usize {
        self.lists.lock().unwrap().len()
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
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.log.lists.lock().unwrap().push(prefix.to_string());
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.log.lists.lock().unwrap().push(prefix.to_string());
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn window(now_ns: i64) -> TimeRange {
    TimeRange {
        start_ns: 0,
        end_ns: now_ns,
    }
}

/// `estimated_catalog_requests` is unchanged: it still reports the per-bucket
/// worst case `shard_count * hour_buckets + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`
/// (3), the upper envelope threaded into ADR-0044's cost accounting. It is no
/// longer the admission gate for wide windows.
#[tokio::test]
async fn estimate_formula_is_unchanged() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store, ceiling_config(1, 100)).expect("catalog");

    // shard_count 1, zero padding: end_hour 96 -> 97 buckets -> 97 + 3 = 100.
    let at = 96 * NS_PER_HOUR;
    assert_eq!(catalog.estimated_catalog_requests(window(at), at), 100);
    // One bucket wider: 98 buckets -> 101.
    let over = 97 * NS_PER_HOUR;
    assert_eq!(catalog.estimated_catalog_requests(window(over), over), 101);
    // shard_count multiplies.
    let store16 = Arc::new(MemoryStore::new());
    let cat16 = Catalog::new(store16, ceiling_config(16, u64::MAX)).expect("catalog");
    assert_eq!(
        cat16.estimated_catalog_requests(window(at), at),
        16 * 97 + 3
    );
}

/// A window whose per-bucket estimate is far over the ceiling, over a *sparse*
/// corpus, is SERVED by the prefix scan rather than refused (ADR-0056
/// INTERACTION 1: the direction the request-ceiling refusal would have refused a cheap
/// query). The whole point of the change: the LIST count is O(objects), not
/// O(hours).
#[tokio::test]
async fn wide_sparse_window_is_served_by_the_prefix_scan() {
    let memory = Arc::new(MemoryStore::new());
    publish_segment(memory.as_ref(), 0, 1_000).await;
    let (counting, log) = CountingStore::new(memory);
    // Ceiling 100; estimate for a 10_000-hour window is 10_004, far over it.
    let catalog = Catalog::new(counting, ceiling_config(1, 100)).expect("catalog");

    let now_ns = 10_000 * NS_PER_HOUR;
    assert_eq!(
        catalog.estimated_catalog_requests(window(now_ns), now_ns),
        10_004,
        "the per-bucket estimate is still far over the ceiling"
    );

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, window(now_ns), &[], now_ns)
        .await
        .expect("wide-but-sparse window is served, not refused");
    assert_eq!(snapshot.segments.len(), 1, "the one segment is returned");
    assert!(
        log.list_count() <= 2,
        "prefix scan over a single sparse shard issues O(objects/page_size) \
         LISTs, not O(hours): issued {} for a 10_001-hour window that the \
         per-bucket loop would have listed with 10_001 LISTs",
        log.list_count()
    );
}

/// A window whose actual corpus is large enough that the prefix scan would
/// exceed the ceiling is refused at runtime, having issued at most `ceiling`
/// LISTs. This is the request bound, preserved.
#[tokio::test]
async fn oversized_corpus_trips_the_runtime_cap() {
    // page_size 2 so a modest object count paginates past a small ceiling.
    let memory = Arc::new(MemoryStore::with_page_size(2));
    // 20 commit objects in one bucket -> draining the shard subtree at
    // page_size 2 needs floor(20/2)+1 = 11 LISTs, well over the ceiling of 3.
    for i in 0..20u64 {
        publish_segment(memory.as_ref(), 0, 1_000 + i as i64).await;
    }
    let (counting, log) = CountingStore::new(memory);
    let ceiling = 3u64;
    let catalog = Catalog::new(counting, ceiling_config(1, ceiling)).expect("catalog");

    // Wide window -> prefix path (suffix_buckets = 10_001 > ceiling).
    let now_ns = 10_000 * NS_PER_HOUR;
    let err = catalog
        .resolve(&tenant(), Signal::Metrics, window(now_ns), &[], now_ns)
        .await
        .expect_err("a corpus that overruns the ceiling must be refused");
    match err {
        CatalogError::WindowTooWide { estimate, limit } => {
            assert_eq!(limit, ceiling, "the limit reported is the ceiling");
            assert!(
                estimate > limit,
                "estimate {estimate} must exceed limit {limit}"
            );
        }
        other => panic!("expected CatalogError::WindowTooWide, got {other:?}"),
    }
    // The runtime cap bounds the prefix SCAN at the ceiling. The pending-
    // erasure LIST (ADR-0064) is a separate, always-one LIST that now overlaps
    // the scan (issue #730), so it is issued once even on the refuse path;
    // hence the observed total is at most ceiling + 1, not ceiling.
    assert!(
        (log.list_count() as u64) <= ceiling + 1,
        "the runtime cap bounds the prefix scan at the ceiling ({ceiling}), plus \
         the one overlapping erasure LIST; issued {}",
        log.list_count()
    );
}

/// An ordinary narrow window (a few hours, as the endpoints' one-hour default
/// produces) stays on the per-bucket path and returns its records, unaffected.
#[tokio::test]
async fn ordinary_window_still_returns_records() {
    let store = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), 0, 1_000).await;
    let catalog = Catalog::new(store, ceiling_config(1, 100)).expect("catalog");

    // now = 4 hours, window [0, 4h]: estimate 1 * 5 + 3 = 8, per-bucket path.
    let now_ns = 4 * NS_PER_HOUR;
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, window(now_ns), &[], now_ns)
        .await
        .expect("ordinary window must resolve");
    assert_eq!(
        snapshot.segments.len(),
        1,
        "the published segment is returned"
    );
}
