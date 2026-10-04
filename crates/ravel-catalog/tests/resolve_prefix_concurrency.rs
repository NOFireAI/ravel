//! ADR-2509 decision 1: the prefix traversal drains its shards concurrently,
//! each page reserving a slot from one shared LIST cap before it is issued.
//!
//! `MemoryStore` never yields, so concurrency is proven with `FaultStore`
//! holds: a held LIST stays in flight until the test releases it, and the
//! test observes which other LISTs reach the store meanwhile.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig, CatalogError, Snapshot};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SHARDS: u32 = 4;
/// Window `[0, 200h]` at 4 shards: 4 * 201 = 804 suffix buckets, at or above
/// the default `prefix_list_crossover_requests` (720), so resolve takes the
/// prefix path with no crossover override.
const NOW_HOUR: i64 = 200;

fn tenant() -> TenantHash {
    TenantHash([0x6b; 16])
}

/// Zero listing padding so the hour math is exact; default crossover.
fn base_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: SHARDS,
        max_ingest_lag_ns: 0,
        clock_skew_allowance_ns: 0,
        ..Default::default()
    }
}

fn window() -> (TimeRange, i64) {
    (
        TimeRange {
            start_ns: 0,
            end_ns: NOW_HOUR * NS_PER_HOUR,
        },
        NOW_HOUR * NS_PER_HOUR,
    )
}

/// The commit-record LIST prefix of every shard, and of no other LIST the
/// resolve issues (the pending-erasure LIST is under `/m/del/`).
const COMMIT_LIST_MARK: &str = "/m/c/";

fn shard_prefix(shard: u32) -> String {
    keys::commit_shard_prefix(&tenant(), Signal::Metrics, shard).expect("shard prefix")
}

/// Publish one L0 commit record and its data object in `(shard, hour)`;
/// returns the data object key.
async fn publish_at(store: &dyn ObjectStoreBackend, shard: u32, hour: u32, minute: i64) -> String {
    let event_ts_ns = i64::from(hour) * NS_PER_HOUR + minute * 60_000_000_000;
    let payload = format!("seg-{shard}-{hour}-{minute}").into_bytes();
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
    data_key
}

/// Every commit-record LIST prefix that reached the store, in arrival order.
/// Counted before the `FaultStore` underneath, so a held LIST counts as
/// issued the moment it is sent.
#[derive(Clone, Default)]
struct ListLog(Arc<Mutex<Vec<String>>>);

impl ListLog {
    fn commit_lists(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: ListLog,
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
        self.inner.list(p, t).await
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        if p.contains(COMMIT_LIST_MARK) {
            self.log.0.lock().unwrap().push(p.to_string());
        }
        self.inner.list_after(p, start_after, t).await
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
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

/// `inner` behind a `FaultStore` (for holds) behind a LIST log.
fn wrap(
    inner: Arc<MemoryStore>,
) -> (
    Arc<CountingStore>,
    Arc<FaultStore<Arc<MemoryStore>>>,
    ListLog,
) {
    let fault = Arc::new(FaultStore::new(inner, FaultPlan::empty()));
    let log = ListLog::default();
    let counting = Arc::new(CountingStore {
        inner: fault.clone(),
        log: log.clone(),
    });
    (counting, fault, log)
}

/// Resolve the fixture window while `release` decides, each time at least one
/// LIST is held, which held calls to let go. `select!` is biased toward the
/// resolve, so every woken shard advances to its next page (and is held
/// there) before the releaser looks again. Returns the resolve result and the
/// largest number of LISTs seen held at once.
async fn resolve_releasing(
    catalog: &Catalog,
    gate: &GateHandle,
    release: impl Fn(&[(u64, Op, String)]) -> Vec<u64>,
) -> (Result<Snapshot, CatalogError>, usize) {
    let (range, now_ns) = window();
    let tenant = tenant();
    let resolve = catalog.resolve(&tenant, Signal::Metrics, range, &[], now_ns);
    tokio::pin!(resolve);
    let mut max_held = 0;
    loop {
        tokio::select! {
            biased;
            result = &mut resolve => return (result, max_held),
            () = gate.wait_until_held(1) => {
                let held = gate.held_details();
                max_held = max_held.max(held.len());
                for id in release(&held) {
                    assert!(gate.release(id), "released a call that was held");
                }
            }
        }
    }
}

/// While shard 0's first LIST is held, the other three shards' LISTs are
/// issued. Against the sequential drain this fails: nothing past shard 0 is
/// listed until its page returns.
#[tokio::test]
async fn prefix_listing_overlaps_shards_under_reserved_cap() {
    let inner = Arc::new(MemoryStore::new());
    let mut expected = BTreeSet::new();
    for shard in 0..SHARDS {
        expected.insert(publish_at(inner.as_ref(), shard, 10 + shard, 5).await);
        expected.insert(publish_at(inner.as_ref(), shard, 150, 7).await);
    }
    let (counting, fault, log) = wrap(inner);
    let catalog = Catalog::new(counting, base_config()).expect("catalog");

    let gate = fault.hold(Op::List, Some(shard_prefix(0)), Occurrence::Nth(1));
    let (range, now_ns) = window();
    let tenant = tenant();
    let driver = async {
        gate.wait_until_held(1).await;
        // Give the other shards' futures every chance to reach the store; a
        // sequential drain never does, so this bound ends the wait.
        for _ in 0..1_000 {
            if log.commit_lists().len() >= SHARDS as usize {
                break;
            }
            tokio::task::yield_now().await;
        }
        let held = gate.held_details();
        assert_eq!(held.len(), 1, "exactly shard 0's first LIST is held");
        assert_eq!(held[0].2, shard_prefix(0));
        let listed: BTreeSet<String> = log.commit_lists().into_iter().collect();
        for shard in 1..SHARDS {
            assert!(
                listed.contains(&shard_prefix(shard)),
                "shard {shard}'s LIST must be issued while shard 0's is held; listed: {listed:?}"
            );
        }
        gate.release(held[0].0)
    };
    let (snapshot, released) = tokio::join!(
        catalog.resolve(&tenant, Signal::Metrics, range, &[], now_ns),
        driver
    );
    assert!(released, "shard 0's held LIST was released");
    let snapshot = snapshot.expect("resolve");
    let got: BTreeSet<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    assert_eq!(got, expected);
    assert_eq!(
        log.commit_lists().len(),
        SHARDS as usize,
        "one page per shard"
    );
}

/// Four shards of two pages each (8 pages) against a cap of 5. Every commit
/// LIST is held and released in waves, so all four shards contend for the
/// last slot with 4 = cap - 1 already taken. The reserved counter grants it
/// to exactly one; a check-then-increment counter lets all four through and
/// issues 8.
#[tokio::test]
async fn prefix_listing_cap_is_exact_under_concurrency() {
    const CAP: u64 = 5;
    let inner = Arc::new(MemoryStore::with_page_size(2));
    for shard in 0..SHARDS {
        // Three records at page size 2: a full first page, then a last page.
        for minute in [1, 2, 3] {
            publish_at(inner.as_ref(), shard, 20, minute).await;
        }
    }
    let (counting, fault, log) = wrap(inner);
    let catalog = Catalog::new(
        counting,
        CatalogConfig {
            max_catalog_list_requests: CAP,
            ..base_config()
        },
    )
    .expect("catalog");

    let gate = fault.hold(
        Op::List,
        Some(COMMIT_LIST_MARK.to_string()),
        Occurrence::Always,
    );
    let (result, max_held) =
        resolve_releasing(&catalog, &gate, |held| held.iter().map(|h| h.0).collect()).await;

    assert_eq!(
        max_held, SHARDS as usize,
        "all four shards' first pages were held at once"
    );
    let issued = log.commit_lists().len() as u64;
    assert!(
        issued <= CAP,
        "{issued} commit LISTs issued, over the cap of {CAP}"
    );
    match result {
        Err(CatalogError::WindowTooWide { estimate, limit }) => {
            assert_eq!(limit, CAP);
            assert_eq!(
                estimate,
                CAP + 1,
                "all slots reserved plus the refused page"
            );
        }
        other => panic!("expected WindowTooWide, got {other:?}"),
    }
}

/// The same multi-shard, multi-page corpus resolved by the prefix path (shards
/// completing in reverse order) and by the bounded path gives identical
/// snapshots: the same keys in the same order.
#[tokio::test]
async fn prefix_listing_key_set_matches_sequential() {
    let inner = Arc::new(MemoryStore::with_page_size(2));
    let mut expected = BTreeSet::new();
    for shard in 0..SHARDS {
        // A different page count per shard: 1 + shard * 2 records.
        for i in 0..(1 + shard * 2) {
            let hour = 3 + shard * 17 + i * 11;
            expected.insert(publish_at(inner.as_ref(), shard, hour, i64::from(i) + 1).await);
        }
        // Two writers in one hour, so a bucket spans a page boundary.
        expected.insert(publish_at(inner.as_ref(), shard, 120, 1).await);
        expected.insert(publish_at(inner.as_ref(), shard, 120, 2).await);
    }

    let bounded = Catalog::new(
        inner.clone(),
        CatalogConfig {
            prefix_list_crossover_requests: u64::MAX,
            max_catalog_list_requests: u64::MAX,
            ..base_config()
        },
    )
    .expect("catalog");
    let (range, now_ns) = window();
    let bounded_snapshot = bounded
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("bounded resolve");

    let (counting, fault, log) = wrap(inner);
    let prefix = Catalog::new(counting, base_config()).expect("catalog");
    let gate = fault.hold(
        Op::List,
        Some(COMMIT_LIST_MARK.to_string()),
        Occurrence::Always,
    );
    // Release only the held page of the highest shard each time, so later
    // shards finish before earlier ones.
    let (result, max_held) = resolve_releasing(&prefix, &gate, |held| {
        held.iter()
            .max_by(|a, b| a.2.cmp(&b.2))
            .map(|h| vec![h.0])
            .unwrap_or_default()
    })
    .await;
    let prefix_snapshot = result.expect("prefix resolve");

    assert_eq!(
        max_held, SHARDS as usize,
        "the shards were in flight together"
    );
    let pages = log.commit_lists().len();
    assert!(
        pages > SHARDS as usize,
        "the corpus paginates: {pages} commit LISTs"
    );
    assert_eq!(prefix_snapshot, bounded_snapshot);
    let keys_in_order: Vec<String> = prefix_snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    assert_eq!(
        keys_in_order,
        bounded_snapshot
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        keys_in_order.iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
}
