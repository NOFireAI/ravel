//! issue #730: the resolve lists the suffix above the snapshot watermark with
//! one bounded LIST per shard (not one per (shard, hour)), and overlaps the
//! pending-erasure LIST with that fan-out.
//!
//! Three properties, each with a stated red flip.
//!
//! Differential + request count: a folded tenant with data in some but not all
//! (shard, hour) buckets above the watermark, plus sealed data below it,
//! resolves to the same segment set/order/origins as the independent prefix
//! traversal and an independent reconstruction, in exactly 9 LISTs (8 shards +
//! 1 erasure), and no below-watermark key is ever listed. RED: restoring the
//! per-(shard, hour) loop makes it 25 LISTs; dropping the `start_after` makes
//! below-watermark keys appear in the listing.
//!
//! Pagination: a bounded shard LIST pages across a small page size and stops at
//! the first key past the window end. RED: dropping the stop-at-end paging
//! fetches one extra page.
//!
//! Concurrency: the erasure LIST is issued concurrently with the shard fan-out,
//! so all nine LISTs can be held at once. RED: awaiting the erasure LIST after
//! the fan-out means it is never issued while the shard LISTs are held, so at
//! most eight are ever held simultaneously.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, SegmentOrigin,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SEAL_MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0x73; 16])
}

/// A `config` at default margins, so the fold's own seal rule matches
/// [`now_at_seal`] below.
fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

/// `now_ns` at which ingest hour `hour` has just sealed under the default
/// margins (mirrors `Catalog::fold`'s crate-private `now_at_seal`).
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + SEAL_MARGIN_NS
}

/// Zero listing padding so window hour math is exact (mirrors
/// resolve_prefix_traversal's base config), with the chosen shard count.
fn exact_config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        max_ingest_lag_ns: 0,
        clock_skew_allowance_ns: 0,
        ..Default::default()
    }
}

/// Force the prefix traversal for the differential cross-check: crossover 0
/// takes it for any non-empty suffix, ceiling unbounded. Same (default)
/// margins as [`config`] so both traversals see the identical window.
fn prefix_config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        prefix_list_crossover_requests: 0,
        max_catalog_list_requests: u64::MAX,
        ..config(shard_count)
    }
}

async fn publish_at(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    ingest_hour_bucket: u32,
    event_ts_ns: i64,
) -> String {
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
    data_key
}

fn hour_mid_ns(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR + 5 * 60_000_000_000
}

/// A store double that records every LIST prefix and every key any LIST
/// returned, forwarding `list_after` to the inner store's native start-after
/// so the below-watermark skip is exercised as it is in production.
#[derive(Clone, Default)]
struct ListRecord {
    prefixes: Arc<Mutex<Vec<String>>>,
    returned_keys: Arc<Mutex<Vec<String>>>,
}
impl ListRecord {
    fn count_for(&self, needle: &str) -> usize {
        self.prefixes
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.contains(needle))
            .count()
    }
    fn total(&self) -> usize {
        self.prefixes.lock().unwrap().len()
    }
    fn returned_keys(&self) -> Vec<String> {
        self.returned_keys.lock().unwrap().clone()
    }
}
struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    rec: ListRecord,
}
impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, ListRecord) {
        let rec = ListRecord::default();
        (
            Arc::new(RecordingStore {
                inner,
                rec: rec.clone(),
            }),
            rec,
        )
    }
    fn note(&self, prefix: &str, page: &ListPage) {
        self.rec.prefixes.lock().unwrap().push(prefix.to_string());
        let mut keys = self.rec.returned_keys.lock().unwrap();
        for meta in &page.objects {
            keys.push(meta.key.clone());
        }
    }
}
#[async_trait]
impl ObjectStoreBackend for RecordingStore {
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
        let page = self.inner.list(p, t).await?;
        self.note(p, &page);
        Ok(page)
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let page = self.inner.list_after(p, start_after, t).await?;
        self.note(p, &page);
        Ok(page)
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

/// Differential + exact request count for the folded-tenant warm path.
#[tokio::test]
async fn bounded_listing_matches_reference_in_nine_lists() {
    let shard_count = 8u32;
    let inner = Arc::new(MemoryStore::new());

    // Sealed region below/at the watermark: hours 998..=1000, some shards.
    let mut sealed_keys: BTreeSet<String> = BTreeSet::new();
    for hour in [998u32, 999, 1000] {
        for shard in 0..shard_count {
            sealed_keys.insert(publish_at(inner.as_ref(), shard, hour, hour_mid_ns(hour)).await);
        }
    }
    // Fold seals everything through hour 1000 -> watermark 1000.
    let watermark = 1000u32;
    let fold_now = now_at_seal(watermark);
    let fold_catalog = Catalog::new(inner.clone(), config(shard_count)).expect("catalog");
    let report = fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            fold_now,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.watermark_hour,
        Some(watermark),
        "the fold must seal exactly through hour 1000"
    );

    // Unsealed tail: 3 hours above the watermark, populated in SOME but not
    // all (shard, hour) buckets. Several shards have nothing above the
    // watermark; several hours are empty for a given shard.
    let mut recent_keys: BTreeSet<String> = BTreeSet::new();
    let above: &[(u32, u32)] = &[
        (0, 1001),
        (0, 1003),
        (1, 1002),
        (2, 1001),
        (2, 1002),
        (2, 1003),
        (4, 1003),
    ];
    for &(shard, hour) in above {
        recent_keys.insert(publish_at(inner.as_ref(), shard, hour, hour_mid_ns(hour)).await);
    }

    // Window covers the sealed region through the unsealed tail. Anchored on
    // now at hour 1003 so hours 1001..=1003 are the live suffix.
    let now_ns = i64::from(1003u32) * NS_PER_HOUR + 40 * 60_000_000_000;
    let range = TimeRange {
        start_ns: i64::from(998u32) * NS_PER_HOUR,
        end_ns: now_ns,
    };

    // Bounded path, through the recording store, with request accounting.
    let (recording, rec) = RecordingStore::new(inner.clone());
    let bounded = Catalog::new(recording, config(shard_count)).expect("catalog");
    let acc = QueryAccounting::new();
    let (snap, origins, _gens) = bounded
        .resolve_pruned_with_generations(&tenant(), Signal::Metrics, range, &[], now_ns, None, &acc)
        .await
        .expect("bounded resolve");

    // Every published record (sealed and recent) is visible, nothing else.
    let mut expected: Vec<String> = sealed_keys
        .iter()
        .chain(recent_keys.iter())
        .cloned()
        .collect();
    expected.sort();
    let mut got: Vec<String> = snap
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    got.sort();
    assert_eq!(
        got, expected,
        "bounded resolve must return every published segment"
    );

    // Origins: sealed keys came from the snapshot, recent keys from the live
    // listing. This is what dropping `start_after` would corrupt (a
    // below-watermark key listed live and tagged Recent).
    for (segment, origin) in snap.segments.iter().zip(origins.origins.iter()) {
        let key = &segment.data_object_key;
        if sealed_keys.contains(key) {
            assert_eq!(
                *origin,
                SegmentOrigin::SealedBelowWatermark,
                "sealed key must be served from the snapshot, not listed"
            );
        } else {
            assert_eq!(
                *origin,
                SegmentOrigin::Recent,
                "tail key must be listed live"
            );
        }
    }

    // Exactly 8 shard LISTs + 1 erasure LIST = 9, regardless of the empty
    // (shard, hour) buckets in the tail. The per-(shard, hour) loop would be
    // 8 shards * 3 hours + 1 = 25.
    assert_eq!(
        acc.snapshot().s3_requests(AccountedOp::List),
        9,
        "8 bounded shard LISTs + 1 erasure LIST"
    );
    assert_eq!(rec.total(), 9, "the store observed exactly 9 LISTs");

    // No below-watermark key is ever listed: the shard LISTs resume strictly
    // after the watermark via start_after, so no commit record for a sealed
    // hour is transferred. (Dropping start_after would make the sealed hours'
    // commit records appear in the listing.) The listed keys are commit-record
    // keys, so this checks the sealed hour strings, not the data-object keys in
    // `sealed_keys`.
    let sealed_hour_markers: Vec<String> = [998u32, 999, 1000]
        .iter()
        .map(|h| format!("/{}/", keys::ingest_hour_string(*h)))
        .collect();
    let listed = rec.returned_keys();
    assert!(!listed.is_empty(), "the tail must have been listed");
    for key in &listed {
        for marker in &sealed_hour_markers {
            assert!(
                !key.contains(marker.as_str()),
                "a below-watermark hour ({marker}) must never appear in the live listing: {key}"
            );
        }
    }

    // Cross-check: the independent prefix traversal returns the identical
    // snapshot and origins over the same corpus.
    let prefix = Catalog::new(inner.clone(), prefix_config(shard_count)).expect("catalog");
    let (snap_px, origins_px, _g) = prefix
        .resolve_pruned_with_generations(
            &tenant(),
            Signal::Metrics,
            range,
            &[],
            now_ns,
            None,
            &QueryAccounting::new(),
        )
        .await
        .expect("prefix resolve");
    assert_eq!(snap, snap_px, "bounded and prefix traversals must agree");
    assert_eq!(origins, origins_px, "origins must agree across traversals");
}

/// A bounded shard LIST pages across a small page size and stops at the first
/// key whose hour is past the window end.
#[tokio::test]
async fn bounded_shard_list_paginates_and_stops_at_window_end() {
    let inner = Arc::new(MemoryStore::with_page_size(2));
    // Shard 0: two in-window hours with two writers each, then a bucket past
    // the window end. Sorted key order within the shard is 1001*, 1002*, 1005*.
    publish_at(inner.as_ref(), 0, 1001, hour_mid_ns(1001)).await;
    publish_at(inner.as_ref(), 0, 1001, hour_mid_ns(1001) + 60_000_000_000).await;
    publish_at(inner.as_ref(), 0, 1002, hour_mid_ns(1002)).await;
    publish_at(inner.as_ref(), 0, 1002, hour_mid_ns(1002) + 60_000_000_000).await;
    // Past the window end (hour 1005 > 1002): must trigger the stop, and must
    // not be resolved into the snapshot.
    let past_end = publish_at(inner.as_ref(), 0, 1005, hour_mid_ns(1005)).await;
    publish_at(inner.as_ref(), 0, 1005, hour_mid_ns(1005) + 60_000_000_000).await;

    // No fold: the whole window is listed live. now anchored at hour 1002.
    let now_ns = i64::from(1002u32) * NS_PER_HOUR + 30 * 60_000_000_000;
    let range = TimeRange {
        start_ns: i64::from(1000u32) * NS_PER_HOUR,
        end_ns: now_ns,
    };

    let (recording, rec) = RecordingStore::new(inner.clone());
    let catalog = Catalog::new(recording, exact_config(1)).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve");

    assert_eq!(
        snapshot.segments.len(),
        4,
        "only the four in-window segments resolve; the hour-1005 buckets are past the window"
    );
    assert!(
        !snapshot
            .segments
            .iter()
            .any(|s| s.data_object_key == past_end),
        "the past-window bucket must not leak into the snapshot"
    );

    let shard_prefix = keys::commit_shard_prefix(&tenant(), Signal::Metrics, 0).unwrap();
    // Pages of size 2: [1001,1001], [1002,1002], then the page that first
    // yields 1005 (past the end) stops the scan. Three shard LISTs; without
    // the stop-at-end paging the scan would fetch a fourth (empty) page.
    assert_eq!(
        rec.count_for(&shard_prefix),
        3,
        "the bounded shard LIST pages and stops at the first past-window key"
    );
}

/// The pending-erasure LIST is issued concurrently with the shard fan-out:
/// all nine LISTs (8 shards + del) can be held simultaneously.
#[tokio::test]
async fn erasure_list_overlaps_shard_fanout() {
    let shard_count = 8u32;
    let inner = Arc::new(MemoryStore::new());
    // One record per shard, all in one unsealed hour, so every shard is listed.
    for shard in 0..shard_count {
        publish_at(inner.as_ref(), shard, 1001, hour_mid_ns(1001)).await;
    }

    // Hold every LIST call, unconditionally, before it reaches the backend.
    let store = Arc::new(FaultStore::new(
        Arc::clone(&inner) as Arc<dyn ObjectStoreBackend>,
        FaultPlan::empty(),
    ));
    let gate = store.hold(Op::List, None, Occurrence::Always);
    let catalog = Arc::new(Catalog::new(store, exact_config(shard_count)).expect("catalog"));

    let now_ns = i64::from(1001u32) * NS_PER_HOUR + 40 * 60_000_000_000;
    let range = TimeRange {
        start_ns: i64::from(1000u32) * NS_PER_HOUR,
        end_ns: now_ns,
    };

    let resolver = Arc::clone(&catalog);
    let handle = tokio::spawn(async move {
        resolver
            .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
            .await
    });

    // Concurrency: the erasure LIST and all 8 shard LISTs are in flight at
    // once, so nine calls are held simultaneously. If the erasure LIST were
    // awaited after the fan-out, holding the 8 shard LISTs would stall the
    // fan-out and the erasure LIST would never be issued, so this would time
    // out at eight.
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_until_held(9))
        .await
        .expect("all nine LISTs (8 shards + erasure) must be held concurrently");

    let details = gate.held_details();
    assert_eq!(details.len(), 9, "exactly nine LISTs held at once");
    let del_held = details
        .iter()
        .filter(|(_, _, key)| key.contains("/del/"))
        .count();
    assert_eq!(del_held, 1, "the erasure LIST is one of the nine held");

    // Release every held LIST and let the resolve finish.
    for id in gate.held() {
        gate.release(id);
    }
    let snapshot = handle.await.expect("join").expect("resolve");
    assert_eq!(
        snapshot.segments.len(),
        shard_count as usize,
        "every shard's record resolves once the LISTs are released"
    );
}
