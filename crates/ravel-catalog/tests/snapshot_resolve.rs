//! Integration tests for snapshot-backed resolve with listing
//! fallback (ADR-0020).
//!
//! Exercises `Catalog::resolve` against real `Catalog::fold` output built
//! through the public API (this is an external test, so it has no access to
//! crate-private helpers). Proves three things: resolving before a fold ever
//! ran (pure listing) and resolving after one (snapshot for sealed hours,
//! listing for the rest) return identical data for the same published
//! segments; every failure mode named in plan 5.3 degrades to listing rather
//! than an error or wrong data; and a stale HEAD cache only ever widens the
//! listed suffix, never breaks correctness.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use proptest::prelude::*;
use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_HEAD_CACHE_TTL_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0xef; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

/// `now_ns` at which ingest hour `hour` has just sealed under default
/// margins (mirrors `Catalog::fold`'s own seal rule, `fold.rs`'s
/// `now_at_seal`, not reachable from here since it is crate-private).
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

/// Reconstructs the HEAD object key the same way `fold::head_object_key`
/// does, from public `Signal`/`TenantHash` methods only.
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

fn full_window_range(start_hour: u32, now_ns: i64) -> TimeRange {
    TimeRange {
        start_ns: i64::from(start_hour) * NS_PER_HOUR,
        end_ns: now_ns,
    }
}

/// Build, PUT the data object, and publish a fully self-consistent commit
/// record for one segment. Each call uses a fresh writer id, so no two
/// segments ever collide on commit identity regardless of `seq`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    writer_id: Uuid,
    seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
) -> CommitRecord {
    let payload = format!("seg-{shard}-{writer_id}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
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
// Counting store: records every GET key and every LIST prefix, so tests can
// assert exactly which path (snapshot or listing) served a given hour.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RequestLog {
    gets: Arc<Mutex<Vec<String>>>,
    lists: Arc<Mutex<Vec<String>>>,
}

impl RequestLog {
    fn get_count_for(&self, needle: &str) -> usize {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.contains(needle))
            .count()
    }

    fn list_count_for(&self, needle: &str) -> usize {
        self.lists
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.contains(needle))
            .count()
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: RequestLog,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, RequestLog) {
        let log = RequestLog::default();
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
        self.log.gets.lock().unwrap().push(key.to_string());
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

// ---------------------------------------------------------------------------
// Snapshot-vs-listing equivalence
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// For any set of published segments spread across shards and sealed
    /// hours, `resolve` must return the exact same segment set whether it
    /// runs before any fold (pure Phase 1 listing) or after one (snapshot
    /// for every sealed hour, listing only for the trailing hot hour). The
    /// two paths share no code past `Catalog::resolve` itself, so agreement
    /// here is the load-bearing proof that folding is a pure optimization.
    #[test]
    fn resolve_after_fold_matches_resolve_before_fold(
        specs in proptest::collection::vec((0u32..3, 0u32..4, 0u64..1_000), 1..16)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let store = Arc::new(MemoryStore::new());
            let base_hour = 2_000u32;
            let now_ns = now_at_seal(base_hour + 3);

            for (i, (shard, hour_offset, seq)) in specs.iter().enumerate() {
                let hour = base_hour + hour_offset;
                let created = i64::from(hour) * NS_PER_HOUR + 100_000_000 + (i as i64) * 1_000;
                publish_segment(store.as_ref(), *shard, Uuid::new_v4(), *seq, hour, created).await;
            }

            let range = full_window_range(base_hour, now_ns);

            let before_catalog = Catalog::new(store.clone(), config(3)).expect("catalog");
            let before = before_catalog
                .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
                .await
                .expect("resolve before fold");

            let fold_catalog = Catalog::new(store.clone(), config(3)).expect("catalog");
            fold_catalog
                .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_ns, &[], None)
                .await
                .expect("fold");

            let after_catalog = Catalog::new(store.clone(), config(3)).expect("catalog");
            let after = after_catalog
                .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
                .await
                .expect("resolve after fold");

            assert_eq!(before, after);
        });
    }
}

// ---------------------------------------------------------------------------
// Snapshot path skips listing for sealed hours; hot hours are still listed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_watermark_serves_sealed_hours_without_listing() {
    let inner = Arc::new(MemoryStore::new());
    let base_hour = 3_000u32;
    let now_ns = now_at_seal(base_hour + 1);
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        base_hour,
        now_ns - 2 * NS_PER_HOUR,
    )
    .await;
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        2,
        base_hour + 1,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
    let report = fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(base_hour + 1));

    let (counting, log) = CountingStore::new(inner.clone());
    let catalog = Catalog::new(counting, config(1)).expect("catalog");
    let range = full_window_range(base_hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve");
    assert_eq!(snapshot.segments.len(), 2);

    let sealed_prefix_0 =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, base_hour).unwrap();
    let sealed_prefix_1 =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, base_hour + 1).unwrap();
    assert_eq!(
        log.list_count_for(&sealed_prefix_0),
        0,
        "hour at the watermark must never be listed"
    );
    assert_eq!(
        log.list_count_for(&sealed_prefix_1),
        0,
        "hour at the watermark must never be listed"
    );
    assert!(log.get_count_for("/HEAD") >= 1);
    assert!(log.get_count_for("/snap/") >= 1);
}

#[tokio::test]
async fn hours_above_watermark_are_listed_not_snapshot_served() {
    let inner = Arc::new(MemoryStore::new());
    let base_hour = 4_000u32;
    let now_ns = now_at_seal(base_hour);
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        base_hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");

    // Publish into the next, still-open hour after folding.
    let now_2 = now_ns + 100_000_000;
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        2,
        base_hour + 1,
        now_2 - 10_000_000,
    )
    .await;

    let (counting, log) = CountingStore::new(inner.clone());
    let catalog = Catalog::new(counting, config(1)).expect("catalog");
    let range = full_window_range(base_hour, now_2);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_2)
        .await
        .expect("resolve");
    assert_eq!(
        snapshot.segments.len(),
        2,
        "both the sealed and the still-open segment must be visible"
    );
    let hot_prefix =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, base_hour + 1).unwrap();
    assert!(
        log.list_count_for(&hot_prefix) >= 1,
        "the hour above the watermark must still be listed"
    );
}

// ---------------------------------------------------------------------------
// Every failure mode falls back to listing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn absent_head_falls_back_to_full_listing() {
    let inner = Arc::new(MemoryStore::new());
    let hour = 5_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let (counting, log) = CountingStore::new(inner);
    let catalog = Catalog::new(counting, config(1)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve falls back cleanly when no snapshot has ever been folded");
    assert_eq!(snapshot.segments.len(), 1);
    let prefix = keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, hour).unwrap();
    assert!(log.list_count_for(&prefix) >= 1);
}

#[tokio::test]
async fn corrupt_head_falls_back_to_full_listing() {
    let store = Arc::new(MemoryStore::new());
    let hour = 6_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        store.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");

    store
        .put(
            &head_key(&tenant(), Signal::Metrics),
            Bytes::from_static(b"not a head"),
            PutOptions::default(),
        )
        .await
        .expect("overwrite head with garbage");

    let catalog = Catalog::new(store, config(1)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve still succeeds by falling back to listing");
    assert_eq!(snapshot.segments.len(), 1);
}

#[tokio::test]
async fn corrupt_part_falls_back_to_full_listing() {
    let inner = MemoryStore::new();
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::CorruptRange).with_key_contains("/snap/"));
    let store = Arc::new(FaultStore::new(inner, plan));

    let hour = 7_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        store.inner(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("first fold never reads an existing part");

    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve falls back to listing when the part is corrupt");
    assert_eq!(snapshot.segments.len(), 1);
    assert!(store.fault_count(Op::Get, FaultKind::CorruptRange) >= 1);
}

#[tokio::test]
async fn part_notfound_race_recovers_after_one_head_reread() {
    let inner = MemoryStore::new();
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains("/snap/")
            .then_fault(ScriptedFault::NotFoundBlip)
            .then_passthrough(),
    );
    let store = Arc::new(FaultStore::new(inner, plan));

    let hour = 8_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        store.inner(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("first fold");

    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve recovers after the one allowed HEAD re-read");
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(
        store.sequence_progress(0),
        2,
        "exactly one retry: the first part GET failed, the re-read's part GET succeeded"
    );
}

#[tokio::test]
async fn second_part_notfound_after_head_reread_gives_up_without_further_retry() {
    let inner = MemoryStore::new();
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains("/snap/")
            .then_fault(ScriptedFault::NotFoundBlip)
            .then_fault(ScriptedFault::NotFoundBlip)
            .then_passthrough(),
    );
    let store = Arc::new(FaultStore::new(inner, plan));

    let hour = 9_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        store.inner(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("first fold");

    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve still returns correct data by falling back to listing");
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(
        store.sequence_progress(0),
        2,
        "at most one HEAD re-read is allowed: a second part NotFound must not trigger a third part GET"
    );
}

#[tokio::test]
async fn shard_count_mismatch_against_snapshot_head_is_a_loud_error() {
    let store = Arc::new(MemoryStore::new());
    let hour = 10_000u32;
    let now_ns = now_at_seal(hour);
    publish_segment(
        store.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        hour,
        now_ns - NS_PER_HOUR,
    )
    .await;

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");

    let mismatched = Catalog::new(store, config(2)).expect("catalog");
    let range = full_window_range(hour, now_ns);
    let err = mismatched
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect_err(
            "a shard_count mismatch against a HEAD this catalog is about to trust must be a hard error",
        );
    match err {
        CatalogError::FieldMismatch {
            field,
            expected,
            actual,
            ..
        } => {
            assert_eq!(field, "shard_count");
            assert_eq!(expected, "2");
            assert_eq!(actual, "1");
        }
        other => panic!("expected FieldMismatch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Stale HEAD cache widens the listed suffix but never breaks correctness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_head_cache_widens_listed_suffix_but_stays_correct() {
    let inner = Arc::new(MemoryStore::new());
    let base_hour = 11_000u32;
    let now_1 = now_at_seal(base_hour);
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        1,
        base_hour,
        now_1 - NS_PER_HOUR,
    )
    .await;

    let (counting, log) = CountingStore::new(inner.clone());
    let catalog = Catalog::new(counting, config(1)).expect("catalog");
    catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
        .await
        .expect("first fold seals base_hour");

    let range_1 = full_window_range(base_hour, now_1);
    let first = catalog
        .resolve(&tenant(), Signal::Metrics, range_1, &[], now_1)
        .await
        .expect("resolve populates the head cache at watermark base_hour");
    assert_eq!(first.segments.len(), 1);

    // A second fold advances the watermark in the store to base_hour + 1,
    // but the cache entry from the resolve above (cached at `now_1`) is
    // still within its TTL for a resolve shortly after.
    let hour_plus_one_prefix =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, base_hour + 1).unwrap();
    publish_segment(
        inner.as_ref(),
        0,
        Uuid::new_v4(),
        2,
        base_hour + 1,
        now_1 - 500_000_000,
    )
    .await;
    catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_at_seal(base_hour + 1),
            &[],
            None,
        )
        .await
        .expect("second fold advances the watermark in the store to base_hour + 1");

    let now_2 = now_1 + 10_000_000_000;
    let range_2 = full_window_range(base_hour, now_2);
    let second = catalog
        .resolve(&tenant(), Signal::Metrics, range_2, &[], now_2)
        .await
        .expect("resolve with a still-cached, now-stale HEAD");
    assert_eq!(
        second.segments.len(),
        2,
        "both segments are visible even though the cached HEAD is stale"
    );
    assert!(
        log.list_count_for(&hour_plus_one_prefix) >= 1,
        "the stale cached HEAD's watermark is one hour behind, so hour base_hour+1 must be listed"
    );

    let listed_before = log.list_count_for(&hour_plus_one_prefix);
    let now_3 = now_1 + DEFAULT_HEAD_CACHE_TTL_NS + 1;
    let range_3 = full_window_range(base_hour, now_3);
    let third = catalog
        .resolve(&tenant(), Signal::Metrics, range_3, &[], now_3)
        .await
        .expect("resolve after the head cache ttl expires");
    assert_eq!(third.segments.len(), 2);
    assert_eq!(
        log.list_count_for(&hour_plus_one_prefix),
        listed_before,
        "once the head cache refreshes to watermark base_hour + 1, that hour is snapshot-served"
    );
}
