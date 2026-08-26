//! Issue #783: a repeated resolve over an unsealed ingest hour costs its
//! LISTs and nothing else.
//!
//! The newest `max_flush_lifetime + clock_skew_allowance +
//! fold_safety_margin` of any tenant is always unsealed, so no snapshot part
//! covers it and every query resolves it by listing the buckets and reading
//! each commit record. The LIST must run every time (it is what discovers new
//! records); the per-record GET must not, because a commit record is
//! immutable once published. These tests pin that split by exact GET counts
//! against a store double that records every key, and pin what the bound and
//! a mid-resolve fault do to it.
//!
//! The red lever for four of these is one line: the `return Ok(cached)` in
//! `Catalog::load_and_validate`'s cache-first branch. With it removed,
//! `repeat_resolve_over_unsealed_buckets_costs_the_lists_only` and
//! `a_bound_at_the_hot_region_size_keeps_the_saving` report 24 record GETs
//! where they demand 12 (both of `process_bucket`'s passes re-read every
//! record), `record_published_between_resolves_is_fetched_exactly_once`
//! likewise reports 24 for 12, and
//! `a_faulted_record_get_leaves_the_other_records_cached` names all 24 reads
//! where it demands the one faulted key.
//! `cache_disabled_refetches_every_record_every_resolve` pins that same red
//! permanently through the `0` bound, and
//! `a_bound_below_the_hot_region_loses_the_saving` is red at a bound of 12.
//!
//! Which entry a full cache evicts is an LRU question, not a resolve-cost
//! one; `lru_evicts_the_least_recently_used` and `zero_capacity_admits_nothing`
//! in `src/cache.rs` pin it deterministically.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig, CatalogError};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// First of the two unsealed ingest hours every test below publishes into.
const HOUR_A: u32 = 800_000;
const HOUR_B: u32 = HOUR_A + 1;

fn tenant() -> TenantHash {
    TenantHash([0x5c; 16])
}

/// A single shard so the listing fan-out is one bounded LIST, and the default
/// record-cache bound unless a test overrides it.
fn config(cache_capacity_per_tenant: usize) -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        cache_capacity_per_tenant,
        ..Default::default()
    }
}

/// The `now_ns` every test resolves at: mid-way through `HOUR_B`, so both
/// hours sit inside the query window and neither is sealed.
fn now_ns() -> i64 {
    i64::from(HOUR_B) * NS_PER_HOUR + 30 * 60_000_000_000
}

fn window() -> TimeRange {
    TimeRange {
        start_ns: i64::from(HOUR_A) * NS_PER_HOUR,
        end_ns: now_ns(),
    }
}

/// True for an L0 commit-record key (`<writer_id>.<epoch>.<seq>.cmt`), false
/// for the compaction (`l1.`) and rewrite (`rw.`) record shapes that share the
/// suffix, and for every other object the resolve reads (HEAD, the
/// provisioning record, data objects). This is the per-record GET issue #783
/// counts.
fn is_commit_record_key(key: &str) -> bool {
    let file = key.rsplit('/').next().unwrap_or(key);
    file.ends_with(".cmt") && !file.starts_with("l1.") && !file.starts_with("rw.")
}

/// Every GET and LIST the catalog issued, by key, so a test can assert both
/// how many record GETs a resolve made and exactly which records they named.
#[derive(Clone, Default)]
struct Calls {
    gets: Arc<Mutex<Vec<String>>>,
    lists: Arc<Mutex<Vec<String>>>,
}

impl Calls {
    fn record_gets(&self) -> Vec<String> {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|k| is_commit_record_key(k))
            .cloned()
            .collect()
    }

    fn record_get_count(&self) -> usize {
        self.record_gets().len()
    }

    fn list_count(&self) -> usize {
        self.lists.lock().unwrap().len()
    }

    fn reset(&self) {
        self.gets.lock().unwrap().clear();
        self.lists.lock().unwrap().clear();
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    calls: Calls,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, Calls) {
        let calls = Calls::default();
        (
            Arc::new(CountingStore {
                inner,
                calls: calls.clone(),
            }),
            calls,
        )
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.calls.gets.lock().unwrap().push(k.to_string());
        self.inner.get(k, r).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.calls.lists.lock().unwrap().push(p.to_string());
        self.inner.list(p, t).await
    }
    async fn list_after(
        &self,
        p: &str,
        s: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.calls.lists.lock().unwrap().push(p.to_string());
        self.inner.list_after(p, s, t).await
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

/// Publish one L0 segment (data object plus commit record) under a writer
/// identity derived from `index`, and return its commit-record key.
///
/// `Uuid::from_u128(index + 1)` for `index` below 12 renders as a
/// hex-ascending uuid, and a commit key ends
/// `<writer_id>.<epoch>.<seq:020>.cmt`, so listing order inside a bucket is
/// exactly ascending `index`. Every test below relies on that to name "the
/// n-th record".
async fn publish(store: &dyn ObjectStoreBackend, hour: u32, index: u64) -> String {
    let payload = format!("seg-{hour}-{index}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let event_ts_ns = i64::from(hour) * NS_PER_HOUR + 60_000_000_000 * (index as i64 + 1);
    let writer_id = Uuid::from_u128(u128::from(index) + 1);
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index,
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
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    keys::commit_key_for_record(&rec).expect("commit key")
}

/// The twelve-record, two-unsealed-bucket fixture: records 0..6 in `HOUR_A`,
/// records 6..12 in `HOUR_B`, returned in listing order.
async fn publish_twelve(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut keys = Vec::with_capacity(12);
    for index in 0..6u64 {
        keys.push(publish(store, HOUR_A, index).await);
    }
    for index in 6..12u64 {
        keys.push(publish(store, HOUR_B, index).await);
    }
    keys
}

async fn resolve(catalog: &Catalog) -> Result<ravel_catalog::Snapshot, CatalogError> {
    catalog
        .resolve_with_accounting(
            &tenant(),
            Signal::Metrics,
            window(),
            &[],
            now_ns(),
            &QueryAccounting::new(),
        )
        .await
}

/// The property issue #783 asks for: over two unsealed buckets holding twelve
/// commit records, the first resolve pays one GET per record and the second
/// pays none, while both pay the same LISTs and return the identical segment
/// set.
#[tokio::test]
async fn repeat_resolve_over_unsealed_buckets_costs_the_lists_only() {
    let inner = Arc::new(MemoryStore::new());
    publish_twelve(inner.as_ref()).await;
    let (store, calls) = CountingStore::new(inner);
    let catalog = Catalog::new(store, config(10_000)).expect("catalog");

    let first = resolve(&catalog).await.expect("first resolve");
    let first_gets = calls.record_get_count();
    let first_lists = calls.list_count();
    calls.reset();

    let second = resolve(&catalog).await.expect("second resolve");
    let second_gets = calls.record_get_count();
    let second_lists = calls.list_count();

    assert_eq!(
        first_gets, 12,
        "a cold resolve must GET each of the twelve commit records exactly once"
    );
    assert_eq!(
        second_gets, 0,
        "a repeated resolve over the same unsealed buckets must issue no record GET"
    );
    assert_eq!(
        (first_lists, second_lists),
        (2, 2),
        "both resolves must still pay the same LISTs (one bounded shard LIST plus the \
         pending-erasure LIST): the LIST is what discovers new records"
    );
    assert_eq!(
        first, second,
        "the cached resolve must return the identical snapshot, by value"
    );
    assert_eq!(first.segments.len(), 12);
}

/// The `0` bound disables the cache, which is this file's standing red for
/// the test above: every resolve then re-reads every record. Twenty-four, not
/// twelve, because `process_bucket` reads each record twice with nothing
/// cached in between (the concurrent `prewarm_commit_records` pass, then the
/// sequential include pass).
///
/// This asserts the resolve cost of a disabled cache, which a bound of 1
/// would also produce; that `0` specifically admits nothing rather than
/// silently meaning 1 is pinned by `zero_capacity_admits_nothing` in
/// `src/cache.rs`.
#[tokio::test]
async fn cache_disabled_refetches_every_record_every_resolve() {
    let inner = Arc::new(MemoryStore::new());
    publish_twelve(inner.as_ref()).await;
    let (store, calls) = CountingStore::new(inner);
    let catalog = Catalog::new(store, config(0)).expect("catalog");

    let first = resolve(&catalog).await.expect("first resolve");
    let first_gets = calls.record_get_count();
    calls.reset();
    let second = resolve(&catalog).await.expect("second resolve");

    assert_eq!(first_gets, 24);
    assert_eq!(
        calls.record_get_count(),
        24,
        "with the cache disabled the second resolve must re-read every record"
    );
    assert_eq!(
        first, second,
        "the cache is an optimization only: disabling it must not change the snapshot"
    );
}

/// A record published between two resolves is the one thing the LIST is for.
/// It costs exactly one GET, and the twelve already cached cost none.
#[tokio::test]
async fn record_published_between_resolves_is_fetched_exactly_once() {
    let inner = Arc::new(MemoryStore::new());
    publish_twelve(inner.as_ref()).await;
    let (store, calls) = CountingStore::new(inner.clone());
    let catalog = Catalog::new(store, config(10_000)).expect("catalog");

    let first = resolve(&catalog).await.expect("first resolve");
    assert_eq!(calls.record_get_count(), 12);
    calls.reset();

    let thirteenth = publish(inner.as_ref(), HOUR_B, 12).await;

    let second = resolve(&catalog).await.expect("second resolve");
    assert_eq!(
        calls.record_gets(),
        vec![thirteenth],
        "only the newly published record may be fetched; the twelve cached ones must not be"
    );
    assert_eq!(first.segments.len(), 12);
    assert_eq!(
        second.segments.len(),
        13,
        "the LIST must still discover the new record"
    );
}

/// The bound is enforced: set below the hot region's size it cannot hold it,
/// so the saving disappears and every resolve pays full price again.
///
/// The exact count is 24 per resolve, not 8, and the shape is worth stating.
/// With a bound below a bucket's record count, `process_bucket`'s two passes
/// evict each other: the concurrent prewarm admits all six of a bucket's
/// records and keeps the four it touched last, then the sequential include
/// pass walks all six in listing order, missing on the ones the prewarm
/// dropped and evicting the ones it kept as it refills. Neither pass ever
/// serves the other, so both buckets pay 6 + 6. Sizing the bound at or above
/// the hot region (the default, 10,000 entries) is what avoids this: raising
/// this test's bound to 12 takes the second resolve to 0.
///
/// Which four entries a bound of 4 retains is an LRU question, pinned
/// directly and deterministically by `lru_evicts_the_least_recently_used`
/// in `src/cache.rs`; this test pins the resolve-level cost of a bound that
/// does not fit.
#[tokio::test]
async fn a_bound_below_the_hot_region_loses_the_saving() {
    let inner = Arc::new(MemoryStore::new());
    publish_twelve(inner.as_ref()).await;
    let (store, calls) = CountingStore::new(inner);
    let catalog = Catalog::new(store, config(4)).expect("catalog");

    let first = resolve(&catalog).await.expect("first resolve");
    assert_eq!(calls.record_get_count(), 24);
    calls.reset();

    let second = resolve(&catalog).await.expect("second resolve");
    let second_gets = calls.record_get_count();
    assert_eq!(
        second_gets, 24,
        "a bound of 4 cannot hold the twelve-record hot set, so every pass misses"
    );
    assert!(
        second_gets >= 8,
        "a cache holding at most 4 of 12 can never serve more than 4 of a pass's reads"
    );
    assert_eq!(
        first, second,
        "eviction is a cost, never a correctness change: the snapshot is identical"
    );
}

/// The same fixture at a bound that does fit: the control for the test above,
/// so its 24 reads as "the bound was too small", not "the cache never works".
#[tokio::test]
async fn a_bound_at_the_hot_region_size_keeps_the_saving() {
    let inner = Arc::new(MemoryStore::new());
    publish_twelve(inner.as_ref()).await;
    let (store, calls) = CountingStore::new(inner);
    let catalog = Catalog::new(store, config(12)).expect("catalog");

    resolve(&catalog).await.expect("first resolve");
    assert_eq!(calls.record_get_count(), 12);
    calls.reset();
    resolve(&catalog).await.expect("second resolve");
    assert_eq!(calls.record_get_count(), 0);
}

/// A GET fault on the fifth record's key fails the first resolve with the
/// existing typed store error, and leaves every record whose GET did complete
/// in the cache. The second resolve (fault spent) re-reads only what the
/// first did not cache.
///
/// One GET, not eight: `prewarm_commit_records` issues all six of the
/// bucket's GETs concurrently and only then returns the first error, so the
/// five that succeeded are already cached and only the faulted record is
/// missing. The second resolve's prewarm re-reads exactly that one, and its
/// include pass is served from the cache.
#[tokio::test]
async fn a_faulted_record_get_leaves_the_other_records_cached() {
    let inner = Arc::new(MemoryStore::new());
    let keys = publish_twelve(inner.as_ref()).await;
    let fifth = keys[4].clone();

    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent("record GET failed".into()),
        )
        .with_key_contains(fifth.clone())
        .with_occurrence(Occurrence::Nth(1)),
    );
    let faulting = Arc::new(FaultStore::new(inner, plan));
    let (store, calls) = CountingStore::new(faulting.clone());
    let catalog = Catalog::new(store, config(10_000)).expect("catalog");

    let err = resolve(&catalog)
        .await
        .expect_err("a faulted record GET must fail the resolve, never yield a partial snapshot");
    assert!(
        matches!(err, CatalogError::Store(StoreError::Permanent(_))),
        "expected the existing typed store error, got {err:?}"
    );
    assert_eq!(
        faulting.fault_count(Op::Get, FaultKind::Permanent),
        1,
        "the fault must actually have fired"
    );
    calls.reset();

    let second = resolve(&catalog)
        .await
        .expect("second resolve, fault spent");
    let second_gets = calls.record_gets();
    assert_eq!(
        second_gets,
        vec![fifth],
        "only the record whose GET faulted may be re-read; the eleven cached ones must not be"
    );
    assert_eq!(second.segments.len(), 12);
}
