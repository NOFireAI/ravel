//! The retention sweep's verifying LIST must count a key the store cannot
//! address as residue (issue #4260).
//!
//! The physical sweep retires a bucket's tombstone only after a fresh LIST
//! shows the bucket's commit prefix holds nothing but the tombstone and its
//! `l1/` prefix is empty. A listing skips an unaddressable key (ADR-2637), so a
//! check that reads the skipped key's absence as "empty" retires the tombstone
//! while an object remains. Each test here plants one such key with
//! `MemoryStore::insert_foreign`, the way a writer outside Ravel would, and
//! asserts the tombstone survives, the counter moves by exactly one, and the
//! planted key is still there.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;

use std::collections::BTreeSet;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::retention::unaddressable_held_buckets_total;
use ravel_maintain::{
    Bucket, Clock, CompactorConfig, FixedClock, NoLeases, RetentionConfig, RetentionOutcome,
    RetentionPolicy, retention_sweep_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, is_addressable_key, list_all_reporting};
use uuid::Uuid;

/// Serializes every test in this binary, so each one's before/after delta of
/// the process-wide counter covers only its own sweeps.
static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The pinned-query window (ADR-1133) zeroed, so a candidate's unnamed-since
/// marker is written and clears in the same pass.
fn cfg() -> CompactorConfig {
    CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..CompactorConfig::default()
    }
}

/// A retention window for the test tenant at exactly the floor, so the
/// tiny-timestamp fixture is always expired.
fn retention_at_floor(config: &CompactorConfig) -> RetentionConfig {
    let floor = config.retention_floor_ns(DEFAULT_MAX_INGEST_LAG_NS);
    RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor)],
        },
        config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("valid retention config")
}

fn commit_prefix(bucket: &Bucket) -> String {
    keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("commit prefix")
}

/// The bucket's `l1/` prefix, spelled out so the test does not borrow the
/// helper the code under test uses.
fn l1_prefix(bucket: &Bucket) -> String {
    format!(
        "t/{}/{}/l1/{:04}/{}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard,
        keys::ingest_hour_string(bucket.ingest_hour_bucket),
    )
}

/// A key under `prefix` the store cannot address: `*` is percent-encoded by
/// the adapter, so a request for it would reach a different key.
fn foreign_key(prefix: &str) -> String {
    let key = format!("{prefix}stray*");
    assert!(
        !is_addressable_key(&key),
        "the planted key is unaddressable"
    );
    key
}

/// Seed one L0 metrics input, tombstone the bucket, and advance the clock past
/// the protection horizon, so the next pass runs the physical sweep.
async fn armed_bucket(
    store: &MemoryStore,
    config: &CompactorConfig,
    retention: &RetentionConfig,
) -> (Bucket, FixedClock) {
    let spec = InputSpec::new(
        Uuid::from_u128(1),
        10,
        1,
        vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
    );
    seed_input(store, &spec).await;
    let bucket = bucket();
    let clock = FixedClock::new(sealed_now_ns());
    let created = clock.now_ns();
    let out = retention_sweep_bucket(store, &clock, config, retention, &NoLeases, &bucket)
        .await
        .expect("tombstone pass");
    assert_eq!(out, RetentionOutcome::Tombstoned);
    clock.set(created + config.protection_horizon_ns + 1);
    (bucket, clock)
}

/// Every key under the tenant, addressable or not. Unnamed-since markers are
/// left out: the sweep writes and retires one on its own schedule.
async fn tenant_keys(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> BTreeSet<String> {
    let listing = list_all_reporting(store, &format!("t/{}/", bucket.tenant_hash.to_hex()))
        .await
        .expect("list");
    assert_eq!(
        listing.unaddressable.count as usize,
        listing.unaddressable.sample.len(),
        "the sample holds every skipped key"
    );
    listing
        .objects
        .into_iter()
        .map(|meta| meta.key)
        .chain(listing.unaddressable.sample.into_iter().map(|k| k.key))
        .filter(|key| !key.contains("/maint/unn/"))
        .collect()
}

fn tombstones(keys_present: &BTreeSet<String>) -> Vec<&String> {
    keys_present
        .iter()
        .filter(|key| {
            matches!(
                keys::partition_bucket_entry(key),
                Ok(keys::BucketEntry::Tombstone(_))
            )
        })
        .collect()
}

/// Plant one unaddressable key under `prefix` of an armed bucket and sweep it:
/// the sweep deletes every addressable object but keeps the tombstone, and the
/// counter moves by exactly one.
async fn assert_unaddressable_key_holds_tombstone(prefix_of: fn(&Bucket) -> String) {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, clock) = armed_bucket(&store, &config, &retention).await;
    let stray = foreign_key(&prefix_of(&bucket));
    store.insert_foreign(&stray, Bytes::from_static(b"foreign"));
    let armed = tenant_keys(&store, &bucket).await;
    let tombstone = tombstones(&armed);
    assert_eq!(tombstone.len(), 1, "the fixture is tombstoned: {armed:?}");
    let tombstone = tombstone[0].clone();

    let before = unaddressable_held_buckets_total();
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(
        tenant_keys(&store, &bucket).await,
        BTreeSet::from([tombstone.clone(), stray.clone()]),
        "the tombstone and the unaddressable key survive; everything addressable is swept"
    );
    assert_eq!(
        unaddressable_held_buckets_total() - before,
        1,
        "one held bucket counts one"
    );

    // The bucket is revisited, not parked: the next pass holds it again and
    // counts again.
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("second sweep pass");
    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(
        tenant_keys(&store, &bucket).await,
        BTreeSet::from([tombstone, stray]),
    );
    assert_eq!(
        unaddressable_held_buckets_total() - before,
        2,
        "each pass that holds the bucket counts it once"
    );
}

#[tokio::test]
async fn an_unaddressable_key_under_the_commit_prefix_keeps_the_tombstone() {
    assert_unaddressable_key_holds_tombstone(commit_prefix).await;
}

#[tokio::test]
async fn an_unaddressable_key_under_the_l1_prefix_keeps_the_tombstone() {
    assert_unaddressable_key_holds_tombstone(l1_prefix).await;
}

/// Unaddressable keys under both prefixes still count the bucket once per pass.
#[tokio::test]
async fn unaddressable_keys_under_both_prefixes_count_the_bucket_once() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, clock) = armed_bucket(&store, &config, &retention).await;
    store.insert_foreign(
        &foreign_key(&commit_prefix(&bucket)),
        Bytes::from_static(b"f"),
    );
    store.insert_foreign(&foreign_key(&l1_prefix(&bucket)), Bytes::from_static(b"f"));

    let before = unaddressable_held_buckets_total();
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(tombstones(&tenant_keys(&store, &bucket).await).len(), 1);
    assert_eq!(unaddressable_held_buckets_total() - before, 1);
}

/// Control: the same bucket with no unaddressable key is retired whole, and
/// the counter does not move. Without this, the tests above could pass on a
/// build that never retires a tombstone.
#[tokio::test]
async fn a_bucket_empty_but_its_tombstone_is_still_retired() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, clock) = armed_bucket(&store, &config, &retention).await;
    let armed = tenant_keys(&store, &bucket).await;
    assert_eq!(tombstones(&armed).len(), 1, "the fixture is tombstoned");
    assert!(armed.len() > 1, "the fixture holds objects to sweep");

    let before = unaddressable_held_buckets_total();
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::Swept);
    assert_eq!(tenant_keys(&store, &bucket).await, BTreeSet::new());
    assert_eq!(unaddressable_held_buckets_total() - before, 0);
}
