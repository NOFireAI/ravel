//! Retention's version hold (issue #530, ADR-0066 decisions 1 and 2): the
//! physical sweep must not destroy an object merely because THIS build cannot
//! read its format version, and must still destroy one no build can read.
//!
//! The fixtures rewrite a real current-version object's trailer version field,
//! which is what a reader meets when a peer on the other side of a rolling
//! upgrade writes a version it does not admit: the version is read before
//! anything layout-dependent (docs/segment-format.md reader protocol step 2),
//! so no v6 decoder is reconstructed and none is needed.
//!
//! Each test asserts the exact surviving key set, not a count, and the exact
//! counter delta, not that it moved. The corrupt-object test is the mirror of
//! the held-object test: without it, a "fix" that simply stopped sweeping
//! anything would pass.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;

use std::collections::BTreeSet;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::record;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::retention::held_out_of_window_objects_total;
use ravel_maintain::{
    Bucket, CompactorConfig, FixedClock, NoLeases, RetentionConfig, RetentionOutcome,
    RetentionPolicy, retention_sweep_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_segment::TRAILER_LEN;
use ravel_types::Signal;
use uuid::Uuid;

/// Serializes the tests that assert on the process-wide hold counter, so each
/// one's before/after delta covers only its own sweep. Every test in this
/// binary runs a sweep, so all of them take it.
static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const TRAILER_LEN_USIZE: usize = TRAILER_LEN as usize;
/// Byte offset of the trailer's `version: u16` within the 16-byte trailer
/// (footer_len 4 + footer_crc32c 4), docs/segment-format.md.
const VERSION_OFFSET_IN_TRAILER: usize = 8;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// A retention config whose window for the test tenant is exactly the floor, so
/// the tiny-timestamp fixtures are always expired.
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

/// Seed two L0 metric inputs and return `(bucket, commit keys)`.
async fn seed_two_metrics(store: &dyn ObjectStoreBackend) -> (Bucket, Vec<String>) {
    let specs = vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(1_000, 1.0), (2_000, 2.0)],
            )],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ];
    let mut commit_keys = Vec::new();
    for spec in &specs {
        commit_keys.push(seed_input(store, spec).await);
    }
    (bucket(), commit_keys)
}

/// The L0 data-object key each commit record names, in commit-key order.
async fn data_keys(store: &dyn ObjectStoreBackend, commit_keys: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for key in commit_keys {
        let got = store
            .get(key, GetRange::Full)
            .await
            .expect("commit record present");
        let rec = record::decode(&got.data).expect("commit record decodes");
        out.push(keys::reconstruct_data_key(&rec).expect("data key"));
    }
    out
}

/// Overwrite one stored object's trailer version field in place.
async fn stamp_version(store: &dyn ObjectStoreBackend, key: &str, version: u16) {
    let got = store
        .get(key, GetRange::Full)
        .await
        .expect("object present");
    let mut bytes = got.data.to_vec();
    let at = bytes.len() - TRAILER_LEN_USIZE + VERSION_OFFSET_IN_TRAILER;
    bytes[at..at + 2].copy_from_slice(&version.to_le_bytes());
    store
        .put(key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("overwrite");
}

/// Smash one stored object's trailer magic: corruption no build can read.
async fn smash_magic(store: &dyn ObjectStoreBackend, key: &str) {
    let got = store
        .get(key, GetRange::Full)
        .await
        .expect("object present");
    let mut bytes = got.data.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    store
        .put(key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("overwrite");
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .collect()
}

/// Tombstone the bucket, then return the clock advanced past the protection
/// horizon and the exact key set present at that moment.
async fn tombstone_and_arm(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    config: &CompactorConfig,
    retention: &RetentionConfig,
) -> (FixedClock, BTreeSet<String>) {
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let out = retention_sweep_bucket(store, &clock, config, retention, &NoLeases, bucket)
        .await
        .expect("tombstone pass");
    assert_eq!(out, RetentionOutcome::Tombstoned);
    clock.set(created + config.protection_horizon_ns + 1);
    let armed = all_keys(store).await;
    (clock, armed)
}

/// Control: with every object readable here, the horizon-elapsed sweep empties
/// the bucket and holds nothing. Without this, the two tests below could both
/// pass on a build that never sweeps at all.
#[tokio::test]
async fn a_readable_bucket_is_swept_and_holds_nothing() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, _commit_keys) = seed_two_metrics(&store).await;
    let (clock, armed) = tombstone_and_arm(&store, &bucket, &config, &retention).await;
    assert!(!armed.is_empty(), "the fixture really seeded objects");

    let before = held_out_of_window_objects_total();
    let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(swept, RetentionOutcome::Swept);
    assert_eq!(
        all_keys(&store).await,
        BTreeSet::new(),
        "an expired bucket of readable objects is fully retired"
    );
    assert_eq!(
        held_out_of_window_objects_total() - before,
        0,
        "nothing was held"
    );
}

/// One data object at a version this build does not admit holds the whole
/// bucket: nothing is deleted, the tombstone stays, and the surviving key set
/// is exactly what was there before the sweep.
#[tokio::test]
async fn an_out_of_window_object_holds_the_bucket() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, commit_keys) = seed_two_metrics(&store).await;
    let data = data_keys(&store, &commit_keys).await;
    stamp_version(&store, &data[0], 8).await;

    let (clock, armed) = tombstone_and_arm(&store, &bucket, &config, &retention).await;
    let before = held_out_of_window_objects_total();
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(
        out,
        RetentionOutcome::SweptPartial,
        "the bucket is held, not retired"
    );
    assert_eq!(
        all_keys(&store).await,
        armed,
        "every key survives: the held object, the other object, both commit records, the tombstone"
    );
    assert_eq!(
        held_out_of_window_objects_total() - before,
        1,
        "exactly the one out-of-window object was counted"
    );
}

/// Two out-of-window objects in one bucket count two. Pins that the counter
/// counts objects, not passes or buckets.
#[tokio::test]
async fn the_counter_counts_every_held_object() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, commit_keys) = seed_two_metrics(&store).await;
    let data = data_keys(&store, &commit_keys).await;
    stamp_version(&store, &data[0], 8).await;
    stamp_version(&store, &data[1], 9).await;

    let (clock, armed) = tombstone_and_arm(&store, &bucket, &config, &retention).await;
    let before = held_out_of_window_objects_total();
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(all_keys(&store).await, armed);
    assert_eq!(held_out_of_window_objects_total() - before, 2);
}

/// The mirror: an object no build can read is still swept. Corruption is not a
/// reason to retain data past its window, and a hold that fired here would
/// leave a bucket that can never be retired.
#[tokio::test]
async fn a_corrupt_object_is_still_swept() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let (bucket, commit_keys) = seed_two_metrics(&store).await;
    let data = data_keys(&store, &commit_keys).await;
    smash_magic(&store, &data[0]).await;

    let (clock, _armed) = tombstone_and_arm(&store, &bucket, &config, &retention).await;
    let before = held_out_of_window_objects_total();
    let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(swept, RetentionOutcome::Swept);
    assert_eq!(
        all_keys(&store).await,
        BTreeSet::new(),
        "the corrupt object is destroyed with the rest of the expired bucket"
    );
    assert_eq!(
        held_out_of_window_objects_total() - before,
        0,
        "corruption is not a hold"
    );
}

/// The bucket's signal decides whether the RSEG gate applies at all. An RLOG
/// (logs) bucket is swept on today's rules: its objects carry RLOG trailers,
/// and probing those with the RSEG gate would call every one of them corrupt,
/// which is the collapse this change exists to prevent. Pinned so the day
/// ravel-logseg grows the same probe, this test is what says the gate was
/// widened deliberately.
#[tokio::test]
async fn a_logs_bucket_keeps_todays_sweep() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let bucket = seed_rlog_two_inputs(&store).await;
    assert_eq!(bucket.signal, Signal::Logs);

    let (clock, _armed) = tombstone_and_arm(&store, &bucket, &config, &retention).await;
    let before = held_out_of_window_objects_total();
    let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(swept, RetentionOutcome::Swept);
    assert_eq!(all_keys(&store).await, BTreeSet::new());
    assert_eq!(held_out_of_window_objects_total() - before, 0);
}
