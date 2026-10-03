//! Regression test: the shard buffer
//! must fail loud when two points share one `series_id` under distinct
//! canonical label sets, never silently merge them (ADR-0005 "Decision").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::record;
use ravel_ingest::{IngestConfig, IngestRouter, WriteError, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first point (`target_bytes: 8`) and never on age, so a
/// strict write drives one complete flush inline and returns its outcome.
fn flush_on_first_point(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// Reads back the single commit record in `store` and returns
/// `(series_count, sample_count)` as the writer recorded them.
async fn commit_record_counts(store: &dyn ObjectStoreBackend) -> (u64, u64) {
    let objects = list_all(store, "t/").await.expect("list");
    let commit_key = objects
        .iter()
        .find(|o| o.key.contains("/c/"))
        .expect("exactly one commit record")
        .key
        .clone();
    let raw = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record");
    let decoded = record::decode(&raw.data).expect("decode commit record");
    (decoded.series_count, decoded.sample_count)
}

/// Two points with the same `series_id` but genuinely different label sets
/// must be rejected with a typed error, counted, and must leave nothing in
/// the store. Before the fix this write succeeded and the losing series'
/// sample was merged under the winning label set.
#[tokio::test]
async fn colliding_series_ids_are_rejected_loud() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let first = make_point(&tenant, "cpu_usage", &[("host", "a")], 1_000, 1.0);
    let mut colliding = make_point(&tenant, "cpu_usage", &[("host", "b")], 2_000, 2.0);
    // Forge the collision: same id, genuinely different label set (host=a vs
    // host=b). A real BLAKE3-128 collision is ~2^64 work; the point of the
    // finding is that nothing on the path would notice one.
    colliding.series_id = first.series_id;
    assert_ne!(
        first.labels, colliding.labels,
        "the two points must carry distinct label sets for this to be a collision"
    );

    let err = router
        .write(
            tenant.clone(),
            vec![first, colliding],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the colliding batch must fail loud, not merge silently");
    assert!(
        matches!(err, WriteError::SeriesIdCollision(_)),
        "expected a typed SeriesIdCollision, got {err:?}"
    );
    assert!(
        !err.is_retryable(),
        "a collision is a deterministic input problem, not retryable"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.series_id_collisions, 1,
        "the collision must increment its counter"
    );

    // Fail-loud means the batch is abandoned before any flush: nothing is
    // durably stored under the winning label set.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "a rejected collision must not write any object, found {objects:?}"
    );

    router.shutdown().await;
}

/// Two points with the same `series_id` and equal label sets are the normal
/// case: they must still merge into one stored series carrying both samples,
/// with no collision counted. Guards against the fix over-rejecting.
#[tokio::test]
async fn equal_label_sets_still_merge() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let first = make_point(&tenant, "cpu_usage", &[("host", "a")], 1_000, 1.0);
    let second = make_point(&tenant, "cpu_usage", &[("host", "a")], 2_000, 2.0);
    assert_eq!(
        first.series_id, second.series_id,
        "same tenant/metric/labels derive the same series id"
    );
    assert_eq!(first.labels, second.labels);

    let receipt = router
        .write(
            tenant.clone(),
            vec![first, second],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("non-colliding points merge and commit normally");
    assert_eq!(receipt.tokens.len(), 1);

    let (series_count, sample_count) = commit_record_counts(store.as_ref()).await;
    assert_eq!(series_count, 1, "one label set, one stored series");
    assert_eq!(sample_count, 2, "both samples merged into that series");

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.series_id_collisions, 0,
        "equal label sets are not a collision"
    );

    router.shutdown().await;
}
