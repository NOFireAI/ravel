//! Regression test for issue #64 (audit finding a8-F02): dropping the router
//! without calling `shutdown()` must not silently discard buffered points.
//!
//! Adapted from the a8-F02 reproducer on branch `audit/repro-a8`
//! (`crates/ravel-ingest/tests/audit_a8_repro.rs`). The reproducer asserted
//! the *defective* behavior (store still empty after the drop); this test
//! asserts the fixed behavior: the channel-close arm of `ShardActor::run`
//! flushes buffered tenants before stopping, so the acknowledged buffered-mode
//! point reaches the store durably.
//!
//! docs/consistency-model.md "Buffered mode": buffered points may be lost only
//! to "a crash between ack and flush". Dropping the router is a graceful
//! teardown, not a crash, so the point must survive it.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::record;
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

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

#[tokio::test]
async fn dropping_router_flushes_buffered_points() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Neither size nor age can trigger a flush on its own: the point
        // stays buffered until the channel-close arm flushes it.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    // A buffered ack fires at enqueue, so wait for the actor to actually take
    // the message off the channel and buffer it before dropping the router;
    // otherwise the drop could race the actor and prove nothing.
    for _ in 0..100 {
        if router.metrics().snapshot().buffered_points_total == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        router.metrics().snapshot().buffered_points_total,
        1,
        "the point must be buffered in the actor before the router is dropped"
    );

    // Drop instead of `shutdown()`. Production reaches this path whenever
    // `Arc::try_unwrap` fails or `Running::shutdown` returns early via `?`
    // (services/ravel-server/src/lib.rs).
    drop(router);

    // Give the detached actor task time to notice the closed channel, flush,
    // and stop. The MemoryStore PUTs complete promptly.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The observable outcome: the buffered point was flushed, not silently
    // discarded. A data object and its commit record are now in the store.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        !objects.is_empty(),
        "issue #64: dropping the router must flush buffered points, but the \
         store is empty"
    );
    assert!(
        objects.iter().any(|o| o.key.contains("/c/")),
        "issue #64: a commit record must exist after the graceful-close flush; \
         found only {objects:?}"
    );

    // Never ack more than was durably committed: the one buffered sample of
    // the one buffered series is present in the flushed segment.
    let (series_count, sample_count) = commit_record_counts(store.as_ref()).await;
    assert_eq!(series_count, 1, "one buffered series was flushed");
    assert_eq!(sample_count, 1, "the one buffered sample was flushed");
}
