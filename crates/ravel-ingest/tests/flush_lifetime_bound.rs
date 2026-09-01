//! Regression test: the shard actor must bound each store
//! call to `max_flush_lifetime` itself, not just check the deadline between
//! retryable-error attempts. A call that is merely slow -- never errors,
//! just never returns -- must not be allowed to publish a flush past its
//! abandonment deadline.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// The data-object PUT never errors, but takes far longer than
/// `max_flush_lifetime` to return. The flush must be abandoned once the
/// injected clock crosses the deadline, rather than waiting out the slow
/// call and publishing arbitrarily far past it.
#[tokio::test]
async fn slow_data_object_put_abandons_flush_at_deadline() {
    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        "/l0/",
        Duration::from_secs(10), // far longer than max_flush_lifetime below
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();

    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(10),
        put_retry_max_attempts: 4,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(20),
        max_flush_lifetime: Duration::from_millis(50),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    // Force the flush now (rather than waiting on a size/age trigger) and,
    // concurrently, wait until the slow store's `put` has actually been
    // entered (so the data-object PUT is genuinely in flight, stalled on its
    // artificial delay) before pushing the injected clock past
    // `max_flush_lifetime`. The advance must cross the deadline the flush
    // pinned at open, not just add an arbitrary amount, so this proves the
    // abandonment is driven by the deadline rather than by coincidence.
    tokio::join!(router.flush_all(), async {
        while slow_store.hits() < 1 {
            tokio::task::yield_now().await;
        }
        clock.advance_ns(100_000_000); // 100ms > 50ms max_flush_lifetime
    });

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.abandoned_retry_exhausted, 1,
        "a store call that outlives the deadline must abandon the flush, \
         not publish once it eventually succeeds"
    );
    assert_eq!(snapshot.flushes_by_size, 0);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "an abandoned flush must never publish a data object or commit record, \
         even though the slow put would have succeeded had it been awaited: {objects:?}"
    );

    router.shutdown().await;
}
