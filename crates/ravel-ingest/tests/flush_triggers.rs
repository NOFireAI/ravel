//! Size-triggered and age-triggered adaptive flush (docs/ingest.md "Shard
//! actor").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

#[tokio::test]
async fn size_triggered_flush_lands_without_waiting_on_age() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
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

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("write flushes on size before the deadline");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(objects.iter().any(|o| o.key.contains("/l0/")));
    assert!(objects.iter().any(|o| o.key.contains("/c/")));

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_size, 1);
    assert_eq!(snapshot.flushes_by_age, 0);

    router.shutdown().await;
}

#[tokio::test]
async fn age_triggered_flush_lands_below_size_threshold() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
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

    // The shard actor's "age" check reads the injected `TestClock`, which
    // never advances on its own: nudge it forward past `max_flush_delay`
    // while the write is in flight so the actor's tick notices.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            clock.advance_ns(100_000_000);
        },
    );
    let receipt = write_result.expect("write flushes on age before the deadline");
    assert_eq!(receipt.tokens.len(), 1);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_size, 0);
    assert_eq!(snapshot.flushes_by_age, 1);

    router.shutdown().await;
}
