//! Per-shard ingest-skew metrics (issue #865). Drives loads through a live
//! `IngestRouter` and asserts the per-shard message counts and the
//! on-actor/off-actor time split, so a future argument about shard-actor
//! throughput can rest on these figures instead of assertion.
#![allow(clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, ShardSkewStats, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{METRIC_NAME_LABEL, SeriesId, Signal, TenantId, shard_for};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// A metric name whose series id routes to `target` under `shard_count`.
/// Routing is `shard_for` over the series id's leading bytes, and the id is a
/// hash of the labels, so the shard cannot be chosen by construction; this
/// searches metric names until one lands on the wanted shard. Deterministic:
/// the same tenant, name, and label set always hash to the same id.
fn metric_for_shard(tenant: &TenantId, target: u32, shard_count: u32) -> String {
    for i in 0..100_000u32 {
        let name = format!("m{i}");
        let labels = common::build_labels(&[(METRIC_NAME_LABEL, name.as_str())]);
        let series_id = SeriesId::compute(tenant, &name, &labels).expect("series id");
        if shard_for(&series_id, shard_count) == target {
            return name;
        }
    }
    panic!("no metric name routed to shard {target} of {shard_count}");
}

fn skew_map(router: &IngestRouter) -> HashMap<u32, ShardSkewStats> {
    router.metrics().shard_skew_by_shard().into_iter().collect()
}

fn base_config(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        // Large so no size or age trigger fires on its own: every flush in
        // these tests is the explicit `flush_all` drain, keeping message and
        // time accounting driven by the test rather than by a background tick.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        ..IngestConfig::default()
    }
}

/// A deliberately skewed load -- eight messages to one shard, one each to two
/// others -- must show the skew in the per-shard processed counts, pinned to
/// the exact numbers the fixed input dictates (not merely "hot > cold").
#[tokio::test]
async fn skewed_load_shows_exact_per_shard_processed_counts() {
    let shard_count = 4;
    let clock = TestClock::new(BASE_NS);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = IngestRouter::new(
        base_config(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let hot = metric_for_shard(&tenant, 0, shard_count);
    let warm1 = metric_for_shard(&tenant, 1, shard_count);
    let warm2 = metric_for_shard(&tenant, 2, shard_count);

    // Each `write` sends exactly one message to the one shard its point routes
    // to. Eight to shard 0, one to shard 1, one to shard 2; shard 3 gets none.
    for _ in 0..8 {
        write_one(&router, &tenant, &hot).await;
    }
    write_one(&router, &tenant, &warm1).await;
    write_one(&router, &tenant, &warm2).await;

    // Drain: after `flush_all` returns, every earlier Write message has been
    // processed (the actor pulls its channel in FIFO order, so the FlushNow it
    // acknowledges sits behind them all).
    router.flush_all().await;

    let skew = skew_map(&router);
    assert_eq!(skew[&0].messages_processed, 8, "hot shard processed count");
    assert_eq!(skew[&1].messages_processed, 1);
    assert_eq!(skew[&2].messages_processed, 1);
    assert_eq!(
        skew.get(&3).map(|s| s.messages_processed),
        None,
        "a shard that received no message has no entry, not a zero row"
    );
    // Enqueued matches processed once drained, and the depth is empty.
    for shard in [0u32, 1, 2] {
        assert_eq!(
            skew[&shard].messages_enqueued,
            skew[&shard].messages_processed
        );
        assert_eq!(skew[&shard].queue_depth, 0);
    }
    // The skew is in the asserted direction, not just present.
    assert!(skew[&0].messages_processed > skew[&1].messages_processed);
    assert!(skew[&0].messages_processed > skew[&2].messages_processed);

    router.shutdown().await;
}

/// An even load must produce even per-shard counts: one message each. This
/// pins that the skew signal above is a property of the load, not an artifact
/// of the counter (e.g. every message miscounted onto shard 0).
#[tokio::test]
async fn even_load_shows_even_per_shard_processed_counts() {
    let shard_count = 4;
    let clock = TestClock::new(BASE_NS);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = IngestRouter::new(
        base_config(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let metrics: Vec<String> = (0..shard_count)
        .map(|s| metric_for_shard(&tenant, s, shard_count))
        .collect();
    for m in &metrics {
        write_one(&router, &tenant, m).await;
    }
    router.flush_all().await;

    let skew = skew_map(&router);
    assert_eq!(
        skew.len() as u32,
        shard_count,
        "every shard saw one message"
    );
    for shard in 0..shard_count {
        assert_eq!(
            skew[&shard].messages_processed, 1,
            "shard {shard} under an even load"
        );
        assert_eq!(skew[&shard].messages_enqueued, 1);
        assert_eq!(skew[&shard].queue_depth, 0);
    }

    router.shutdown().await;
}

/// The on-actor/off-actor split is the point of the whole measurement: a slow
/// flush must land its time in `off_actor_ns` (the spawned flush task) and
/// leave `on_actor_ns` (the actor's serial merge/pin section) untouched. The
/// slowness is injected through `SlowStore` on the injected clock, so the
/// figure is exact and deterministic, not a wall-clock band.
#[tokio::test]
async fn slow_flush_time_lands_off_actor_not_on_actor() {
    const SLOW: Duration = Duration::from_secs(5);
    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        "/l0/", // the data-object PUT; the commit-record PUT ("/c/") stays fast
        SLOW,
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();

    let config = IngestConfig {
        // Comfortably longer than SLOW so the flush is not abandoned: this test
        // is about a slow-but-successful flush, not the abandonment path.
        max_flush_lifetime: Duration::from_secs(60),
        ..base_config(1)
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    // Buffered write: acknowledged at enqueue, handled by the actor at the
    // current clock with no store call, so the on-actor section advances the
    // injected clock by nothing.
    router
        .write(
            tenant.clone(),
            vec![make_point(&tenant, "cpu", &[("h", "a")], 1_000, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write acknowledged at enqueue");

    // Drive the flush and, once its data PUT is genuinely in flight (stalled on
    // the injected clock), advance the clock by exactly SLOW so the PUT
    // completes. The flush task brackets `run_flush` on the same clock, so its
    // recorded off-actor time is exactly SLOW.
    tokio::join!(router.flush_all(), async {
        while slow_store.hits() < 1 {
            tokio::task::yield_now().await;
        }
        clock.advance_ns(SLOW.as_nanos() as i64);
    });

    let skew = skew_map(&router);
    let shard0 = skew[&0];
    assert_eq!(
        shard0.on_actor_ns, 0,
        "the actor's serial section did no clock-advancing work; the slow flush \
         runs off the actor and must not be charged here"
    );
    assert_eq!(
        shard0.off_actor_ns,
        SLOW.as_nanos() as u64,
        "the slow flush's whole duration must be charged to off-actor time"
    );
    assert!(
        shard0.off_actor_ns > shard0.on_actor_ns,
        "off-actor time must carry the flush cost, on-actor must not"
    );
    assert_eq!(shard0.messages_processed, 1);

    router.shutdown().await;
}

async fn write_one(router: &IngestRouter, tenant: &TenantId, metric: &str) {
    router
        .write(
            tenant.clone(),
            vec![make_point(tenant, metric, &[], 1_000, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write acknowledged at enqueue");
}
