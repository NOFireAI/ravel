//! Bounded-channel backpressure (docs/ingest.md "Channel": full channel
//! blocks the producer, memory does not grow unbounded) and the
//! one-task-per-shard structural guarantee.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{StallingStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

#[tokio::test]
async fn full_channel_blocks_the_producer_instead_of_growing_memory() {
    // Stall the very first data-object put so the shard actor never drains
    // its channel; then fill the channel to capacity and confirm one more
    // write blocks instead of being buffered unboundedly.
    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), "/l0/", 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let channel_depth = 2;
    let config = IngestConfig {
        shard_count: 1,
        channel_depth,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));

    let tenant = tenant("acme");

    // First write's flush hits the stalled put and never returns, tying up
    // the shard actor so it stops draining its mpsc channel.
    let stuck_router = Arc::clone(&router);
    let stuck_tenant = tenant.clone();
    let _stuck = tokio::spawn(async move {
        let points = vec![make_point(
            &stuck_tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )];
        let _ = stuck_router
            .write(
                stuck_tenant,
                points,
                WriteMode::Buffered,
                Duration::from_secs(30),
            )
            .await;
    });

    // Give the actor a chance to pick up the first write and block inside
    // its flush before we start filling the channel behind it.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Fill the channel to its configured depth with buffered writes. This
    // takes one write more than `channel_depth` under pipelined flushes
    // (ADR-0067 decision 1): the actor pins the stuck write's flush
    // identity, moves its buffer into a spawned task, and returns to
    // `recv()` immediately rather than staying parked inside the PUT
    // itself. It therefore eagerly dequeues the very next write, hits
    // `max_inflight_flushes` (default 1, already held by the stuck task)
    // at that write's own flush trigger, and blocks there -- inside
    // processing, not in the mailbox. That first post-stall write is
    // absorbed for free; the mailbox only starts filling from the one
    // after it, so `channel_depth` more writes enqueue without blocking.
    for i in 0..=channel_depth {
        let points = vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", &format!("b{i}"))],
            2_000,
            1.0,
        )];
        router
            .write(
                tenant.clone(),
                points,
                WriteMode::Buffered,
                Duration::from_secs(30),
            )
            .await
            .expect("enqueue within channel depth must not block");
    }

    // One more write should now block on the full channel rather than
    // returning immediately or growing an unbounded local buffer.
    let extra_tenant = tenant.clone();
    let extra_router = Arc::clone(&router);
    let blocked = tokio::spawn(async move {
        let points = vec![make_point(
            &extra_tenant,
            "cpu_usage",
            &[("host", "overflow")],
            3_000,
            1.0,
        )];
        extra_router
            .write(
                extra_tenant,
                points,
                WriteMode::Buffered,
                Duration::from_secs(30),
            )
            .await
    });
    let observed_block = tokio::time::timeout(Duration::from_millis(100), blocked).await;
    assert!(
        observed_block.is_err(),
        "write beyond channel depth must block on backpressure, not return immediately"
    );

    stalling.release();
    router.flush_all().await;
}

#[tokio::test]
async fn task_count_is_fixed_at_construction_independent_of_point_count() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 3,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());
    assert_eq!(router.shard_count(), 3);

    let tenant = tenant("acme");
    let many_points: Vec<_> = (0..500)
        .map(|i| {
            make_point(
                &tenant,
                "cpu_usage",
                &[("host", &i.to_string())],
                1_000,
                i as f64,
            )
        })
        .collect();
    router
        .write(
            tenant.clone(),
            many_points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("large buffered write enqueues without spawning per-point tasks");

    // Task count is a construction-time property (one `tokio::spawn` per
    // shard in `IngestRouter::new`; see its doc comment) and does not
    // change regardless of how many points a write carries.
    assert_eq!(router.shard_count(), 3);

    router.shutdown().await;
}
