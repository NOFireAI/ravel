//! Ingest backpressure and the one-task-per-shard structural guarantee.
//!
//! The backpressure semantics changed with issue #1292. The
//! `max_inflight_flushes` permit is now acquired inside the spawned flush task,
//! not on the actor, so a stalled flush no longer parks the actor and no longer
//! fills the mpsc channel to block the producer: the actor keeps draining. The
//! memory bound therefore moved from the bounded channel to the ADR-0069 global
//! byte budget -- each in-flight flush holds its byte charge until it completes,
//! so a shard wedged on a stalled prefix drains the budget and `try_charge`
//! sheds at the ceiling. This file pins the new bound (shed, don't block) rather
//! than the old one (block on a full channel), which the pre-#1292 revision of
//! `full_channel_blocks_the_producer_instead_of_growing_memory` asserted.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{StallingStore, TestClock, make_point, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, WriteError, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

#[tokio::test]
async fn stalled_flush_sheds_via_byte_budget_without_blocking_the_producer() {
    // Stall the very first data-object put so that flush -- and, at
    // `max_inflight_flushes == 1`, every flush that follows -- is wedged,
    // holding its ADR-0069 byte charge for the whole test. With the acquire off
    // the actor (#1292) the actor keeps draining, so the producer is never
    // blocked; the only thing that bounds memory is the byte budget, which must
    // shed once the wedged charges reach the ceiling.
    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), "/l0/", 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        // Tiny target so every single-point write opens its own flush, which
        // then waits on the one permit the stalled flush holds, pinning that
        // write's charge in flight.
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    // A small bounded budget so a handful of pinned charges reach the ceiling.
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(8 * 1024));
    let router = Arc::new(
        IngestRouter::new(config, store, Signal::Metrics, clock.clone()).with_budget(budget),
    );

    let tenant = tenant("acme");

    // First write's flush hits the stalled put and never returns, holding the
    // permit and its charge for the rest of the test.
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
    stalling.wait_until_stalled().await;

    // Keep writing. Each write returns promptly -- the actor is not parked, so
    // the producer never blocks -- and each opens a flush that waits on the one
    // permit, pinning its charge. Within a bounded number of writes the budget
    // must shed with `BufferBudgetExceeded` rather than blocking or growing
    // memory without bound.
    let mut shed = false;
    for i in 0..1_000 {
        let points = vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", &format!("b{i}"))],
            2_000,
            1.0,
        )];
        // Bound each write in real time as well: a regression that reparks the
        // actor would block here, and this turns that hang into a failure.
        let write = router.write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(30),
        );
        match tokio::time::timeout(Duration::from_secs(5), write).await {
            Ok(Ok(_)) => {}
            Ok(Err(WriteError::BufferBudgetExceeded)) => {
                shed = true;
                break;
            }
            Ok(Err(e)) => panic!("unexpected write error: {e:?}"),
            Err(_) => panic!(
                "write blocked instead of returning: the actor must keep draining while a \
                 flush is stalled (issue #1292)"
            ),
        }
    }
    assert!(
        shed,
        "the byte budget must shed once the stalled in-flight flushes pin charges to the ceiling"
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
