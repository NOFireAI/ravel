//! Pipelined-flush behavior (ADR-0067 decisions 1-2): a flush's data/commit
//! PUTs run in a spawned task the shard actor no longer waits on, so (a) the
//! actor keeps draining its mailbox while a flush is stuck in flight, and (b)
//! two flushes held concurrently and released out of submission order still
//! ack each caller with its own pinned identity, never swapped with the
//! other's.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

/// Two tenants' flushes held concurrently at the data-object PUT and released
/// in the reverse of submission order still resolve each caller's own
/// `write()` call to its own pinned `CommitToken`, not the other's. Flush
/// identity (and so seq) is pinned synchronously in the actor's own
/// message-processing order, before either spawned flush task's PUT is even
/// issued, so submission order determines `seq` ordering regardless of which
/// PUT the store lets through first.
#[tokio::test]
async fn pipelined_flushes_overlap_and_ack_isolation() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        max_inflight_flushes: 2,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));

    let acme = tenant("acme");
    let globex = tenant("globex");
    let acme_hash = acme.hash().to_hex();
    let globex_hash = globex.hash().to_hex();

    // Held at the data-object PUT ("/l0/") only, not the commit-record PUT
    // ("/c/"), so each flush's second PUT resolves on its own once released.
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);

    let acme_router = Arc::clone(&router);
    let acme_tenant = acme.clone();
    let acme_write = tokio::spawn(async move {
        let points = vec![make_point(
            &acme_tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )];
        acme_router
            .write(
                acme_tenant,
                points,
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
    });

    // Give the actor a beat to process acme's write and pin its flush before
    // globex's write is even sent, so submission order is unambiguous.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let globex_router = Arc::clone(&router);
    let globex_tenant = globex.clone();
    let globex_write = tokio::spawn(async move {
        let points = vec![make_point(
            &globex_tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )];
        globex_router
            .write(
                globex_tenant,
                points,
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
    });

    gate.wait_until_held(2).await;
    assert_eq!(gate.held_count(), 2, "both flushes' data PUTs held at once");
    assert_eq!(
        router.metrics().snapshot().in_flight_flushes_total,
        2,
        "in-flight flush gauge reflects both concurrently spawned flushes"
    );

    let held = gate.held_details();
    let acme_id = held
        .iter()
        .find(|(_, _, key)| key.contains(&acme_hash))
        .map(|(id, _, _)| *id)
        .expect("acme's data PUT is held");
    let globex_id = held
        .iter()
        .find(|(_, _, key)| key.contains(&globex_hash))
        .map(|(id, _, _)| *id)
        .expect("globex's data PUT is held");

    // Release the second-submitted flush's PUT before the first-submitted's:
    // the reverse of submission order.
    assert!(gate.release(globex_id));
    assert!(gate.release(acme_id));
    assert_eq!(gate.held_count(), 0);

    let acme_receipt = acme_write
        .await
        .expect("acme write task")
        .expect("acme write acks despite completing its PUT second");
    let globex_receipt = globex_write
        .await
        .expect("globex write task")
        .expect("globex write acks despite completing its PUT first");

    assert_eq!(acme_receipt.tokens.len(), 1);
    assert_eq!(globex_receipt.tokens.len(), 1);
    assert!(
        acme_receipt.tokens[0].seq < globex_receipt.tokens[0].seq,
        "acme's flush was pinned first (lower seq) even though its PUT \
         completed second: ack isolation must key off pinned identity, not \
         completion order"
    );

    router.flush_all().await;
}

/// The shard actor keeps draining its mailbox and spawning independent
/// flushes while an earlier flush's PUT is still stuck: a second tenant's
/// strict write completes well before the first tenant's held flush is ever
/// released, proving the actor is not parked inside the first flush's PUT
/// the way the pre-pipelining inline-flush actor would have been.
#[tokio::test]
async fn actor_drains_while_flush_inflight() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        max_inflight_flushes: 2,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));

    let stuck = tenant("acme");
    let stuck_hash = stuck.hash().to_hex();
    // `Nth(1)` holds only the data-object PUT (the first PUT this tenant's
    // flush issues); the later commit-record PUT, which also matches the
    // tenant-hash substring, is left to resolve on its own once released.
    let gate = fault_store.hold(Op::Put, Some(stuck_hash), Occurrence::Nth(1));

    let stuck_router = Arc::clone(&router);
    let stuck_tenant = stuck.clone();
    let stuck_write = tokio::spawn(async move {
        let points = vec![make_point(
            &stuck_tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )];
        stuck_router
            .write(
                stuck_tenant,
                points,
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
    });

    gate.wait_until_held(1).await;
    assert_eq!(gate.held_count(), 1);

    // A second tenant's own flush needs no gate release at all: the actor
    // must have already dequeued and processed this write, and spawned (and
    // completed) an independent flush task under its own semaphore permit,
    // while the first tenant's flush is still parked on the gate.
    let other = tenant("globex");
    let other_points = vec![make_point(
        &other,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    let other_receipt = router
        .write(
            other,
            other_points,
            WriteMode::Strict,
            Duration::from_millis(500),
        )
        .await
        .expect("a second tenant's independent flush completes without waiting on the stuck one");
    assert_eq!(other_receipt.tokens.len(), 1);

    // The first tenant's flush is still exactly where it was: draining
    // unrelated messages did not disturb it.
    assert_eq!(
        gate.held_count(),
        1,
        "the stuck flush is untouched by the second tenant's write"
    );

    let ids = gate.held();
    assert!(gate.release(ids[0]));
    stuck_write
        .await
        .expect("stuck write task")
        .expect("stuck write eventually acks once its PUT is released");

    router.flush_all().await;
}
