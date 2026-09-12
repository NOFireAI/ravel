//! Tenant isolation under a stalled flush (issue #1292).
//!
//! Object keys are tenant-prefixed and S3 throttles per key prefix, so a
//! `503 SlowDown` confined to one busy tenant's prefix used to stall every
//! co-resident tenant whose series hash to the same shard: the shard actor
//! acquired the `max_inflight_flushes` permit ON the actor, so at the bound it
//! parked the whole `select!` loop -- no channel drain, no age-flush tick, no
//! flush reaping -- for the wedged flush's whole duration. The permit acquire
//! now runs inside the spawned flush task, so the actor keeps draining and
//! ticking while a prior flush is stalled. This test pins that: a flush stalled
//! on tenant A's prefix must not stop tenant B's age trigger on the same shard.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{StallingStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

fn in_flight(router: &IngestRouter, shard: u32) -> u64 {
    router
        .metrics()
        .in_flight_flushes_by_shard()
        .into_iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, n)| n)
        .unwrap_or(0)
}

fn processed(router: &IngestRouter, shard: u32) -> u64 {
    router
        .metrics()
        .shard_skew_by_shard()
        .into_iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, s)| s.messages_processed)
        .unwrap_or(0)
}

/// Poll `f` until it returns `target`, up to ~2 s of real time in 5 ms steps.
/// This only observes an event-driven metric transition; the age trigger under
/// test is driven by the injected clock's `advance_ns`, never by these sleeps.
async fn poll_until(mut f: impl FnMut() -> u64, target: u64) -> u64 {
    let mut last = f();
    for _ in 0..400 {
        last = f();
        if last >= target {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    last
}

#[tokio::test]
async fn stalled_flush_does_not_stop_a_coresident_tenants_age_trigger() {
    let start_ns = 1_700_000_000_000_000_000;
    let clock = TestClock::new(start_ns);

    let tenant_a = tenant("throttled-prefix-tenant");
    let tenant_b = tenant("healthy-tenant");
    // A's data object key is `t/<a_hash_hex>/metrics/l0/...`; stalling on this
    // substring stalls A's PUT and nothing of B's.
    let a_hash_hex = tenant_a.hash().to_hex();

    // Stall the first matching PUT (A's data object) until released. The hit
    // counter lets the test assert the fault fired exactly once.
    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), a_hash_hex, 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();

    let max_flush_delay = Duration::from_secs(2);
    let config = IngestConfig {
        shard_count: 1,
        // Small enough that A's wide batch size-triggers at once, large enough
        // that B's single point stays buffered until its age trigger fires.
        target_bytes: 4096,
        max_flush_delay,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));

    // Tenant A: a wide strict write that crosses target_bytes, so it opens a
    // size flush. The spawned flush task's data PUT to A's prefix stalls,
    // holding the shard's one flush permit.
    let a_points: Vec<_> = (0..400u32)
        .map(|i| {
            make_point(
                &tenant_a,
                "m",
                &[("series", &i.to_string())],
                start_ns,
                i as f64,
            )
        })
        .collect();
    let router_a = Arc::clone(&router);
    let tenant_a_moved = tenant_a.clone();
    let _a = tokio::spawn(async move {
        let _ = router_a
            .write(
                tenant_a_moved,
                a_points,
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });
    stalling.wait_until_stalled().await;
    assert_eq!(
        in_flight(&router, 0),
        1,
        "A's flush task is spawned and holds the one permit"
    );

    // Tenant B: a single strict point, below target_bytes, so it buffers with a
    // waiter (priority => fast age clock = max_flush_delay). Its ack cannot
    // resolve while A holds the permit, so spawn it and do not await.
    let b_point = vec![make_point(&tenant_b, "m", &[("h", "1")], start_ns, 1.0)];
    let router_b = Arc::clone(&router);
    let _b = tokio::spawn(async move {
        let _ = router_b
            .write(
                tenant_b,
                b_point,
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });

    // The actor keeps draining while A is stalled: it processes B's write
    // (A's write + B's write = 2 processed). Before the fix this still held,
    // because the actor only parks at the NEXT flush that needs a permit.
    let seen = poll_until(|| processed(&router, 0), 2).await;
    assert_eq!(
        seen, 2,
        "the actor must keep draining and buffer B while A's flush is stalled"
    );
    assert_eq!(
        in_flight(&router, 0),
        1,
        "B is buffered, not yet flushed: still just A's flush in flight"
    );

    // Advance the injected clock past max_flush_delay: B's age trigger is due.
    clock.advance_ns(max_flush_delay.as_nanos() as i64 + 1);

    // The age tick fires and spawns B's flush task even though A still holds the
    // only permit, so in-flight rises to exactly 2. Before the fix the actor
    // parked on B's on-actor acquire and this stayed at 1 forever (the age
    // trigger, and every later tick, was stopped).
    let observed = poll_until(|| in_flight(&router, 0), 2).await;
    assert_eq!(
        observed, 2,
        "B's age trigger must spawn its flush task while A is stalled"
    );

    // The stall fired exactly once, on A's data PUT: the isolation held against
    // a real fault, not an unexercised one.
    assert_eq!(
        stalling.stall_hits(),
        1,
        "exactly one PUT stalled, and it was on tenant A's prefix"
    );

    // Let A's PUT proceed and drain both tenants so the actor shuts down clean.
    stalling.release();
    router.flush_all().await;
}
