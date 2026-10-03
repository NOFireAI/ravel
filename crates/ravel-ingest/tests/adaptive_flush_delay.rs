//! End-to-end reachability for the adaptive age trigger (ADR-0067 decision
//! 3): `IngestConfig::adaptive_flush_delay` must actually reach a live shard
//! actor's flush decision, not just the pure corridor math it is built from.
//! That math (`visibility_ceiling_ns`, `adaptive_age_threshold_ns`) is
//! unit-tested directly, as `adaptive_delay_respects_visibility_ceiling` in
//! `crates/ravel-ingest/src/shard.rs`'s `adaptive_delay_tests` module, since
//! it is deterministic pure-function behavior that does not need a live
//! actor. This file is the companion that proves the config flag is wired
//! to that math at all.
//!
//! Building a tenant's observed inter-arrival gap (`avg_gap_ns`) above the
//! fixed floor without ever tripping a premature age-flush is the crux of
//! the design here: the age check is anchored to a buffer's *first* arrival,
//! so naively spacing two strict-mode writes further apart than the floor
//! lets the periodic tick flush the first write alone (on the then-current,
//! still-zero `avg_gap_ns`) before the second write ever lands. Both tests
//! below avoid that by building the gap out of buffered-mode writes with no
//! strict waiter and few bytes: `ShardActor::age_threshold_ns` treats such a
//! buffer as idle regardless of `avg_gap_ns` and uses `max_flush_delay_idle`
//! (set enormous here) instead, so the periodic tick can fire any number of
//! times while the gap accumulates without ever flushing early. Only the
//! final, strict-mode write flips the buffer out of the idle path -- by
//! which point `avg_gap_ns` already reflects the full observed gap.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

/// A short real-wall-clock yield: nothing here waits on the *injected*
/// clock, so this only gives the shard actor's task a chance to run and
/// drain whatever is already pending (a merge, a tick wakeup) before the
/// test's own task resumes and reads state or advances the injected clock
/// further.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

fn base_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        // Large enough that no write in this test ever trips the size
        // trigger; only the age trigger is under test.
        target_bytes: 1_000_000_000,
        max_flush_delay: Duration::from_millis(500),
        flush_tick: Duration::from_millis(20),
        // Enormous on purpose: see the module doc. A buffered-mode write
        // with no strict waiter must never be treated as worth an
        // age-triggered flush while the test is building up `avg_gap_ns`.
        max_flush_delay_idle: Duration::from_secs(10_000),
        min_flush_bytes: 1_000_000,
        put_retry_max_attempts: 1,
        put_retry_base_delay: Duration::from_millis(50),
        put_retry_max_delay: Duration::from_millis(200),
        ..IngestConfig::default()
    }
}

/// With `adaptive_flush_delay: true` and a prior PUT RTT sample on record,
/// a tenant whose observed arrival gap (900ms) exceeds the fixed floor
/// (500ms) is flushed on `FlushTrigger::AgeAdaptive`, at a threshold capped
/// by the visibility-budget ceiling rather than the raw gap -- proving the
/// config flag actually reaches `ShardActor::age_threshold_ns` in a live
/// actor.
#[tokio::test]
async fn adaptive_flush_delay_true_uses_the_corridor_above_the_floor() {
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let slow = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        "/l0/",
        Duration::from_millis(200),
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow.clone();
    let config = IngestConfig {
        adaptive_flush_delay: true,
        ..base_config()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));
    let acme = tenant("acme");

    // Round 1: seed an RTT sample so the corridor's ceiling sits above the
    // floor, via a single ordinary strict write flushed on the fixed floor
    // (no gap has been observed yet, so the threshold is the floor
    // regardless of the adaptive flag).
    let seed_points = vec![make_point(
        &acme,
        "cpu_usage",
        &[("host", "seed")],
        1_000,
        1.0,
    )];
    let seed_write = {
        let router = Arc::clone(&router);
        let acme = acme.clone();
        tokio::spawn(async move {
            router
                .write(
                    acme,
                    seed_points,
                    WriteMode::Strict,
                    Duration::from_secs(30),
                )
                .await
        })
    };
    settle().await;
    clock.advance_ns(500_000_000); // crosses the floor; tick flushes on Age
    settle().await;
    clock.advance_ns(200_000_000); // the seed flush's SlowStore-delayed data PUT resolves
    settle().await;
    seed_write
        .await
        .expect("seed write task")
        .expect("seed write acks");

    let after_seed = router.metrics().snapshot();
    assert_eq!(after_seed.flushes_by_age, 1);
    assert_eq!(after_seed.flushes_by_age_adaptive, 0);

    // Round 2: build a fresh buffer's avg_gap_ns to 900ms via two idle
    // (buffered, sub-min_flush_bytes) arrivals, safe from the tick
    // regardless of interleaving (see module doc), then flip to priority
    // with a final strict write.
    let b = vec![make_point(&acme, "cpu_usage", &[("host", "b")], 2_000, 1.0)];
    router
        .write(acme.clone(), b, WriteMode::Buffered, Duration::from_secs(5))
        .await
        .expect("idle buffered write B enqueues");
    settle().await;

    clock.advance_ns(900_000_000);

    let c = vec![make_point(&acme, "cpu_usage", &[("host", "c")], 3_000, 1.0)];
    router
        .write(acme.clone(), c, WriteMode::Buffered, Duration::from_secs(5))
        .await
        .expect("idle buffered write C enqueues");
    settle().await; // safe even if a stale tick fires here: B alone is still idle-priced

    let d = vec![make_point(&acme, "cpu_usage", &[("host", "d")], 3_000, 1.0)];
    let flip_write = {
        let router = Arc::clone(&router);
        let acme = acme.clone();
        tokio::spawn(async move {
            router
                .write(acme, d, WriteMode::Strict, Duration::from_secs(30))
                .await
        })
    };
    // The clock has not moved since C merged, and the periodic tick was
    // last rescheduled at (clock at C's merge) + flush_tick, still in the
    // future: D is guaranteed to merge (flipping the buffer to priority,
    // avg_gap_ns landing at 675ms) before any tick can observe it.
    settle().await;

    // Now cross the next tick with the buffer already flipped to priority:
    // this is the only advance in the whole test that a real corridor
    // evaluation depends on.
    clock.advance_ns(30_000_000);
    settle().await;
    clock.advance_ns(200_000_000); // the flip flush's SlowStore-delayed data PUT resolves
    settle().await;

    let receipt = flip_write
        .await
        .expect("flip write task")
        .expect("flip write acks");
    assert_eq!(receipt.tokens.len(), 1);

    let after_flip = router.metrics().snapshot();
    assert_eq!(
        after_flip.flushes_by_age, 1,
        "only the round-1 seed flush used the plain Age trigger"
    );
    assert_eq!(
        after_flip.flushes_by_age_adaptive, 1,
        "the round-2 flush must have used the corridor-capped AgeAdaptive trigger, \
         proving adaptive_flush_delay: true reached the live actor's flush decision"
    );

    router.flush_all().await;
}

/// The same buffered-then-priority arrival pattern as
/// `adaptive_flush_delay_true_uses_the_corridor_above_the_floor`, but with
/// `adaptive_flush_delay: false`: the flush must still fire on the plain
/// fixed floor (`FlushTrigger::Age`), never `AgeAdaptive`, even though the
/// same 900ms observed gap would have pushed the threshold above the floor
/// had the flag been on. Same inputs, opposite config, opposite observed
/// trigger -- the contrast is the reachability proof.
#[tokio::test]
async fn adaptive_flush_delay_false_keeps_the_fixed_floor_under_the_same_pattern() {
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let config = IngestConfig {
        adaptive_flush_delay: false,
        ..base_config()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock.clone(),
    ));
    let acme = tenant("acme");

    let b = vec![make_point(&acme, "cpu_usage", &[("host", "b")], 1_000, 1.0)];
    router
        .write(acme.clone(), b, WriteMode::Buffered, Duration::from_secs(5))
        .await
        .expect("idle buffered write B enqueues");
    settle().await;

    clock.advance_ns(900_000_000);

    let c = vec![make_point(&acme, "cpu_usage", &[("host", "c")], 2_000, 1.0)];
    router
        .write(acme.clone(), c, WriteMode::Buffered, Duration::from_secs(5))
        .await
        .expect("idle buffered write C enqueues");
    settle().await;

    let d = vec![make_point(&acme, "cpu_usage", &[("host", "d")], 3_000, 1.0)];
    let flip_write = {
        let router = Arc::clone(&router);
        let acme = acme.clone();
        tokio::spawn(async move {
            router
                .write(acme, d, WriteMode::Strict, Duration::from_secs(30))
                .await
        })
    };
    settle().await;

    clock.advance_ns(30_000_000);
    settle().await;

    let receipt = flip_write
        .await
        .expect("flip write task")
        .expect("flip write acks");
    assert_eq!(receipt.tokens.len(), 1);

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 1,
        "with adaptive_flush_delay off, the flush must use the plain fixed floor"
    );
    assert_eq!(
        snapshot.flushes_by_age_adaptive, 0,
        "the corridor must never engage when the flag is off, regardless of avg_gap_ns"
    );

    router.flush_all().await;
}
