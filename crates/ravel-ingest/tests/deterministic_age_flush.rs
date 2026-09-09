//! Regression test: the shard actor's
//! flush tick must run on the injected `Clock`, so advancing that clock past
//! `max_flush_delay` drives an age-based flush deterministically, with no
//! wall-clock sleep and no flakiness.
//!
//! With the tick on the one injected clock, the two-clock interleaving
//! (the injected `Clock` for the age check, the tokio timer for the tick)
//! cannot occur: buffer the point first, then advance the clock, and the age
//! flush lands every time. The ordering is established with a cooperative poll on
//! the buffered-points counter rather than a real `tokio::time::sleep`, so
//! the test cannot race.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Buffers a point below the size threshold, then advances the injected clock
/// past `max_flush_delay`. The flush tick, now on that same clock, wakes and
/// fires the age flush; the strict write's ack proves it committed.
#[tokio::test]
async fn advancing_the_injected_clock_drives_an_age_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Only the age trigger can fire: the buffer never reaches target_bytes.
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

    // A strict write blocks until its flush acks, so drive the age flush from
    // the joined arm. Wait (cooperatively, no real sleep) until the point is
    // buffered in the actor so `note_arrival` has stamped `oldest_arrival_ns`
    // with the pre-advance time, then push the clock past `max_flush_delay`.
    // Because the tick shares this clock, the advance deterministically wakes
    // it and the age check sees the buffer as due.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        async {
            while router.metrics().snapshot().buffered_points_total < 1 {
                tokio::task::yield_now().await;
            }
            clock.advance_ns(100_000_000);
        },
    );

    let receipt =
        write_result.expect("age flush fires once the injected clock passes max_flush_delay");
    assert_eq!(receipt.tokens.len(), 1);

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 1,
        "the advance must drive exactly one age-triggered flush"
    );
    assert_eq!(
        snapshot.flushes_by_size, 0,
        "the buffer never reached target_bytes, so no size flush may fire"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|o| o.key.contains("/l0/")),
        "the age flush must have stored a data object"
    );
    assert!(
        objects.iter().any(|o| o.key.contains("/c/")),
        "the age flush must have stored a commit record"
    );

    router.shutdown().await;
}

/// A buffer whose age has not reached `max_flush_delay` must not be flushed by
/// a tick, no matter how many ticks fire. This pins the other side of the age
/// threshold so the flush above is attributable to the elapsed age, not to the
/// tick firing at all. The write is buffered (not strict) so nothing blocks on
/// an ack that will never come.
#[tokio::test]
async fn a_tick_below_max_flush_delay_does_not_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
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
    router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    while router.metrics().snapshot().buffered_points_total < 1 {
        tokio::task::yield_now().await;
    }

    // Advance by less than `max_flush_delay` and fire several ticks. The age
    // check must find the buffer not yet due, so no flush happens.
    for _ in 0..5 {
        clock.advance_ns(5_000_000); // 5 ms, total 25 ms < 50 ms
        tokio::task::yield_now().await;
    }

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 0,
        "a buffer younger than max_flush_delay must not age-flush"
    );

    router.shutdown().await;
}

/// ADR-0051 section 7: a buffered-mode buffer with no strict-mode
/// waiter and fewer than `min_flush_bytes` is idle, so the fast
/// `max_flush_delay` age trigger must not fire for it -- only the slower
/// `max_flush_delay_idle` does. This pins the idle side of the predicate;
/// `advancing_the_injected_clock_drives_an_age_flush` above already pins the
/// non-idle (strict-waiter) side.
#[tokio::test]
async fn idle_buffer_defers_age_trigger() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Only the age trigger can fire: the buffer never reaches target_bytes.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(300),
        // Comfortably above one point's estimated buffered size, so the
        // buffer stays "idle" for the whole test.
        min_flush_bytes: 1_000_000,
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

    // Buffered mode: no strict waiter, so `waiters` stays empty and the
    // buffer's idleness is decided purely by `min_flush_bytes`.
    router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    while router.metrics().snapshot().buffered_points_total < 1 {
        tokio::task::yield_now().await;
    }

    // Past max_flush_delay (50ms) but below max_flush_delay_idle (300ms): an
    // idle buffer must not flush yet.
    for _ in 0..10 {
        clock.advance_ns(10_000_000); // 10ms, total 100ms
        tokio::task::yield_now().await;
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 0,
        "an idle buffer must defer past max_flush_delay to max_flush_delay_idle"
    );

    // Past max_flush_delay_idle (300ms total): the deferred flush must fire.
    for _ in 0..25 {
        clock.advance_ns(10_000_000); // 10ms, total 350ms
        tokio::task::yield_now().await;
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 1,
        "the deferred age flush must fire once max_flush_delay_idle elapses"
    );

    router.shutdown().await;
}
