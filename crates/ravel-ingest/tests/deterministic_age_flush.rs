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
        // Pins the middle band: with the default floor this one-point buffer
        // would be held toward max_flush_lifetime instead (issue #1737).
        idle_flush_floor_bytes: 0,
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

/// One injected-clock hour at the default 200 ms `flush_tick`, in ticks.
const TICKS_PER_HOUR: u64 = 18_000;

/// Drives one tenant through one injected-clock hour, one `flush_tick` per
/// step: every `write_every_ticks` ticks it buffers `points_per_write`
/// scalar points on one series, then advances the clock by one tick and
/// yields so the actor's age check and any flush it opens run before the next
/// write. Returns `(flushes_by_age, data objects, commit records)`.
async fn one_hour_at_rate(
    config: IngestConfig,
    points_per_write: usize,
    write_every_ticks: u64,
) -> (u64, usize, usize) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let tick_ns = i64::try_from(config.flush_tick.as_nanos()).expect("tick fits i64");
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());
    let tenant = tenant("acme");
    let mut written = 0u64;
    for tick in 0..TICKS_PER_HOUR {
        if tick % write_every_ticks == 0 {
            let points = (0..points_per_write)
                .map(|i| {
                    let ts =
                        i64::try_from(written).expect("fits") + i64::try_from(i).expect("fits");
                    make_point(&tenant, "cpu_usage", &[("host", "a")], 1_000 + ts, 1.0)
                })
                .collect();
            router
                .write(
                    tenant.clone(),
                    points,
                    WriteMode::Buffered,
                    Duration::from_secs(5),
                )
                .await
                .expect("buffered write is acknowledged at enqueue");
            written += points_per_write as u64;
            while router.metrics().snapshot().buffered_points_total < written {
                tokio::task::yield_now().await;
            }
        }
        clock.advance_ns(tick_ns);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_size, 0,
        "only the age trigger may fire at these rates"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commits = objects.iter().filter(|o| o.key.contains("/c/")).count();
    router.shutdown().await;
    (snapshot.flushes_by_age, data, commits)
}

/// Issue #1737: a near-idle buffer (one scalar point every 10 s, 16 object
/// bytes each, so about 5.8 KB in the hour and never at
/// `idle_flush_floor_bytes`) used to flush on every 40 s idle tick, 90 flushes
/// and 180 objects an hour. Below the floor it is held until one idle window
/// short of `max_flush_lifetime` (3560 s), so the hour writes one flush.
#[tokio::test]
async fn near_idle_buffer_flushes_once_an_hour() {
    let (flushes, data, commits) = one_hour_at_rate(IngestConfig::default(), 1, 50).await;
    assert_eq!(flushes, 1, "one hold-to-lifetime flush in the hour");
    assert_eq!((data, commits), (1, 1));
}

/// Middle band: 25 points a second (400 object bytes a second) crosses
/// `idle_flush_floor_bytes` inside every 40 s idle window without reaching
/// `min_flush_bytes`, so the buffer keeps today's idle clock: 90 flushes and
/// 180 objects an hour, unchanged by the floor.
#[tokio::test]
async fn middle_band_buffer_keeps_the_idle_clock() {
    let (flushes, data, commits) = one_hour_at_rate(IngestConfig::default(), 5, 1).await;
    assert_eq!(flushes, 90, "one flush per 40 s idle window");
    assert_eq!((data, commits), (90, 90));
}

/// Fast band: 32 points per 200 ms tick (2560 object bytes a second) reaches a
/// 4 KiB `min_flush_bytes` inside every 2 s `max_flush_delay` window, so the
/// buffer flushes on the fast clock: 1800 flushes and 3600 objects an hour,
/// unchanged by the floor.
#[tokio::test]
async fn fast_band_buffer_keeps_the_fast_clock() {
    let config = IngestConfig {
        min_flush_bytes: 4 * 1024,
        ..IngestConfig::default()
    };
    let (flushes, data, commits) = one_hour_at_rate(config, 32, 1).await;
    assert_eq!(flushes, 1800, "one flush per 2 s fast window");
    assert_eq!((data, commits), (1800, 1800));
}

/// A strict-mode waiter on a one-point buffer, far below
/// `idle_flush_floor_bytes`, still flushes on the 2 s fast clock: the floor
/// holds only buffers nobody is blocked on, so acknowledged-write latency is
/// unchanged.
#[tokio::test]
async fn strict_waiter_below_the_floor_flushes_on_the_fast_clock() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig::default();
    let tick_ns = i64::try_from(config.flush_tick.as_nanos()).expect("tick fits i64");
    let fast_ns = i64::try_from(config.max_flush_delay.as_nanos()).expect("delay fits i64");
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());
    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

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
            // Exactly `max_flush_delay` in ticks, then stop: the ack must
            // arrive without the clock reaching the 40 s idle threshold.
            for _ in 0..fast_ns / tick_ns {
                clock.advance_ns(tick_ns);
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
            }
        },
    );

    let receipt = write_result.expect("the strict waiter flushes on the fast clock");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(clock.now(), BASE_NS + fast_ns);
    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_age, 1);
    router.shutdown().await;
}
