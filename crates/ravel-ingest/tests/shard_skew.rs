//! Per-shard ingest-skew metrics (issue #865). Drives loads through a live
//! `IngestRouter` and asserts the per-shard message counts and the three-way
//! on-actor / flush-permit-wait / off-actor time split, so a future argument
//! about shard-actor throughput can rest on these figures instead of assertion.
//!
//! The three spans and where each starts and stops:
//!
//! 1. `on_actor_ns`: actor pulls a `Write` off its channel -> `handle_write`
//!    returns, minus any permit wait nested inside.
//! 2. `flush_permit_wait_ns`: `flush_tenant` reaches the `max_inflight_flushes`
//!    acquire -> that acquire grants.
//! 3. `off_actor_ns`: the spawned flush task enters `run_flush` (permit held)
//!    -> `run_flush` returns.
//!
//! 1 and 2 both accrue on the actor task, so they are disjoint in wall time
//! too. 3 accrues in tasks that run concurrently with the actor, which is what
//! ADR-0067's pipelining is for.
#![allow(clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestMetrics, IngestRouter, ShardSkewStats, WriteMode};
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

/// One shard's figures read through a metrics handle rather than the router,
/// for tests that must call the router-consuming `shutdown` (which joins every
/// flush task) before the final read.
fn skew_of(metrics: &IngestMetrics, shard: u32) -> ShardSkewStats {
    metrics
        .shard_skew_by_shard()
        .into_iter()
        .collect::<HashMap<u32, ShardSkewStats>>()[&shard]
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
    assert_eq!(
        shard0.flush_permit_wait_ns, 0,
        "one flush against a free permit never parks on the semaphore"
    );
    assert_eq!(shard0.messages_processed, 1);

    router.shutdown().await;
}

/// Config for the two permit-wait tests below: one shard, one flush permit, and
/// a `target_bytes` of 1 so every write is a size trigger. The age and tick
/// thresholds stay at `base_config`'s hour, so the only thing that opens a
/// flush is a write, and the only thing that advances the clock is the test.
fn contended_flush_config() -> IngestConfig {
    IngestConfig {
        // Any single point clears this, so every write is a size trigger.
        target_bytes: 1,
        // One permit: the second concurrent flush trigger must park.
        max_inflight_flushes: 1,
        // Comfortably longer than the whole scripted timeline, so no flush is
        // abandoned: these tests are about slow-but-successful flushes.
        max_flush_lifetime: Duration::from_secs(600),
        ..base_config(1)
    }
}

/// Spins until the shard actor has merged `n` points in total. `buffered_*` is
/// incremented in `handle_write` before the flush trigger, and the only
/// suspension point between that increment and the flush-permit acquire is the
/// acquire itself, so on the current-thread runtime `#[tokio::test]` provides,
/// observing this count from the test task means the actor has already reached
/// (and parked on) that acquire.
async fn await_points_merged(router: &IngestRouter, n: u64) {
    while router.metrics().snapshot().buffered_points_total < n {
        tokio::task::yield_now().await;
    }
}

/// Spins until `store` has entered its artificial delay `n` times, i.e. `n`
/// flushes have their data PUT genuinely in flight and stalled on the injected
/// clock.
async fn await_slow_puts(store: &SlowStore, n: u64) {
    while store.hits() < n {
        tokio::task::yield_now().await;
    }
}

/// The defect this test exists for: at `max_inflight_flushes`, a size-triggered
/// write's `handle_write` parks on the flush semaphore, and what elapses there
/// is a PRIOR flush's remaining duration -- time `flush_tenant` already charges
/// to `off_actor_ns`. Charging it to `on_actor_ns` as well both double-counts
/// the interval and makes the actor read as busy at exactly the moment flushing
/// is the bottleneck, which would manufacture evidence for the shard-actor
/// throughput claim #865 was filed to test.
///
/// Fixed input, exact figures. The first flush's data PUT costs SLOW; the clock
/// is advanced by PRE_WAIT while it is in flight and before the second write, so
/// the actor parks for exactly the remaining SLOW - PRE_WAIT. That remainder
/// must land in `flush_permit_wait_ns` and nowhere else -- in particular it must
/// be strictly less than the flush it waited on, so the counter cannot be a copy
/// of `off_actor_ns`.
#[tokio::test]
async fn permit_wait_lands_in_its_own_counter_not_in_on_actor() {
    const SLOW: Duration = Duration::from_secs(5);
    const PRE_WAIT: Duration = Duration::from_secs(2);
    const REMAINING_NS: u64 = 3_000_000_000; // SLOW - PRE_WAIT

    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        "/l0/", // the data-object PUT; the commit-record PUT ("/c/") stays fast
        SLOW,
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
    let router = IngestRouter::new(
        contended_flush_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    // Write 1 opens flush A, which takes the only permit and stalls in its data
    // PUT until the test advances the clock.
    write_one(&router, &tenant, "cpu").await;
    await_slow_puts(&slow_store, 1).await;

    // Burn PRE_WAIT of flush A's SLOW before the second write, so the wait the
    // actor is about to take is a strict subset of flush A's own span.
    clock.advance_ns(PRE_WAIT.as_nanos() as i64);

    // Write 2: the actor merges it (no clock-advancing work), opens a size
    // flush, and parks on the semaphore at the bound.
    write_one(&router, &tenant, "cpu").await;
    await_points_merged(&router, 2).await;

    // Retire flush A's remaining SLOW - PRE_WAIT. Its permit is released, and
    // the actor's park ends at that same instant.
    clock.advance_ns(REMAINING_NS as i64);

    // Flush B now holds the permit and stalls the same way; retire it too so
    // both flushes reach a terminal outcome and nothing is abandoned.
    await_slow_puts(&slow_store, 2).await;
    clock.advance_ns(SLOW.as_nanos() as i64);

    // `shutdown` consumes the router and joins every flush task, so take the
    // metrics handle first and read the final figures through it.
    let metrics = router.metrics_handle();
    router.shutdown().await;

    let shard0 = skew_of(&metrics, 0);
    assert_eq!(shard0.messages_processed, 2);
    assert_eq!(
        shard0.flush_permit_wait_ns, REMAINING_NS,
        "the second write parked for flush A's remaining {REMAINING_NS}ns; that is \
         backpressure and belongs in the permit-wait span"
    );
    assert_eq!(
        shard0.on_actor_ns, 0,
        "the actor's merge-and-pin work advanced the injected clock by nothing; \
         charging it the permit wait would report an actor bottleneck at exactly \
         the moment flushing is the bottleneck"
    );
    assert_eq!(
        shard0.off_actor_ns,
        2 * SLOW.as_nanos() as u64,
        "two flushes at SLOW each, measured inside the spawned tasks"
    );
    assert!(
        shard0.flush_permit_wait_ns < SLOW.as_nanos() as u64,
        "the wait is the part of flush A the actor was still parked through \
         ({REMAINING_NS}ns of {}ns), not a second copy of that flush's whole span",
        SLOW.as_nanos()
    );

    // Wall window on the injected clock: PRE_WAIT + REMAINING + SLOW.
    let elapsed_ns = (clock.now() - BASE_NS) as u64;
    assert_eq!(elapsed_ns, 2 * SLOW.as_nanos() as u64);
    assert!(
        shard0.on_actor_ns + shard0.flush_permit_wait_ns <= elapsed_ns,
        "spans 1 and 2 both accrue on the actor task, so together they can never \
         exceed the window"
    );
}

/// The double-counting check, on a scenario whose every span is known: three
/// size-triggered writes at `max_inflight_flushes == 1`, each flush costing
/// SLOW, so the flush tasks are serialized and their spans tile the whole
/// window exactly once.
///
/// The actor does no clock-advancing work at all here: every nanosecond in the
/// window belongs to a flush task. So `on_actor_ns` must be 0, and
/// `on_actor_ns + off_actor_ns` must not exceed the window -- an actor-work span
/// and a flush span cannot both own the same wall nanosecond. Folding the permit
/// wait into `on_actor_ns` breaks exactly that: it reports 2 * SLOW of actor
/// work inside a 3 * SLOW window that `off_actor_ns` already accounts for in
/// full, for 5 * SLOW of attributed work in a window that holds 3.
#[tokio::test]
async fn the_three_spans_do_not_double_count_a_known_window() {
    const SLOW: Duration = Duration::from_secs(5);
    const FLUSHES: u64 = 3;

    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        "/l0/",
        SLOW,
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
    let router = IngestRouter::new(
        contended_flush_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    // Write 1 opens the first flush against a free permit.
    write_one(&router, &tenant, "cpu").await;
    await_slow_puts(&slow_store, 1).await;

    // Writes 2 and 3 each open a flush that must park for a full SLOW: the
    // preceding flush has only just started when its successor's write lands.
    for merged in [2u64, 3] {
        write_one(&router, &tenant, "cpu").await;
        await_points_merged(&router, merged).await;
        clock.advance_ns(SLOW.as_nanos() as i64);
        await_slow_puts(&slow_store, merged).await;
    }
    // Retire the third flush.
    clock.advance_ns(SLOW.as_nanos() as i64);

    let metrics = router.metrics_handle();
    router.shutdown().await;

    let elapsed_ns = (clock.now() - BASE_NS) as u64;
    assert_eq!(
        elapsed_ns,
        FLUSHES * SLOW.as_nanos() as u64,
        "three serialized flushes at SLOW each"
    );

    let shard0 = skew_of(&metrics, 0);
    assert_eq!(shard0.messages_processed, 3);

    // The double-count assertion first, so it is what fails if the spans ever
    // overlap again rather than being masked by an exact-figure assert above it.
    assert!(
        shard0.on_actor_ns + shard0.off_actor_ns <= elapsed_ns,
        "actor work and flush execution cannot both own the same wall nanosecond: \
         on_actor_ns={} + off_actor_ns={} exceeds the {elapsed_ns}ns window, which \
         means one interval was counted twice",
        shard0.on_actor_ns,
        shard0.off_actor_ns
    );
    assert!(
        shard0.on_actor_ns + shard0.flush_permit_wait_ns <= elapsed_ns,
        "the two actor-task spans are disjoint and bounded by the window"
    );

    assert_eq!(shard0.on_actor_ns, 0);
    assert_eq!(
        shard0.flush_permit_wait_ns,
        2 * SLOW.as_nanos() as u64,
        "writes 2 and 3 each parked for one whole preceding flush"
    );
    assert_eq!(
        shard0.off_actor_ns, elapsed_ns,
        "one permit serializes the flushes, so their spans tile the window once"
    );
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
