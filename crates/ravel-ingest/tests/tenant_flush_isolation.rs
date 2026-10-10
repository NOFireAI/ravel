//! Tenant isolation under a stalled flush (issues #1292, #1641).
//!
//! Object keys are tenant-prefixed and S3 throttles per key prefix, so a
//! `503 SlowDown` confined to one busy tenant's prefix used to stall every
//! co-resident tenant whose series hash to the same shard: the shard actor
//! acquired the `max_inflight_flushes` permit ON the actor, so at the bound it
//! parked the whole `select!` loop -- no channel drain, no age-flush tick, no
//! flush reaping -- for the wedged flush's whole duration. The permit acquire
//! now runs inside the spawned flush task, so the actor keeps draining and
//! ticking while a prior flush is stalled.
//!
//! All three pipelines carry that defect and all three carry the fix, so all
//! three are pinned here, one test each: a flush stalled on tenant A's prefix
//! must not stop tenant B's age trigger on the same shard. The three actor
//! loops run the same three arms (channel receive, age tick, flush reaping), so
//! the age trigger is a single observation that covers all of what a parked
//! actor stops.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{StallingStore, TestClock, make_point, tenant};
use ravel_ingest::{
    Clock, IngestConfig, IngestRouter, LogIngestRouter, LogWriteError, SpanIngestRouter,
    SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

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

    // The gauge alone does not separate the two shapes: the in-flight increment
    // happens before the acquire, so an on-actor acquire would move it too and
    // then park. What no parked actor can do is handle the NEXT message, so
    // tenant C's write is the assertion that fails under any on-actor acquire,
    // whatever the gauge does.
    let tenant_c = tenant("third-tenant");
    router
        .write(
            tenant_c.clone(),
            vec![make_point(&tenant_c, "m", &[("h", "1")], start_ns, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");
    let after_age = poll_until(|| processed(&router, 0), 3).await;
    assert_eq!(
        after_age, 3,
        "the actor keeps draining after the age trigger too: two flushes are now \
         waiting on the one permit and neither of them is on the actor"
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

/// A buffered flush that queues behind a stalled prefix must not burn its
/// abandonment lifetime while it waits for the shard permit (issue #1739).
///
/// Tenant A's data PUT stalls, holding the shard's one permit. Tenant B writes
/// in buffered mode, so its rows are acknowledged at enqueue; its age-triggered
/// flush then queues on the permit A holds. The injected clock advances past
/// `max_flush_lifetime` while B is still queued, which also crosses A's own
/// flush-open deadline, so A's stalled PUT is abandoned and releases the
/// permit. B then acquires it -- but with the deadline pinned at flush-open, B's
/// own lifetime has already elapsed in the queue, so B is abandoned before it
/// attempts a PUT and its already-acked rows are dropped with no crash. The fix
/// re-derives the deadline from the moment the permit is granted, so B gets its
/// full lifetime for its own store calls and its rows reach the store.
#[tokio::test]
async fn buffered_flush_queued_behind_a_stall_reaches_the_store_past_lifetime() {
    let start_ns = 1_700_000_000_000_000_000;
    let clock = TestClock::new(start_ns);

    let tenant_a = tenant("throttled-prefix-tenant");
    let tenant_b = tenant("healthy-tenant");
    // A's data object key is `t/<a_hash_hex>/metrics/l0/...`; stalling on this
    // substring stalls A's PUT and nothing of B's. B's own objects live under
    // `t/<b_hash_hex>/`, the prefix the durability assertion reads.
    let a_hash_hex = tenant_a.hash().to_hex();
    let b_hash_hex = tenant_b.hash().to_hex();

    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), a_hash_hex, 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();

    let max_flush_delay = Duration::from_secs(2);
    // B is buffered (no strict waiter) and tiny, so its buffer takes the idle
    // age threshold, not `max_flush_delay`; the test advances past this to open
    // B's flush.
    let max_flush_delay_idle = Duration::from_secs(40);
    let max_flush_lifetime = Duration::from_secs(3600);
    let config = IngestConfig {
        shard_count: 1,
        // Small enough that A's wide batch size-triggers at once, large enough
        // that B's single point stays buffered until its age trigger fires.
        target_bytes: 4096,
        max_flush_delay,
        max_flush_delay_idle,
        max_flush_lifetime,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    ));

    // Tenant A: a wide strict write that crosses target_bytes, so it opens a
    // size flush whose data PUT stalls on A's prefix, holding the one permit.
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

    // Tenant B: a single buffered point. Buffered mode acks at enqueue, so these
    // rows are already acknowledged; an abandoned flush drops them with no crash.
    router
        .write(
            tenant_b.clone(),
            vec![make_point(&tenant_b, "m", &[("h", "1")], start_ns, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");
    let seen = poll_until(|| processed(&router, 0), 2).await;
    assert_eq!(
        seen, 2,
        "the actor must keep draining and buffer B while A's flush is stalled"
    );

    // Advance past the idle age threshold: B's age trigger opens B's flush,
    // which then queues on the one permit A holds. B's flush-open deadline is
    // pinned here.
    clock.advance_ns(max_flush_delay_idle.as_nanos() as i64 + 1);
    let observed = poll_until(|| in_flight(&router, 0), 2).await;
    assert_eq!(
        observed, 2,
        "B's age trigger must spawn its flush task while A holds the permit"
    );

    // Advance past max_flush_lifetime while B is still queued. A's flush-open
    // deadline (pinned at start) is now past too, so A's stalled PUT is
    // abandoned and releases the permit for B.
    clock.advance_ns(max_flush_lifetime.as_nanos() as i64 + 1);

    // Release the (already abandoned) stall and drain: `flush_all` joins every
    // in-flight flush task, so both A's and B's flushes reach a terminal
    // outcome before it returns.
    stalling.release();
    router.flush_all().await;

    // The stall fired exactly once, on A's data PUT: the queue behind it was a
    // real fault, not an unexercised one.
    assert_eq!(
        stalling.stall_hits(),
        1,
        "exactly one PUT stalled, and it was on tenant A's prefix"
    );

    // B's acked buffered rows must be durable. This fails on unmodified code:
    // B's flush burned its lifetime in the queue and was abandoned before its
    // PUT, so nothing lands under B's prefix.
    let b_objects = list_all(store.as_ref(), &format!("t/{b_hash_hex}/"))
        .await
        .expect("list B's objects");
    assert!(
        !b_objects.is_empty(),
        "B's acked buffered rows must reach the store, not be dropped when its \
         queued flush outran a lifetime it spent waiting for the permit: {b_objects:?}"
    );
}

/// The sibling of the test above, past the other bound (issue #1921, ADR-2708
/// D3). A flush granted its permit gets `min(grant + max_flush_lifetime,
/// end(hour) + max_flush_lifetime)`, measured from the hour it was pinned to at
/// flush-open. Maintain seals that hour once its bound passes, so a flush
/// whose queue wait crosses it must not publish into the sealed hour: it is
/// abandoned before any PUT and counted under its own reason, and the hour is
/// not re-pinned to a later one.
///
/// Same rig: A's data PUT stalls on the one permit, B's buffered flush queues
/// behind it. `start_ns` sits 800 s into its hour, so B's pinned hour ends 2800
/// s after it and the bound is 6400 s after it. The clock jumps past that bound
/// while B is queued, so the permit B is granted comes too late.
#[tokio::test]
async fn buffered_flush_queued_past_its_hours_bound_is_abandoned_and_counted() {
    let start_ns: i64 = 1_700_000_000_000_000_000;
    const NS: i64 = 1_000_000_000;
    let hour_ns = 3600 * NS;
    let hour_end_ns = (start_ns / hour_ns + 1) * hour_ns;
    assert_eq!(hour_end_ns - start_ns, 2800 * NS);
    let clock = TestClock::new(start_ns);

    let tenant_a = tenant("throttled-prefix-tenant");
    let tenant_b = tenant("healthy-tenant");
    let a_hash_hex = tenant_a.hash().to_hex();
    let b_hash_hex = tenant_b.hash().to_hex();

    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), a_hash_hex, 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();

    let max_flush_delay_idle = Duration::from_secs(40);
    let max_flush_lifetime = Duration::from_secs(3600);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 4096,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle,
        max_flush_lifetime,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    ));

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
    assert_eq!(in_flight(&router, 0), 1, "A's flush holds the one permit");

    router
        .write(
            tenant_b.clone(),
            vec![make_point(&tenant_b, "m", &[("h", "1")], start_ns, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");
    assert_eq!(poll_until(|| processed(&router, 0), 2).await, 2);

    // B's flush opens and pins the hour `start_ns` is in.
    clock.advance_ns(max_flush_delay_idle.as_nanos() as i64 + 1);
    assert_eq!(
        poll_until(|| in_flight(&router, 0), 2).await,
        2,
        "B's age trigger must spawn its flush task while A holds the permit"
    );

    // Past end(hour) + max_flush_lifetime while B is still queued. A's own
    // deadline is long past too, so its stalled PUT is abandoned and the
    // permit goes to B, granted past B's hour bound.
    let hour_bound_ns = hour_end_ns + max_flush_lifetime.as_nanos() as i64;
    clock.advance_ns(hour_bound_ns + NS - clock.now_ns());
    assert!(clock.now_ns() > hour_bound_ns);

    stalling.release();
    router.flush_all().await;

    assert_eq!(
        stalling.stall_hits(),
        1,
        "exactly one PUT stalled, and it was on tenant A's prefix"
    );
    assert_eq!(
        router.metrics().snapshot().abandoned_hour_bound,
        1,
        "B's flush is abandoned under the hour-bound reason, once"
    );
    let b_objects = list_all(store.as_ref(), &format!("t/{b_hash_hex}/"))
        .await
        .expect("list B's objects");
    assert!(
        b_objects.is_empty(),
        "a flush granted past its pinned hour's bound must not PUT anything, \
         into that hour or a re-pinned later one: {b_objects:?}"
    );
}

/// The pre-acquire guard (issue #1739 part 2) abandons a flush whose flush-open
/// deadline already elapsed before its task takes a permit, and counts it under
/// the new queue-deadline reason, distinct from a store failure (part 4). A
/// `max_flush_lifetime` of zero makes the deadline expire at flush-open, so the
/// guard fires deterministically on the injected clock, before any store call.
#[tokio::test]
async fn expired_flush_is_abandoned_in_the_queue_without_a_permit() {
    let start_ns = 1_700_000_000_000_000_000;
    let clock = TestClock::new(start_ns);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let config = IngestConfig {
        shard_count: 1,
        // The flush-open deadline is raw_ns + max_flush_lifetime; zero makes it
        // equal to the flush-open reading, so the flush is already past deadline
        // when its task first polls, and the pre-acquire guard fires.
        max_flush_lifetime: Duration::ZERO,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let t = tenant("acme");
    router
        .write(
            t.clone(),
            vec![make_point(&t, "m", &[("h", "1")], start_ns, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");

    // Flush now: the flush opens already past its deadline, so its task takes the
    // guard's abandon path before acquiring a permit or issuing a store call.
    router.flush_all().await;

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.abandoned_queue_deadline, 1,
        "the expired flush is counted under the queue-deadline reason"
    );
    assert_eq!(
        snap.abandoned_retry_exhausted, 0,
        "no store call ran, so the store-failure reason must not move"
    );

    // No permit was taken and no object written: the guard ran before the
    // acquire and before `run_flush`.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "an expired flush must take no permit and write no object: {objects:?}"
    );
}

fn log_in_flight(router: &LogIngestRouter, shard: u32) -> u64 {
    router
        .metrics()
        .in_flight_flushes_by_shard()
        .into_iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, n)| n)
        .unwrap_or(0)
}

fn log_processed(router: &LogIngestRouter, shard: u32) -> u64 {
    router
        .metrics()
        .shard_skew_by_shard()
        .into_iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, s)| s.messages_processed)
        .unwrap_or(0)
}

/// A consistently-built record: `stream_id` and `stream_attrs` share the same
/// resource inputs, so `RlogWriter::finish`'s collision check passes.
fn norm_record(host: &str, ts_ns: i64) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
        ("host".to_string(), AttrValue::Str(host.to_string())),
    ];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "hello".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// The logs pipeline's copy of the property above. `log_shard.rs` had the same
/// on-actor acquire and the same consequence; issue #1641 moved it into the
/// spawned flush task.
#[tokio::test]
async fn stalled_log_flush_does_not_stop_a_coresident_tenants_age_trigger() {
    let start_ns = 1_700_000_000_000_000_000;
    let clock = TestClock::new(start_ns);

    let tenant_a = tenant("throttled-prefix-tenant");
    let tenant_b = tenant("healthy-tenant");
    // A's RLOG data object key is `t/<a_hash_hex>/logs/...`; stalling on this
    // substring stalls A's PUT and nothing of B's.
    let a_hash_hex = tenant_a.hash().to_hex();
    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), a_hash_hex, 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();

    let max_flush_delay = Duration::from_secs(2);
    let config = IngestConfig {
        shard_count: 1,
        // Small enough that A's batch size-triggers at once, large enough that
        // B's single record stays buffered until its age trigger fires.
        target_bytes: 4096,
        max_flush_delay,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = Arc::new(LogIngestRouter::new(config, store, clock.clone()));

    let a_records: Vec<_> = (0..400u32)
        .map(|i| norm_record(&format!("a{i}"), start_ns))
        .collect();
    let router_a = Arc::clone(&router);
    let tenant_a_moved = tenant_a.clone();
    let _a = tokio::spawn(async move {
        let _ = router_a
            .write(
                tenant_a_moved,
                a_records,
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });
    stalling.wait_until_stalled().await;
    assert_eq!(
        log_in_flight(&router, 0),
        1,
        "A's flush task is spawned and holds the one permit"
    );

    // Tenant B: one strict record, below target_bytes, so it buffers with a
    // waiter (priority => fast age clock = max_flush_delay). Its ack cannot
    // resolve while A holds the permit, so spawn it and do not await.
    let router_b = Arc::clone(&router);
    let _b = tokio::spawn(async move {
        let _ = router_b
            .write(
                tenant_b,
                vec![norm_record("b0", start_ns)],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });

    let seen = poll_until(|| log_processed(&router, 0), 2).await;
    assert_eq!(
        seen, 2,
        "the actor must keep draining and buffer B while A's flush is stalled"
    );
    assert_eq!(
        log_in_flight(&router, 0),
        1,
        "B is buffered, not yet flushed: still just A's flush in flight"
    );

    clock.advance_ns(max_flush_delay.as_nanos() as i64 + 1);

    let observed = poll_until(|| log_in_flight(&router, 0), 2).await;
    assert_eq!(
        observed, 2,
        "B's age trigger must spawn its flush task while A is stalled"
    );

    // See the metrics test: the gauge moves before the acquire either way, so
    // the discriminating assertion is that the actor handles the next message.
    let tenant_c = tenant("third-tenant");
    router
        .write(
            tenant_c,
            vec![norm_record("c0", start_ns)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");
    let after_age = poll_until(|| log_processed(&router, 0), 3).await;
    assert_eq!(
        after_age, 3,
        "the actor keeps draining after the age trigger too: two flushes are now \
         waiting on the one permit and neither of them is on the actor"
    );

    assert_eq!(
        stalling.stall_hits(),
        1,
        "exactly one PUT stalled, and it was on tenant A's prefix"
    );

    stalling.release();
    router.flush_all().await;
}

fn span_in_flight(router: &SpanIngestRouter, shard: u32) -> u64 {
    router
        .metrics()
        .in_flight_flushes_by_shard()
        .into_iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, n)| n)
        .unwrap_or(0)
}

fn norm_span(seed: u32, start_ns: i64) -> NormalizedSpan {
    let mut trace_id = [0u8; 16];
    trace_id[..4].copy_from_slice(&seed.to_be_bytes());
    let mut span_id = [0u8; 8];
    span_id[..4].copy_from_slice(&seed.to_be_bytes());
    NormalizedSpan {
        trace_id,
        span_id,
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: start_ns,
        end_ts_ns: start_ns + 100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// The spans pipeline's copy of the property above. `span_shard.rs` carries no
/// per-shard skew instrumentation (issue #865 never reached it), so the
/// "the actor kept draining" step reads `buffered_spans_total` -- incremented
/// by the actor as it merges each write -- instead of `messages_processed`.
#[tokio::test]
async fn stalled_span_flush_does_not_stop_a_coresident_tenants_age_trigger() {
    const A_SPANS: u32 = 400;

    let start_ns = 1_700_000_000_000_000_000;
    let clock = TestClock::new(start_ns);

    let tenant_a = tenant("throttled-prefix-tenant");
    let tenant_b = tenant("healthy-tenant");
    // A's RSPAN data object key is `t/<a_hash_hex>/s/...`.
    let a_hash_hex = tenant_a.hash().to_hex();
    let stalling = Arc::new(StallingStore::new(MemoryStore::new(), a_hash_hex, 1));
    let store: Arc<dyn ObjectStoreBackend> = stalling.clone();

    let max_flush_delay = Duration::from_secs(2);
    let config = IngestConfig {
        // One shard, so both tenants' spans land on it whatever their trace ids
        // hash to: spans shard by trace id, not by tenant.
        shard_count: 1,
        target_bytes: 4096,
        max_flush_delay,
        flush_tick: Duration::from_millis(50),
        max_inflight_flushes: 1,
        ..IngestConfig::default()
    };
    let router = Arc::new(SpanIngestRouter::new(config, store, clock.clone()));

    let a_spans: Vec<_> = (0..A_SPANS).map(|i| norm_span(i, start_ns)).collect();
    let router_a = Arc::clone(&router);
    let tenant_a_moved = tenant_a.clone();
    let _a = tokio::spawn(async move {
        let _ = router_a
            .write(
                tenant_a_moved,
                a_spans,
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });
    stalling.wait_until_stalled().await;
    assert_eq!(
        span_in_flight(&router, 0),
        1,
        "A's flush task is spawned and holds the one permit"
    );

    let router_b = Arc::clone(&router);
    let _b = tokio::spawn(async move {
        let _ = router_b
            .write(
                tenant_b,
                vec![norm_span(A_SPANS, start_ns)],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await;
    });

    let buffered = poll_until(
        || router.metrics().snapshot().buffered_spans_total,
        A_SPANS as u64 + 1,
    )
    .await;
    assert_eq!(
        buffered,
        A_SPANS as u64 + 1,
        "the actor must keep draining and buffer B's span while A's flush is stalled"
    );
    assert_eq!(
        span_in_flight(&router, 0),
        1,
        "B is buffered, not yet flushed: still just A's flush in flight"
    );

    clock.advance_ns(max_flush_delay.as_nanos() as i64 + 1);

    let observed = poll_until(|| span_in_flight(&router, 0), 2).await;
    assert_eq!(
        observed, 2,
        "B's age trigger must spawn its flush task while A is stalled"
    );

    // See the metrics test: the gauge moves before the acquire either way, so
    // the discriminating assertion is that the actor handles the next message.
    let tenant_c = tenant("third-tenant");
    router
        .write(
            tenant_c,
            vec![norm_span(A_SPANS + 1, start_ns)],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("a buffered write acks at enqueue");
    let after_age = poll_until(
        || router.metrics().snapshot().buffered_spans_total,
        A_SPANS as u64 + 2,
    )
    .await;
    assert_eq!(
        after_age,
        A_SPANS as u64 + 2,
        "the actor keeps draining after the age trigger too: two flushes are now \
         waiting on the one permit and neither of them is on the actor"
    );

    assert_eq!(
        stalling.stall_hits(),
        1,
        "exactly one PUT stalled, and it was on tenant A's prefix"
    );

    stalling.release();
    router.flush_all().await;
}

// ---------------------------------------------------------------------------
// Per-tenant flush shares (issue #1921, ADR-2708 D3)
// ---------------------------------------------------------------------------
//
// The tests above pin that a stalled flush does not park the actor. These pin
// that it does not take every flush permit either: with the default four
// permits a tenant owns a share of three, so one tenant whose PUTs are all held
// leaves a permit for its neighbours on the same shard.

/// The default permit count, and the share it resolves to (`N - 1`).
const DEFAULT_PERMITS: u32 = 4;
const SHARE: usize = 3;

/// Past `max_flush_delay` (50 ms) on every tick.
const SHARE_TICK_NS: i64 = 100_000_000;

const SHARE_BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Bound on every cooperative wait below, so a defect the tests exist to
/// catch fails an assertion instead of hanging.
const YIELD_LIMIT: usize = 10_000;

async fn settles(mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..YIELD_LIMIT {
        if probe() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    probe()
}

/// One shard, the default permit count and share, and only the age trigger.
fn share_config(max_queued_flushes: usize) -> IngestConfig {
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_queued_flushes,
        ..IngestConfig::default()
    };
    assert_eq!(config.max_inflight_flushes, DEFAULT_PERMITS);
    assert_eq!(config.max_inflight_flushes_per_tenant, None);
    config
}

/// Holds every PUT under `tenant`'s prefix until released.
fn hold_tenant(store: &FaultStore<MemoryStore>, tenant: &TenantId) -> GateHandle {
    store.hold(
        Op::Put,
        Some(format!("t/{}/", tenant.hash().to_hex())),
        Occurrence::Always,
    )
}

fn held_for(gate: &GateHandle, tenant: &TenantId) -> usize {
    let hex = tenant.hash().to_hex();
    gate.held_details()
        .into_iter()
        .filter(|(_, _, key)| key.contains(&hex))
        .count()
}

/// Ticks the injected clock, which is what drives the actor's age trigger,
/// until every handle has finished, so a deferred trigger re-fires.
async fn drain<T>(clock: &TestClock, writes: &[tokio::task::JoinHandle<T>]) {
    for _ in 0..400 {
        if writes.iter().all(|w| w.is_finished()) {
            return;
        }
        clock.advance_ns(SHARE_TICK_NS);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    panic!("writes did not finish after their PUTs were released");
}

/// Releases every call `gate` holds from now on, so the tests can drain.
fn release_all_from_now(gate: GateHandle) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            gate.wait_until_held(1).await;
            for id in gate.held() {
                gate.release(id);
            }
        }
    })
}

fn spawn_metric_write(
    router: &Arc<IngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::WriteReceipt, WriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let host = format!("h{i}");
        let point = make_point(
            &tenant,
            "cpu_usage",
            &[("host", &host)],
            1_000 + i as i64,
            1.0,
        );
        router
            .write(
                tenant,
                vec![point],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
    })
}

fn spawn_log_write(
    router: &Arc<LogIngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::LogWriteReceipt, LogWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        router
            .write(
                tenant,
                vec![norm_record(&format!("h{i}"), 1_000 + i as i64)],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
    })
}

fn spawn_span_write(
    router: &Arc<SpanIngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::SpanWriteReceipt, SpanWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        router
            .write(
                tenant,
                vec![norm_span(i as u32 + 1, 1_000 + i as i64)],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
    })
}

fn new_metrics_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> IngestRouter {
    IngestRouter::new(config, store, Signal::Metrics, clock)
}

fn new_log_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> LogIngestRouter {
    LogIngestRouter::new(config, store, clock)
}

fn new_span_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> SpanIngestRouter {
    SpanIngestRouter::new(config, store, clock)
}

/// The three share tests, once per pipeline. `$new` builds the router,
/// `$spawn` issues one strict single-item write, `$buffered` is the snapshot's
/// cumulative buffered-item counter.
macro_rules! flush_share_tests {
    ($coresident:ident, $at_share:ident, $nothing_in_flight:ident, $new:ident, $spawn:ident, $buffered:ident) => {
        /// Tenant A's PUTs are all held; A fills its share of three flushes
        /// and its fourth trigger is deferred. Tenant B's strict write on the
        /// same shard takes the fourth permit and acks within its write
        /// deadline.
        #[tokio::test]
        async fn $coresident() {
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault.clone();
            let clock = TestClock::new(SHARE_BASE_NS);
            let router = Arc::new($new(share_config(8), store, clock.clone()));
            let (a, b) = (tenant("held-tenant"), tenant("strict-tenant"));
            let gate = hold_tenant(&fault, &a);
            let a_writes = fill_share!(router, clock, gate, a, $spawn, $buffered);

            let b_write = $spawn(&router, &b, 100);
            let want = (SHARE + 2) as u64;
            assert!(settles(|| router.metrics().snapshot().$buffered >= want).await);
            clock.advance_ns(SHARE_TICK_NS);
            assert!(
                settles(|| b_write.is_finished()).await,
                "B's strict write must ack while A's flushes hold every PUT \
                 they reached; A in flight {}",
                in_flight_of!(router)
            );
            let receipt = b_write
                .await
                .expect("B's write task")
                .expect("B's strict write acks within its deadline");
            assert_eq!(receipt.tokens.len(), 1);
            assert_eq!(
                held_for(&gate, &a),
                SHARE,
                "the hold fired on A's data PUTs, one per flush in A's share"
            );
            assert!(a_writes.iter().all(|w| !w.is_finished()));

            let _releaser = release_all_from_now(gate.clone());
            drain(&clock, &a_writes).await;
            for w in a_writes {
                w.await
                    .expect("A's write task")
                    .expect("A acks once released");
            }
            router.flush_all().await;
        }

        /// The permit A leaves free is B's to take: with B's PUTs held too,
        /// B's flush reaches its PUT beside A's three, and A's own next
        /// trigger stays deferred. Counting the share per shard instead of
        /// per (shard, tenant) refuses B's trigger as well.
        #[tokio::test]
        async fn $at_share() {
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault.clone();
            let clock = TestClock::new(SHARE_BASE_NS);
            let router = Arc::new($new(share_config(8), store, clock.clone()));
            let (a, b) = (tenant("held-tenant"), tenant("second-tenant"));
            let gate = hold_tenant(&fault, &a);
            let _b_gate = hold_tenant(&fault, &b);
            let a_writes = fill_share!(router, clock, gate, a, $spawn, $buffered);
            let deferred_before = deferred_of!(router);

            let b_write = $spawn(&router, &b, 100);
            let want = (SHARE + 2) as u64;
            assert!(settles(|| router.metrics().snapshot().$buffered >= want).await);
            clock.advance_ns(SHARE_TICK_NS);
            assert!(
                settles(|| held_for(&gate, &b) == 1).await,
                "B's flush must take the permit A's share leaves free and reach \
                 its PUT; in flight {}",
                in_flight_of!(router)
            );
            assert_eq!(in_flight_of!(router), (SHARE + 1) as u64);
            assert_eq!(held_for(&gate, &a), SHARE);
            assert!(
                deferred_of!(router) > deferred_before,
                "A's deferred trigger is refused again on the tick B's opens"
            );

            let _releaser = release_all_from_now(gate.clone());
            drain(&clock, &a_writes).await;
            drain(&clock, std::slice::from_ref(&b_write)).await;
            b_write
                .await
                .expect("B's write task")
                .expect("B acks once released");
            for w in a_writes {
                w.await
                    .expect("A's write task")
                    .expect("A acks once released");
            }
            router.flush_all().await;
        }

        /// A queued-flush cap of three is full with A's three held flushes.
        /// B has nothing in flight, so its trigger is not refused at the cap:
        /// it spawns, past the cap, and acks.
        #[tokio::test]
        async fn $nothing_in_flight() {
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault.clone();
            let clock = TestClock::new(SHARE_BASE_NS);
            let router = Arc::new($new(share_config(SHARE), store, clock.clone()));
            let (a, b) = (tenant("held-tenant"), tenant("idle-tenant"));
            let gate = hold_tenant(&fault, &a);
            let a_writes = fill_share!(router, clock, gate, a, $spawn, $buffered);
            assert_eq!(queued_of!(router), SHARE as u64, "the queue is at its cap");

            let b_write = $spawn(&router, &b, 100);
            let want = (SHARE + 2) as u64;
            assert!(settles(|| router.metrics().snapshot().$buffered >= want).await);
            clock.advance_ns(SHARE_TICK_NS);
            assert!(
                settles(|| b_write.is_finished()).await,
                "B, with nothing in flight, must not be refused at the queued-flush \
                 cap; in flight {}",
                in_flight_of!(router)
            );
            let receipt = b_write
                .await
                .expect("B's write task")
                .expect("B's strict write acks past the cap");
            assert_eq!(receipt.tokens.len(), 1);
            assert_eq!(held_for(&gate, &a), SHARE);

            let _releaser = release_all_from_now(gate.clone());
            drain(&clock, &a_writes).await;
            for w in a_writes {
                w.await
                    .expect("A's write task")
                    .expect("A acks once released");
            }
            router.flush_all().await;
        }
    };
}

macro_rules! in_flight_of {
    ($router:expr) => {
        $router
            .metrics()
            .in_flight_flushes_by_shard()
            .into_iter()
            .map(|(_, n)| n)
            .sum::<u64>()
    };
}

macro_rules! queued_of {
    ($router:expr) => {
        $router
            .metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flushes_queued)
            .sum::<u64>()
    };
}

macro_rules! deferred_of {
    ($router:expr) => {
        $router
            .metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flush_trigger_deferred)
            .sum::<u64>()
    };
}

/// Drives tenant `$a` to its share: `SHARE` strict writes, one age tick each,
/// every flush held at its PUT, then one more write whose trigger must be
/// deferred rather than spawned. Returns the `SHARE + 1` write handles.
macro_rules! fill_share {
    ($router:ident, $clock:ident, $gate:ident, $a:ident, $spawn:ident, $buffered:ident) => {{
        let mut writes = Vec::new();
        for i in 0..SHARE {
            writes.push($spawn(&$router, &$a, i));
            let want = (i + 1) as u64;
            assert!(settles(|| $router.metrics().snapshot().$buffered >= want).await);
            $clock.advance_ns(SHARE_TICK_NS);
            assert!(
                settles(|| in_flight_of!($router) == want).await,
                "A's flush {i} spawns under its share; in flight {}",
                in_flight_of!($router)
            );
        }
        assert!(settles(|| held_for(&$gate, &$a) == SHARE).await);
        writes.push($spawn(&$router, &$a, SHARE));
        let want = (SHARE + 1) as u64;
        assert!(settles(|| $router.metrics().snapshot().$buffered >= want).await);
        $clock.advance_ns(SHARE_TICK_NS);
        assert!(
            settles(|| deferred_of!($router) >= 1 || in_flight_of!($router) > SHARE as u64).await
        );
        assert_eq!(
            in_flight_of!($router),
            SHARE as u64,
            "A's trigger at its share of {SHARE} is deferred, not spawned"
        );
        writes
    }};
}

// ---------------------------------------------------------------------------
// Grant deadline bounded by the pinned hour (ADR-2708 D3), log and span
// actors. The metrics actor's copies are unit tests in `shard.rs`.
// ---------------------------------------------------------------------------

/// An exact ingest-hour boundary, and a base one second before it, so every
/// flush below opens and pins in the hour ending at `GRANT_BOUNDARY_NS`.
const GRANT_BOUNDARY_NS: i64 = 472_223 * 3_600_000_000_000;
const GRANT_BASE_NS: i64 = GRANT_BOUNDARY_NS - 1_000_000_000;

/// One permit, so `parked`'s held flush leaves `late`'s queued on the shard
/// semaphore; `late` has nothing in flight, so neither its share nor the queue
/// cap refuses it.
fn grant_config(lifetime: Duration) -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_inflight_flushes: 1,
        max_queued_flushes: 4,
        max_flush_lifetime: lifetime,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// The clamp and abandon tests, once per pipeline.
macro_rules! grant_deadline_tests {
    ($new:ident, $spawn:ident, $buffered:ident) => {
        /// Opens `parked`'s flush and parks it at its first PUT, then opens
        /// `late`'s behind it on the one permit. `late`'s every PUT is held.
        macro_rules! grant_rig {
            ($lifetime:expr) => {{
                let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
                let store: Arc<dyn ObjectStoreBackend> = fault.clone();
                let clock = TestClock::new(GRANT_BASE_NS);
                let router = Arc::new($new(grant_config($lifetime), store, clock.clone()));
                let (parked, late) = (tenant("parked"), tenant("late"));
                let parked_gate = fault.hold(
                    Op::Put,
                    Some(format!("t/{}/", parked.hash().to_hex())),
                    Occurrence::Nth(1),
                );
                let late_gate = hold_tenant(&fault, &late);
                let first = $spawn(&router, &parked, 0);
                assert!(settles(|| router.metrics().snapshot().$buffered >= 1).await);
                clock.advance_ns(SHARE_TICK_NS);
                assert!(settles(|| in_flight_of!(router) == 1).await);
                assert!(settles(|| held_for(&parked_gate, &parked) == 1).await);
                let queued = $spawn(&router, &late, 1);
                assert!(settles(|| router.metrics().snapshot().$buffered >= 2).await);
                clock.advance_ns(SHARE_TICK_NS);
                assert!(
                    settles(|| in_flight_of!(router) == 2).await,
                    "late's flush opens behind parked's on the one permit"
                );
                assert!(clock.now_ns() < GRANT_BOUNDARY_NS);
                (
                    fault,
                    router,
                    clock,
                    parked,
                    late,
                    parked_gate,
                    late_gate,
                    first,
                    queued,
                )
            }};
        }

        /// See the metrics actor's copy in `shard.rs`.
        #[tokio::test]
        async fn a_flush_granted_past_its_pinned_hours_lifetime_is_abandoned_without_a_put() {
            let lifetime = Duration::from_secs(3600);
            let (fault, router, clock, _parked, late, parked_gate, late_gate, first, queued) =
                grant_rig!(lifetime);
            let objects_before = list_all(fault.as_ref(), "t/").await.expect("list").len();

            let hour_bound_ns = GRANT_BOUNDARY_NS + lifetime.as_nanos() as i64;
            clock.advance_ns(hour_bound_ns + 1_000_000_000 - clock.now_ns());
            assert!(
                settles(|| queued.is_finished()).await,
                "late's flush reaches a terminal outcome once granted"
            );
            assert_eq!(router.metrics().snapshot().abandoned_hour_bound, 1);
            assert_eq!(
                held_for(&late_gate, &late),
                0,
                "the flush granted past its hour's bound attempted no PUT"
            );
            assert_eq!(
                list_all(fault.as_ref(), "t/").await.expect("list").len(),
                objects_before,
                "no object was written into the sealed hour or a later one"
            );
            let answer = queued.await.expect("write task");
            assert!(
                answer.is_err(),
                "late's strict write is not acked: {answer:?}"
            );

            let _releaser = release_all_from_now(parked_gate);
            drain(&clock, std::slice::from_ref(&first)).await;
            router.flush_all().await;
        }

        /// See the metrics actor's copy in `shard.rs`.
        #[tokio::test]
        async fn a_flush_granted_inside_the_hour_is_clamped_to_the_hour_end() {
            let lifetime = Duration::from_secs(7200);
            let (_fault, router, clock, parked, late, parked_gate, late_gate, first, queued) =
                grant_rig!(lifetime);

            let grant_ns = GRANT_BOUNDARY_NS + 600 * 1_000_000_000;
            clock.advance_ns(grant_ns - clock.now_ns());
            let hex = parked.hash().to_hex();
            let (id, _, _) = parked_gate
                .held_details()
                .into_iter()
                .find(|(_, _, key)| key.contains(&hex))
                .expect("parked's PUT is held");
            assert!(parked_gate.release(id));
            assert!(
                settles(|| held_for(&late_gate, &late) == 1).await,
                "late is granted the permit and parks at its own PUT"
            );
            assert!(!queued.is_finished());

            let hour_bound_ns = GRANT_BOUNDARY_NS + lifetime.as_nanos() as i64;
            clock.advance_ns(hour_bound_ns + 1_000_000_000 - clock.now_ns());
            assert!(clock.now_ns() < grant_ns + lifetime.as_nanos() as i64);
            assert!(
                settles(|| queued.is_finished()).await,
                "the held PUT is abandoned at the hour bound, not at grant + lifetime"
            );
            let answer = queued.await.expect("write task");
            assert!(
                answer.is_err(),
                "late's strict write is not acked: {answer:?}"
            );
            assert_eq!(router.metrics().snapshot().abandoned_hour_bound, 0);
            first
                .await
                .expect("write task")
                .expect("parked's flush lands once released");

            let _releaser = release_all_from_now(late_gate);
            router.flush_all().await;
        }
    };
}

flush_share_tests!(
    coresident_strict_write_acks_within_its_deadline_while_a_tenants_puts_are_held,
    a_tenant_at_its_share_leaves_a_permit_free,
    a_tenant_with_nothing_in_flight_is_not_refused_at_the_queue_cap,
    new_metrics_router,
    spawn_metric_write,
    buffered_points_total
);

mod logs {
    use super::*;

    flush_share_tests!(
        coresident_strict_write_acks_within_its_deadline_while_a_tenants_puts_are_held,
        a_tenant_at_its_share_leaves_a_permit_free,
        a_tenant_with_nothing_in_flight_is_not_refused_at_the_queue_cap,
        new_log_router,
        spawn_log_write,
        buffered_records_total
    );

    grant_deadline_tests!(new_log_router, spawn_log_write, buffered_records_total);
}

mod spans {
    use super::*;

    flush_share_tests!(
        coresident_strict_write_acks_within_its_deadline_while_a_tenants_puts_are_held,
        a_tenant_at_its_share_leaves_a_permit_free,
        a_tenant_with_nothing_in_flight_is_not_refused_at_the_queue_cap,
        new_span_router,
        spawn_span_write,
        buffered_spans_total
    );

    grant_deadline_tests!(new_span_router, spawn_span_write, buffered_spans_total);
}
