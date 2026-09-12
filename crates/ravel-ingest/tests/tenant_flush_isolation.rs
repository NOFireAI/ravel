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
use ravel_ingest::{IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::Signal;
use ravel_types::logstream::{AttrValue, log_stream_id};

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
