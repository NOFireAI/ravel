//! The put-retry budget is exactly `put_retry_max_attempts + 1` total inner
//! PUT calls, pinned by counting scripted `FaultStore` faults,
//! and the retry backoff waits on the injected `Clock` rather than the real
//! timer, so its timing is observable and controllable with no wall-clock
//! sleep.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first write (`target_bytes` tiny) and never on age. With
/// zero backoff delay every retry runs back to back on the injected clock,
/// so a retry-exhaustion count is deterministic without advancing the clock.
fn exhaustion_config(max_attempts: u32) -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: max_attempts,
        put_retry_base_delay: Duration::from_millis(0),
        put_retry_max_delay: Duration::from_millis(0),
        ..IngestConfig::default()
    }
}

fn one_point() -> Vec<ravel_otlp::NormalizedPoint> {
    vec![make_point(
        &tenant("acme"),
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )]
}

/// A permanently-retryable fault on every data-object PUT drives exactly
/// `put_retry_max_attempts + 1` inner PUT calls before the flush is abandoned:
/// one first attempt plus `max_attempts` retries. Counted straight off the
/// `FaultStore`'s own fired-fault counter.
#[tokio::test]
async fn data_put_makes_exactly_max_attempts_plus_one_calls() {
    let max_attempts = 3;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Transient("data down".into())).with_key_contains("/l0/"),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        exhaustion_config(max_attempts),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let err = router
        .write(
            tenant("acme"),
            one_point(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a data PUT that always fails must abandon the flush");
    assert!(err.is_retryable(), "abandonment is retryable: {err}");

    assert_eq!(
        fault.fault_count(Op::Put, FaultKind::Transient),
        u64::from(max_attempts) + 1,
        "total data PUT calls must be max_attempts + 1"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.put_retries,
        u64::from(max_attempts),
        "put_retries counts the retries only, i.e. max_attempts"
    );
    assert_eq!(snap.abandoned_retry_exhausted, 1);

    // The data object never landed, so no orphan either.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "a failed data PUT leaves nothing behind"
    );

    router.shutdown().await;
}

/// Same budget on the commit-record PUT: the data object lands, then every
/// commit PUT fails, giving `max_attempts + 1` commit PUT calls before the
/// flush is abandoned (with the data object left orphaned).
#[tokio::test]
async fn commit_put_makes_exactly_max_attempts_plus_one_calls() {
    let max_attempts = 3;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Transient("commit down".into())).with_key_contains("/c/"),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        exhaustion_config(max_attempts),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let err = router
        .write(
            tenant("acme"),
            one_point(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a commit PUT that always fails must abandon the flush");
    assert!(err.is_retryable(), "abandonment is retryable: {err}");

    // Every fired Transient fault is a commit PUT (the data PUT succeeds),
    // so the counter is exactly the commit-path call count.
    assert_eq!(
        fault.fault_count(Op::Put, FaultKind::Transient),
        u64::from(max_attempts) + 1,
        "total commit PUT calls must be max_attempts + 1"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.put_retries, u64::from(max_attempts));
    assert_eq!(snap.abandoned_retry_exhausted, 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|o| o.key.contains("/l0/")),
        "the data object landed (now orphaned) before the commit PUT failed"
    );
    assert!(
        !objects.iter().any(|o| o.key.contains("/c/")),
        "no commit record ever lands"
    );

    router.shutdown().await;
}

/// The retry backoff waits on the injected `Clock`: with a huge backoff delay
/// the flush parks in backoff after one retryable data-PUT fault and does not
/// complete until the test advances the injected clock past that delay. If the
/// backoff used the real tokio timer, advancing the injected clock would have
/// no effect and this test could only finish by really sleeping the delay.
#[tokio::test]
async fn retry_backoff_waits_on_the_injected_clock() {
    // One data-PUT fault then a passthrough success: exactly one retry, so
    // exactly one backoff wait to control.
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Put)
            .with_key_contains("/l0/")
            .then_fault(ScriptedFault::Transient("blip".into()))
            .then_passthrough(),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(BASE_NS);
    // A backoff delay far larger than any real test would wait for: only an
    // injected-clock advance can retire it.
    let backoff = Duration::from_secs(1_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 4,
        put_retry_base_delay: backoff,
        put_retry_max_delay: backoff,
        ..IngestConfig::default()
    };
    let router = Arc::new(IngestRouter::new(
        config,
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    ));

    let write_router = Arc::clone(&router);
    let handle = tokio::spawn(async move {
        write_router
            .write(
                tenant("acme"),
                one_point(),
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
    });

    // Wait until the flush has taken the one retry and parked in backoff.
    while router.metrics().snapshot().put_retries < 1 {
        tokio::task::yield_now().await;
    }
    // The clock has not moved, so the backoff must still be blocking: the
    // write cannot have completed.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        !handle.is_finished(),
        "the flush must stay parked in backoff while the injected clock is still"
    );

    // Advance past the (jittered, but <= backoff) delay: the backoff retires
    // and the retry's passthrough PUT then commits.
    clock.advance_ns(1_100 * 1_000_000_000);
    let receipt = handle
        .await
        .expect("join")
        .expect("the retried flush commits once the clock advances");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(router.metrics().snapshot().put_retries, 1);

    // Unwrap the Arc so `shutdown` (which consumes the router) can run.
    let router = match Arc::try_unwrap(router) {
        Ok(router) => router,
        Err(_) => panic!("sole router owner"),
    };
    router.shutdown().await;
}
