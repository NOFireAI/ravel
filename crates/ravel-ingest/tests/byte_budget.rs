//! Process-wide ingest buffer byte budget (ADR-0069 decision 1):
//! a request's estimated buffered bytes are charged at admission and refunded
//! when the flush holding them completes or fails. These tests drive the real
//! [`IngestRouter`] with a bounded budget and a [`FaultStore`] whose hold gates
//! keep flushes in flight, so the charge/refund lifecycle is observed end to
//! end rather than in the budget's own unit tests.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, WriteError, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::Signal;

/// A single-point write's charge, per [`IngestPoint::est_charge_bytes`]: 16
/// bytes for the sample plus, per label, the `Label` struct header (two
/// `String` headers, 48 bytes) and the name/value bytes. `make_point(_, "m",
/// &[], ..)` carries exactly one label, `__name__="m"` (8 + 1 bytes), so the
/// charge is 16 + 48 + 9 = 73. Hard-coded here on purpose: a change to the
/// estimate formula must break this test, not silently shift the ceiling
/// arithmetic below.
const PER_POINT_CHARGE: u64 = 73;

fn budgeted_router(
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
    budget: Arc<IngestByteBudget>,
    max_inflight_flushes: u32,
) -> Arc<IngestRouter> {
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        // A far-future age threshold so nothing flushes on the tick: every
        // flush in these tests is size-triggered by a single point exceeding
        // `target_bytes`, and stays in flight only because a gate holds its PUT.
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        max_inflight_flushes,
        ..IngestConfig::default()
    };
    Arc::new(IngestRouter::new(config, store, Signal::Metrics, clock).with_budget(budget))
}

/// A request that would push the gauge past the ceiling is shed before any
/// buffering: it returns [`WriteError::BufferBudgetExceeded`], mints no commit
/// token, buffers nothing, and increments the shed counter, while the bytes
/// already held by the in-flight (gate-held) flushes stay charged.
#[tokio::test]
async fn ceiling_shed_before_buffering() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);

    // Ceiling admits two single-point writes (2 * 73 = 146) but not a third
    // (146 + 73 = 219 > 180).
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(180));
    let router = budgeted_router(store, clock.clone(), budget.clone(), 2);

    // Hold every data-object PUT so the two fill flushes stay in flight,
    // keeping their charges on the gauge (the fault injection this test's
    // shed depends on; asserted fired via `held_count` below).
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);

    // Two tenants, each one point, each flushing immediately (target_bytes 8 <
    // the 73-byte point) and parking at the held data PUT.
    let mut fills = Vec::new();
    for name in ["acme", "globex"] {
        let router = Arc::clone(&router);
        let t = tenant(name);
        fills.push(tokio::spawn(async move {
            let points = vec![make_point(&t, "m", &[], 1_000, 1.0)];
            router
                .write(t, points, WriteMode::Strict, Duration::from_secs(30))
                .await
        }));
    }

    gate.wait_until_held(2).await;
    assert_eq!(
        gate.held_count(),
        2,
        "both fill flushes held at their data PUT"
    );
    assert_eq!(
        budget.in_flight_bytes(),
        2 * PER_POINT_CHARGE,
        "both fills' charges are held on the gauge while their flushes are in flight"
    );

    // A third write would charge 73 more (219 > 180): it must shed before
    // routing. Buffered mode so the assertion does not depend on ack timing;
    // the shed happens in `write_points` before any shard is touched
    // regardless of mode.
    let victim = tenant("initech");
    let shed = router
        .write(
            victim,
            vec![make_point(&tenant("initech"), "m", &[], 1_000, 1.0)],
            WriteMode::Buffered,
            Duration::from_secs(30),
        )
        .await;
    assert!(
        matches!(shed, Err(WriteError::BufferBudgetExceeded)),
        "over-ceiling request sheds with BufferBudgetExceeded, got {shed:?}"
    );
    assert_eq!(budget.shed_total(), 1, "the shed is counted");
    assert_eq!(
        budget.in_flight_bytes(),
        2 * PER_POINT_CHARGE,
        "a shed request charges nothing: the gauge is unchanged, so nothing was buffered"
    );

    // Release the held flushes so the spawned writes finish and the runtime
    // has no detached in-flight PUTs at teardown.
    for id in gate.held() {
        gate.release(id);
    }
    for fill in fills {
        fill.await
            .expect("join fill")
            .expect("fill commits once released");
    }
}

/// Once the held flushes are released and drain to a durable commit, every
/// charge is refunded and the gauge returns to its exact pre-fill baseline.
#[tokio::test]
async fn refund_on_flush_completion() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(10_000));
    let router = budgeted_router(store, clock.clone(), budget.clone(), 2);

    assert_eq!(
        budget.in_flight_bytes(),
        0,
        "baseline is zero before any write"
    );

    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);

    let mut writes = Vec::new();
    for name in ["acme", "globex"] {
        let router = Arc::clone(&router);
        let t = tenant(name);
        writes.push(tokio::spawn(async move {
            let points = vec![make_point(&t, "m", &[], 1_000, 1.0)];
            router
                .write(t, points, WriteMode::Strict, Duration::from_secs(30))
                .await
        }));
    }

    gate.wait_until_held(2).await;
    assert_eq!(
        budget.in_flight_bytes(),
        2 * PER_POINT_CHARGE,
        "both charges held while their flushes are in flight"
    );

    // Release both held data PUTs; each flush finishes its commit and acks.
    for id in gate.held() {
        gate.release(id);
    }
    for w in writes {
        let receipt = w.await.expect("join write").expect("strict write commits");
        assert_eq!(receipt.tokens.len(), 1, "one token per flushed shard");
    }

    assert_eq!(
        budget.in_flight_bytes(),
        0,
        "every charge is refunded once its flush completes: gauge returns to the exact baseline"
    );
}

/// A flush that fails permanently still refunds its bytes: the gauge returns
/// to baseline on the failure path, not only the success path.
#[tokio::test]
async fn refund_on_flush_failure() {
    // A permanent (non-retryable) PUT fault abandons the flush immediately,
    // with no retry backoff to advance the injected clock through.
    let plan = FaultPlan::empty().with_rule(Rule::new(
        Op::Put,
        ScriptedFault::Permanent("fault: budget refund-on-error test".into()),
    ));
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(10_000));
    let router = budgeted_router(store, clock.clone(), budget.clone(), 1);

    let t = tenant("acme");
    let result = router
        .write(
            t.clone(),
            vec![make_point(&t, "m", &[], 1_000, 1.0)],
            WriteMode::Strict,
            Duration::from_secs(30),
        )
        .await;
    assert!(
        matches!(result, Err(WriteError::Abandoned(_))),
        "a permanent PUT fault abandons the flush, got {result:?}"
    );
    assert!(
        fault_store.fault_count(Op::Put, FaultKind::Permanent) >= 1,
        "the injected permanent PUT fault fired at least once"
    );
    assert_eq!(
        budget.in_flight_bytes(),
        0,
        "an abandoned flush refunds its charge: gauge back to baseline"
    );
}
