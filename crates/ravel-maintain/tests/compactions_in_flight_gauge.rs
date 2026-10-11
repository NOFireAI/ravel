//! `ravel_maintain_compactions_in_flight` (issue #2666): the gauge reads 1
//! while a bucket merge is in progress and returns to 0 on every way a merge
//! ends: a publish, a failed input read, and the merge future being dropped
//! mid-flight.
//!
//! The gauge is process-wide, so the tests in this binary run one at a time
//! under [`SERIAL`] and each asserts absolute values. A merge is parked
//! in-flight with a `FaultStore` hold gate on its first L0 data GET, which is
//! the first read after the gauge rises; no test sleeps.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;

use common::{InputSpec, bucket, raw_series, sealed_now_ns, seed_input};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MaintainError, PublishOutcome, compact_bucket,
    compactions_in_flight,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// L0 data object keys; commit records and the bucket listing never match.
const L0_KEYS: &str = "/l0/";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn seeded(plan: FaultPlan) -> FaultStore<MemoryStore> {
    let store = FaultStore::new(MemoryStore::new(), plan);
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(1_000, 1.0), (2_000, 2.0)],
            )],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(store.inner(), &spec).await;
    }
    store
}

/// Wait until `gate` parks exactly one L0 data GET and return its id.
async fn parked(gate: &GateHandle) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, op, key) = &held[0];
    assert_eq!(*op, Op::Get);
    assert!(
        key.contains(L0_KEYS),
        "the parked call is an L0 read: {key}"
    );
    *id
}

/// Poll `merge` until `gate` parks its first input read, failing if the merge
/// finishes first.
async fn park<F: Future + Unpin>(merge: &mut F, gate: &GateHandle) -> u64 {
    tokio::select! {
        _ = merge => panic!("the merge finished without reaching its first input read"),
        id = parked(gate) => id,
    }
}

#[tokio::test]
async fn gauge_reads_one_while_parked_and_zero_after_publish() {
    let _serial = SERIAL.lock().await;
    let store = seeded(FaultPlan::empty()).await;
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    assert_eq!(compactions_in_flight(), 0, "no merge before the run");

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park(&mut merge, &gate).await;
    assert_eq!(compactions_in_flight(), 1, "the parked merge is in flight");
    assert!(gate.release(id), "the parked GET was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the merge published: {outcome:?}"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a published merge is not in flight"
    );
}

#[tokio::test]
async fn gauge_returns_to_zero_after_an_input_read_fails() {
    let _serial = SERIAL.lock().await;
    let store = seeded(
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("injected".to_string()))
                .with_key_contains(L0_KEYS)
                .with_occurrence(Occurrence::Always),
        ),
    )
    .await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();

    let result = compact_bucket(&store, &clock, &config, &bucket()).await;

    assert!(
        matches!(result, Err(MaintainError::Store(_))),
        "the injected input read error fails the merge: {result:?}"
    );
    assert!(
        store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
        "the input read fault fired"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a failed merge is not in flight"
    );
}

#[tokio::test]
async fn gauge_returns_to_zero_when_the_merge_future_is_dropped() {
    let _serial = SERIAL.lock().await;
    let store = seeded(FaultPlan::empty()).await;
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    park(&mut merge, &gate).await;
    assert_eq!(compactions_in_flight(), 1, "the parked merge is in flight");
    drop(merge);

    assert_eq!(
        compactions_in_flight(),
        0,
        "a dropped merge is not in flight"
    );
}
