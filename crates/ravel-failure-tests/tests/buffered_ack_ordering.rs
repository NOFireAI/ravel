//! Traceability row `BufferedAck` (crates/ravel-ingest/src/router.rs,
//! `write_points`). docs/consistency-model.md: buffered mode is "Acknowledged
//! after admission and enqueue to a shard actor ... Never described as
//! durable. No commit token is returned." This pins that ordering with a real
//! concurrency mechanism (a `FaultStore` hold gate on every PUT) rather than a
//! timing assumption: the ack must return while every PUT is still blocked
//! closed, not merely before one we happened to check late.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// A buffered ack returns before any data or commit object reaches the
/// store, even when every PUT the store will ever see is held gated closed
/// at the time of the call.
#[tokio::test]
async fn buffered_ack_returns_before_any_durable_object() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let gate = fault_store.hold(Op::Put, None, Occurrence::Always);
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let points = vec![make_point(
        &tid,
        "cpu_usage",
        &[("host", "a")],
        BASE_NS - NS_PER_MIN,
        1.0,
    )];

    // Every PUT is gated closed for the whole store. A buffered ack is
    // defined to precede any durable write, not merely a slow one: it must
    // return promptly regardless.
    let receipt = tokio::time::timeout(
        Duration::from_secs(5),
        router.write(
            tid.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        ),
    )
    .await
    .expect("buffered ack must return while every PUT is still gated closed")
    .expect("buffered write is Ok");
    assert!(
        receipt.tokens.is_empty(),
        "buffered mode returns no commit token"
    );

    // Nothing has reached the store yet: the ack was observed strictly
    // before any durable object, not merely before one we happened to check
    // late.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "no data or commit object may exist yet, found {objects:?}"
    );

    // Confirm the write is not vacuous: it does reach the store eventually,
    // once its PUTs are unblocked.
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_held(1))
        .await
        .expect("the buffered write must still flush to the store eventually");

    // Release every PUT as it becomes held, until the data object lands.
    for _ in 0..8 {
        for id in gate.held() {
            gate.release(id);
        }
        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        if objects.iter().any(|o| o.key.contains("/l0/")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    router.shutdown().await;
    let objects_after = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects_after.iter().any(|o| o.key.contains("/l0/")),
        "the buffered write eventually reaches a durable data object"
    );
}
