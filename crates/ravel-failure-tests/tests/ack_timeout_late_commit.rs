//! Traceability row `AckTimeout` (crates/ravel-ingest/src/error.rs,
//! `AckTimeout`). Issue #1130: the ack round times out and returns no token,
//! but the flush that was already in flight is not aborted. If it lands
//! after the deadline, docs/consistency-model.md's crash-matrix row "Ack
//! round times out" applies: "the data is durable and query-visible but its
//! token is unobservable to that client."
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, expect_vector, make_point, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteError, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_query::{EngineConfig, QueryEngine};
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
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// An ack round that times out returns `AckTimeout` with no recovered
/// tokens, but the shard's already in-flight flush is not aborted by the
/// timeout: once released it still commits, and the record it wrote is
/// durable and query-visible even though the client that timed out has no
/// token that names it.
#[tokio::test]
async fn ack_timeout_then_late_commit_is_durable_and_unobservable() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    // Hold the data-object PUT so the flush cannot complete before the ack
    // deadline elapses.
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let event_ts = BASE_NS - NS_PER_MIN;
    let points = vec![make_point(
        &tid,
        "requests_total",
        &[("route", "checkout")],
        event_ts,
        42.0,
    )];

    let err = router
        .write(
            tid.clone(),
            points,
            WriteMode::Strict,
            Duration::from_millis(200),
        )
        .await
        .expect_err("the held flush cannot ack before the deadline");
    assert!(
        matches!(err, WriteError::AckTimeout),
        "a whole-round timeout is AckTimeout, got {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "an ack timeout never carries a recovered token: the client has no \
         token for this write, even after it lands"
    );

    // Release the held PUT: the shard actor's flush, already in flight, is
    // not aborted by the caller's timeout and completes in the background.
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_held(1))
        .await
        .expect("the background flush must still hold the data-object PUT");
    for id in gate.held() {
        assert!(gate.release(id), "held call {id} released");
    }
    router.shutdown().await;

    // The late commit is durable and query-visible, exactly as
    // docs/consistency-model.md's crash-matrix row describes, even though no
    // token was ever returned to name it.
    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let result = expect_vector(
        engine
            .instant(
                tid.hash(),
                "requests_total",
                event_ts / 1_000_000,
                &[],
                clock.now(),
                Duration::from_secs(5),
            )
            .await
            .expect("query"),
    );
    assert_eq!(
        result.len(),
        1,
        "the late commit is durable and query-visible despite the timeout"
    );
    assert_eq!(result[0].value, 42.0);
}
