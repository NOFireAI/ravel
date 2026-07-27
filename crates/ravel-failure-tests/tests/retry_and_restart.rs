//! Retry-storm coverage across every op kind ingest and publish touch, and
//! restart-from-empty-local-state (docs/consistency-model.md; ADR-0010
//! SS1/SS3: writer identity is a fresh UUIDv4 per process start, never
//! derived from any prior state).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
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
        put_retry_max_attempts: 6,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(10),
        ..IngestConfig::default()
    }
}

/// One retry-storm scenario: a single retryable fault kind fires once (via
/// `Occurrence::Nth(1)`) on a single PUT target (the data object or the
/// commit record), then gets out of the way. `FaultStore::resolve` lets the
/// *first* rule matching an (op, key) pair fully govern every call to it, so
/// stacking several rules on the same key is a no-op past the first; the
/// matrix of fault kinds x PUT targets is exercised as independent
/// scenarios instead, each with its own store.
async fn assert_retry_recovers(fault: ScriptedFault, key_target: &str) {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, fault.clone())
            .with_key_contains(key_target)
            .with_occurrence(Occurrence::Nth(1)),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let event_ts = BASE_NS - NS_PER_MIN;
    let points = vec![make_point(&tid, "retried_metric", &[], event_ts, 3.0)];
    let receipt = router
        .write(
            tid.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .unwrap_or_else(|e| panic!("write must survive {fault:?} on {key_target}: {e}"));
    assert_eq!(receipt.tokens.len(), 1);

    assert_eq!(
        fault_store.fault_count(Op::Put, fault.kind()),
        1,
        "the fault must actually have fired exactly once for {fault:?} on {key_target}"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_objects = objects.iter().filter(|o| o.key.contains("/c/")).count();
    assert_eq!(
        data_objects, 1,
        "retries of the pinned flush identity must not create extra data objects ({fault:?} on {key_target})"
    );
    assert_eq!(
        commit_objects, 1,
        "retries must not create extra commit records ({fault:?} on {key_target})"
    );

    let snapshot = router.metrics().snapshot();
    assert!(
        snapshot.put_retries >= 1,
        "the injected fault must be observed as a retry ({fault:?} on {key_target})"
    );

    router.shutdown().await;

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let result = engine
        .instant(
            tid.hash(),
            "retried_metric",
            event_ts / 1_000_000,
            &[],
            clock.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("query");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 3.0);
}

/// Every retryable fault kind (`Timeout`, `Throttled`, `Transient`), on both
/// the data PUT and the commit PUT retry paths in `ravel-ingest`
/// (`IngestConfig::put_retry_max_attempts` governs both), independently
/// recovers to a correct, exactly-once-committed final state.
#[tokio::test]
async fn retry_storm_on_every_put_fault_kind_still_commits_exactly_once() {
    let faults = [
        ScriptedFault::Timeout,
        ScriptedFault::Throttled { retry_after_ms: 1 },
        ScriptedFault::Transient("blip".into()),
    ];
    let targets = ["/l0/", "/c/"];
    for fault in &faults {
        for target in &targets {
            assert_retry_recovers(fault.clone(), target).await;
        }
    }
}

/// A router (writer identity A) ingests, then is dropped entirely --
/// modeling a process crash and restart with no surviving local state. A
/// brand new router (fresh `IngestRouter::new` call, hence a fresh internal
/// writer UUID per ADR-0010 SS3), catalog, and query engine are built over
/// the SAME object store and ingest more data. Queries must see both
/// generations, and a `min_commit_token` minted by the new writer (B) must
/// resolve, all without router B ever touching any state from router A.
#[tokio::test]
async fn restart_from_empty_local_state_sees_both_writer_generations() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock_a = TestClock::new(BASE_NS);
    let tid = tenant("acme");
    let ts_a = BASE_NS - 4 * NS_PER_MIN;
    let ts_b = BASE_NS - 2 * NS_PER_MIN;

    {
        let router_a = IngestRouter::new(
            config(),
            Arc::clone(&store),
            Signal::Metrics,
            clock_a.clone(),
        );
        router_a
            .write(
                tid.clone(),
                vec![make_point(
                    &tid,
                    "restart_metric",
                    &[("gen", "a")],
                    ts_a,
                    1.0,
                )],
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("router A writes and commits");
        router_a.shutdown().await;
        // router_a is fully dropped here: no reference to it, its writer
        // UUID, or any in-memory state survives past this block.
    }

    let clock_b = TestClock::new(BASE_NS);
    let router_b = IngestRouter::new(
        config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock_b.clone(),
    );
    let receipt_b = router_b
        .write(
            tid.clone(),
            vec![make_point(
                &tid,
                "restart_metric",
                &[("gen", "b")],
                ts_b,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("router B (fresh writer identity) writes and commits");
    router_b.shutdown().await;

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let writer_ids: std::collections::HashSet<_> = objects
        .iter()
        .filter_map(|o| ravel_commit::keys::parse_data_key(&o.key).ok())
        .map(|p| p.writer_id)
        .collect();
    assert_eq!(
        writer_ids.len(),
        2,
        "router A and router B must have used two distinct writer identities"
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let engine = QueryEngine::new(
        Arc::new(catalog),
        Arc::clone(&store),
        EngineConfig::default(),
    );

    let result_a = engine
        .instant(
            tid.hash(),
            "restart_metric{gen=\"a\"}",
            ts_a / 1_000_000,
            &[],
            clock_b.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("query for generation A's data");
    assert_eq!(
        result_a.len(),
        1,
        "generation A's data must still be queryable after A is gone"
    );
    assert_eq!(result_a[0].value, 1.0);

    let result_b = engine
        .instant(
            tid.hash(),
            "restart_metric{gen=\"b\"}",
            ts_b / 1_000_000,
            &[],
            clock_b.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("query for generation B's data");
    assert_eq!(result_b.len(), 1);
    assert_eq!(result_b[0].value, 2.0);

    // Read-your-write: a token minted by the new writer B resolves cleanly
    // through the fresh catalog, with no dependency on any state from A.
    let result_token = engine
        .instant(
            tid.hash(),
            "restart_metric{gen=\"b\"}",
            ts_b / 1_000_000,
            &receipt_b.tokens,
            clock_b.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("min_commit_token from the new writer must resolve");
    assert_eq!(result_token.len(), 1);
    assert_eq!(result_token[0].value, 2.0);
}
