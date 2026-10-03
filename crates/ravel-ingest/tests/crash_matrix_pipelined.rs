//! Crash-matrix rows 1 and 2 (docs/consistency-model.md "Crash matrix"),
//! re-run with `max_inflight_flushes: 3` (ADR-0067 decision 2) instead of the
//! default of 1, plus a scenario the default depth cannot exercise at all: a
//! killed flush and a healthy sibling flush genuinely in flight on the same
//! shard at once.
//!
//! `crates/ravel-failure-tests/tests/crash_matrix.rs` owns the canonical,
//! depth-1 versions of rows 1 and 2 (including the orphan-GC spec-model
//! half, which is orthogonal to this crate's pipelining knob and not
//! duplicated here). These are ravel-ingest-scoped companions: the same two
//! rows, confirming the fixed durability contract survives raising the
//! semaphore bound, and one genuinely new row pipelining introduces.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{make_point, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TimeRange};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 1,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        max_inflight_flushes: 3,
        ..IngestConfig::default()
    }
}

/// Row 1 at depth 3: killed before the data PUT lands. Raising the in-flight
/// bound changes nothing about a single tenant's single flush, but this
/// pins down that the flip does not, say, let a later retry attempt bypass
/// the fault or leave a half-pinned identity behind.
#[tokio::test]
async fn crash_before_data_put_leaves_nothing_stored_or_visible_at_depth_three() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("kill before data put".into()),
        )
        .with_key_contains("/l0/"),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = common::TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let points = vec![make_point(&tid, "cpu_usage", &[("host", "a")], 1_000, 1.0)];
    let err = router
        .write(
            tid.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a data object that never lands must error the writer");
    assert!(err.is_retryable(), "client must be told to retry: {err}");
    assert_eq!(
        fault_store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the data-PUT fault must have fired exactly once at the /l0/ site"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "no data object and no commit record must ever appear, got {objects:?}"
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &tid.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: clock.now() - NS_PER_HOUR,
                end_ns: clock.now(),
            },
            &[],
            clock.now(),
        )
        .await
        .expect("resolve");
    assert!(
        snapshot.segments.is_empty(),
        "nothing must be visible to Catalog::resolve"
    );

    router.shutdown().await;
}

/// Row 2 at depth 3 (orphan-creation half only; the spec-model GC half in
/// `ravel-failure-tests` is not depth-sensitive and is not duplicated here):
/// data PUT lands, commit PUT is permanently killed, so the data object is an
/// orphan and nothing is visible.
#[tokio::test]
async fn crash_after_data_put_before_commit_orphans_at_depth_three() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("kill before commit put".into()),
        )
        .with_key_contains("/c/"),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = common::TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let points = vec![make_point(&tid, "cpu_usage", &[("host", "a")], 1_000, 1.0)];
    let err = router
        .write(
            tid.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a commit record that never lands must error the writer");
    assert!(err.is_retryable());
    assert_eq!(
        fault_store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the commit-PUT fault must have fired exactly once at the /c/ site"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|o| o.key.contains("/l0/")),
        "data object must have landed as an orphan"
    );
    assert!(
        !objects.iter().any(|o| o.key.contains("/c/")),
        "commit record must never land"
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &tid.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: clock.now() - NS_PER_HOUR,
                end_ns: clock.now(),
            },
            &[],
            clock.now(),
        )
        .await
        .expect("resolve");
    assert!(
        snapshot.segments.is_empty(),
        "orphan must not be visible to Catalog::resolve"
    );

    router.shutdown().await;
}

/// A row pipelining introduces that depth 1 cannot: a killed flush and a
/// healthy sibling flush for a different tenant, genuinely in flight on the
/// same shard at once. The ownership-transfer design (each `PinnedFlush`
/// owns its own waiters, moved by value into its own spawned task) predicts
/// these cannot contaminate each other; this drives that prediction against
/// real concurrent execution instead of trusting the design argument alone.
#[tokio::test]
async fn concurrent_flush_crash_is_isolated_from_a_healthy_sibling_at_depth_three() {
    let victim = tenant("acme");
    let healthy = tenant("globex");
    let victim_hash = victim.hash().to_hex();

    // Let the victim's own data PUT (1st match against its hash) land, then
    // kill its commit PUT (2nd match): the same after-data/before-commit
    // crash site as row 2, but now racing a sibling flush.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("kill victim's commit put".into()),
        )
        .with_key_contains(victim_hash)
        .with_occurrence(Occurrence::Nth(2)),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = common::TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let victim_points = vec![make_point(
        &victim,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    let healthy_points = vec![make_point(
        &healthy,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let (victim_result, healthy_result) = tokio::join!(
        router.write(
            victim.clone(),
            victim_points,
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
        router.write(
            healthy.clone(),
            healthy_points,
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
    );

    let err = victim_result.expect_err("the victim's killed commit put must error its writer");
    assert!(err.is_retryable());
    let healthy_receipt =
        healthy_result.expect("a concurrently in-flight sibling flush must be unaffected");
    assert_eq!(healthy_receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().filter(|o| o.key.contains("/l0/")).count() >= 2,
        "both the orphaned victim data object and the healthy one must land, got {objects:?}"
    );
    assert_eq!(
        objects.iter().filter(|o| o.key.contains("/c/")).count(),
        1,
        "exactly the healthy sibling's commit record must land"
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let victim_snapshot = catalog
        .resolve(
            &victim.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: clock.now() - NS_PER_HOUR,
                end_ns: clock.now(),
            },
            &[],
            clock.now(),
        )
        .await
        .expect("resolve");
    assert!(
        victim_snapshot.segments.is_empty(),
        "the victim's orphan must not be visible"
    );
    let healthy_snapshot = catalog
        .resolve(
            &healthy.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: clock.now() - NS_PER_HOUR,
                end_ns: clock.now(),
            },
            &healthy_receipt.tokens,
            clock.now(),
        )
        .await
        .expect("resolve");
    assert_eq!(
        healthy_snapshot.segments.len(),
        1,
        "the healthy sibling's segment must be visible via read-your-write"
    );

    router.shutdown().await;
}
