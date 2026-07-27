//! Crash-matrix rows 1 and 2 (docs/consistency-model.md "Crash matrix"),
//! plus the orphan GC sweep it prescribes (docs/catalog-and-mvcc.md "MVCC
//! rules"; ADR-0010 SS11).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{GcWindow, TestClock, make_point, sweep_orphans, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TimeRange};

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 1,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// Crash-matrix row 1: killed before the data PUT lands.
#[tokio::test]
async fn crash_before_data_put_leaves_nothing_stored_or_visible() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("kill before data put".into()),
        )
        .with_key_contains("/l0/"),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
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
                start_ns: clock.now() - common::NS_PER_HOUR,
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

/// Crash-matrix row 2: data PUT lands, commit PUT is permanently killed, so
/// the data object is an orphan. GC must not touch it before
/// `grace + max_flush_lifetime`, must re-verify commit absence, and only
/// then delete it.
#[tokio::test]
async fn crash_after_data_put_before_commit_orphans_then_gcs_after_grace() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("kill before commit put".into()),
        )
        .with_key_contains("/c/"),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(1_700_000_000_000_000_000);
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

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let orphan = objects
        .iter()
        .find(|o| o.key.contains("/l0/"))
        .expect("data object must have landed as an orphan")
        .clone();
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
                start_ns: clock.now() - common::NS_PER_HOUR,
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

    let window = GcWindow::default();
    let tenant_hash = tid.hash();

    // Not yet within the grace window: nothing to delete.
    let boundary_ms = window.grace_ms + window.max_flush_lifetime_ms;
    fault_store.inner().set_clock_ms(boundary_ms as u64);
    let deleted = sweep_orphans(
        fault_store.inner(),
        tenant_hash,
        Signal::Metrics,
        0,
        boundary_ms,
        window,
    )
    .await;
    assert!(
        deleted.is_empty(),
        "GC must not delete an orphan before grace + max_flush_lifetime elapses"
    );
    let still_there = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(still_there.iter().any(|o| o.key == orphan.key));

    // Past the grace window: the orphan is eligible, and commit-record
    // absence is re-verified immediately before delete.
    let now_ms = boundary_ms + 1;
    fault_store.inner().set_clock_ms(now_ms as u64);
    let deleted = sweep_orphans(
        fault_store.inner(),
        tenant_hash,
        Signal::Metrics,
        0,
        now_ms,
        window,
    )
    .await;
    assert_eq!(deleted, vec![orphan.key.clone()]);

    let after = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        after.is_empty(),
        "orphan must be gone after GC sweeps past the grace window"
    );
}
