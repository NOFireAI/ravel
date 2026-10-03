//! Crash-matrix rows 1 and 2 (docs/consistency-model.md "Crash matrix").
//!
//! Row 2's orphan-CREATION half (a killed commit PUT leaves a live data
//! object) exercises real production ingest. Its orphan-GC half still does
//! NOT: this crate has no dependency on `ravel-maintain` (see its
//! `Cargo.toml`), so those assertions drive `common::spec_model_sweep_orphans`,
//! an executable restatement of the GC rule from docs/consistency-model.md
//! "Deletion and GC" and ADR-0010 SS11, kept deliberately: it cross-checks the
//! GC rule against the same fault-injected store this file already builds for
//! the orphan-creation half, without pulling the sweeper crate into a suite
//! scoped to ingest crash safety. A green GC-half run here is evidence about
//! the spec model, NOT about the shipped GC path.
//!
//! Production orphan GC now exists (`crates/ravel-maintain/src/sweep.rs`,
//! ADR-0048 decisions 4-5: record-less L0 deletion re-verified by a batched
//! fresh LIST, gated by the mass-orphan circuit breaker). Its own crash-matrix
//! coverage lives in `crates/ravel-maintain/tests/sweep_crash_matrix.rs`
//! (e.g. row 8's convergence case and `orphan_gc_respects_live_records_and_age_gate`),
//! which exercises the real `sweep_orphans` fn against a `FaultStore`.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{GcWindow, TestClock, make_point, spec_model_sweep_orphans, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
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

    // Prove the documented crash point was actually reached: the data PUT
    // was attempted and killed. Without this, an ingest error raised before
    // the PUT (early admission/flush failure) would satisfy every other
    // assertion below while never exercising the crash path this row names.
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
/// the data object is an orphan. The orphan-creation half is real production
/// coverage. The GC half asserts the intended sweep behavior (no delete
/// before `grace + max_flush_lifetime`, re-verify commit absence, then
/// delete) against `spec_model_sweep_orphans`, a SPECIFICATION MODEL, not the
/// shipped GC path. The test name carries `spec_model_gc`
/// so a green run is never read as production-GC coverage. When the real GC
/// lands, retarget these assertions at it.
#[tokio::test]
async fn crash_after_data_put_before_commit_orphans_then_spec_model_gc_sweeps_after_grace() {
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

    // Prove the crash point was reached: the data PUT landed, then the
    // commit PUT was attempted and killed. Without this, an error raised
    // before the commit PUT would satisfy the orphan/visibility assertions
    // below while never exercising the after-data/before-commit crash site.
    assert_eq!(
        fault_store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the commit-PUT fault must have fired exactly once at the /c/ site"
    );

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

    // Everything below drives `spec_model_sweep_orphans`, NOT production
    // code: it checks the GC rule against an executable restatement of that
    // same rule, so it validates the model and cannot catch a real GC that
    // is built with different timing or a missing re-verify.
    let window = GcWindow::default();
    let tenant_hash = tid.hash();

    // Not yet within the grace window: nothing to delete.
    let boundary_ms = window.grace_ms + window.max_flush_lifetime_ms;
    fault_store.inner().set_clock_ms(boundary_ms as u64);
    let deleted = spec_model_sweep_orphans(
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
    let deleted = spec_model_sweep_orphans(
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
