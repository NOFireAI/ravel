//! Retry idempotency and orphan-avoidance under injected object-store
//! faults (docs/ingest.md "Put retry", ADR-0002 "two-object commit").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, commit_and_trailer_versions, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, SEGMENT_FORMAT_VERSION, WriteMode};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 4,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

#[tokio::test]
async fn retried_data_put_is_idempotent_and_commits_exactly_once() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/l0/")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("retried data put still succeeds and acks");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects: Vec<_> = objects.iter().filter(|o| o.key.contains("/l0/")).collect();
    let commit_objects: Vec<_> = objects.iter().filter(|o| o.key.contains("/c/")).collect();
    assert_eq!(
        data_objects.len(),
        1,
        "duplicate delivery must not duplicate the data object"
    );
    assert_eq!(commit_objects.len(), 1, "exactly one commit record");

    let snapshot = router.metrics().snapshot();
    assert!(
        snapshot.put_retries >= 1,
        "the duplicate-delivery fault must count as a retry"
    );

    router.shutdown().await;
}

/// Mirror of `retried_data_put_is_idempotent_and_commits_exactly_once` with
/// `segment_format_version` set to v2: the retry/idempotency path in
/// `put_data_object_with_retry` and `publish_with_retry` operates on the
/// already-serialized, pinned segment bytes and never branches on RSEG
/// version, so a duplicate-delivery fault on the data PUT must still
/// resolve to exactly one data object and one commit record, and that
/// object's trailer must still genuinely be v5.
#[tokio::test]
async fn retried_data_put_is_idempotent_and_commits_exactly_once_with_v5() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/l0/")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("retried data put still succeeds and acks");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects: Vec<_> = objects.iter().filter(|o| o.key.contains("/l0/")).collect();
    let commit_objects: Vec<_> = objects.iter().filter(|o| o.key.contains("/c/")).collect();
    assert_eq!(
        data_objects.len(),
        1,
        "duplicate delivery must not duplicate the data object"
    );
    assert_eq!(commit_objects.len(), 1, "exactly one commit record");

    let snapshot = router.metrics().snapshot();
    assert!(
        snapshot.put_retries >= 1,
        "the duplicate-delivery fault must count as a retry"
    );

    let (record_version, trailer_version) = commit_and_trailer_versions(
        store.as_ref(),
        &tenant.hash(),
        Signal::Metrics,
        &receipt.tokens[0],
    )
    .await;
    assert_eq!(record_version, u32::from(SEGMENT_FORMAT_VERSION));
    assert_eq!(
        trailer_version, SEGMENT_FORMAT_VERSION,
        "the retried-and-deduplicated object must still be a genuine v5 object"
    );

    router.shutdown().await;
}

#[tokio::test]
async fn permanent_fault_before_data_put_leaves_no_visible_commit() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Permanent("kill".into())).with_key_contains("/l0/"),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let err = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a data object that never lands must error the waiter");
    assert!(
        err.is_retryable(),
        "abandoned flushes are retryable at a higher level: {err}"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "no data object and no commit record must ever appear"
    );

    let snapshot = router.metrics().snapshot();
    // A permanent data-object PUT fault exhausts the retry budget, so this is
    // a durability abandonment, not an input rejection (a8-F05 split).
    assert_eq!(snapshot.abandoned_retry_exhausted, 1);
    assert_eq!(snapshot.abandoned_input_rejected, 0);

    router.shutdown().await;
}

#[tokio::test]
async fn permanent_fault_between_puts_leaves_data_object_orphaned() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Permanent("kill".into())).with_key_contains("/c/"),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let err = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a commit record that never lands must error the waiter");
    assert!(err.is_retryable());

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|o| o.key.contains("/l0/")),
        "data object must have landed (orphan)"
    );
    assert!(
        !objects.iter().any(|o| o.key.contains("/c/")),
        "commit record must never land"
    );

    router.shutdown().await;
}
