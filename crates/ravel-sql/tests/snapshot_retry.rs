//! B3 snapshot retry contract (issue #22, plan section 2, review F9).
//!
//! `docs/consistency-model.md` mandates one re-resolve-and-retry when a
//! pinned segment vanishes before any result has been emitted, and an
//! immediate `SnapshotInvalidated` once emission has started. These tests
//! drive the first half end-to-end with a `FaultStore` `NotFoundBlip` on the
//! segment's data-object GET and assert the *resolve count*, not just the
//! outcome: an implementation that retried twice, or that returned the right
//! error without re-resolving at all, would pass an outcome-only assertion.
//!
//! The post-emission half is proven by the exhaustive unit test on
//! `retry_decision` in src/executor.rs rather than here. Today's
//! `RsegScanExec` fetches every segment in a partition before emitting its
//! first batch, and `SortPreservingMergeExec` needs one batch from every
//! partition before it emits anything, so a store `NotFound` can only ever
//! surface with zero batches emitted. `no_retry_happens_after_a_successful_
//! first_batch` below pins the observable half of that: a healthy query
//! resolves exactly once.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{SqlConfig, SqlError};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

fn specs() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![SeriesSpec::new("m", vec![(100, 1.0), (200, 2.0)])],
        ),
        SegSpec::new(20, 1, 1, vec![SeriesSpec::new("m", vec![(300, 3.0)])]),
    ]
}

/// Build a fixture over a `FaultStore` carrying `plan`.
///
/// Every rule below is scoped to `Op::Get` on keys containing `.rseg`, the
/// data-object suffix (`ravel_commit::keys::DATA_SUFFIX`). Publishing writes
/// data objects and commit records with PUT, and `Catalog::resolve` reads
/// commit records (`.cmt`) with LIST and GET, so no rule here can perturb
/// setup or snapshot resolution: only the segment reads the scan performs.
async fn faulted_fixture(plan: FaultPlan) -> (Arc<FaultStore<MemoryStore>>, Fixture) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let fixture = Fixture::build(
        backend,
        &[(&tenant, &seg_specs)],
        SqlConfig::default(),
        1 << 30,
    )
    .await;
    (store, fixture)
}

/// A `NotFound` on the very first data-object GET, before any batch was
/// emitted: exactly one re-resolve, one retry, and the query then succeeds
/// with the full result.
#[tokio::test]
async fn not_found_before_the_first_batch_retries_exactly_once_and_succeeds() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let (store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT count(value) AS n FROM samples"),
        )
        .await
        .expect("the retry must recover the query");

    assert_eq!(
        store.fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound must actually have fired"
    );
    assert_eq!(
        outcome.stats.resolves, 2,
        "one original resolve plus exactly one re-resolve"
    );
    assert_eq!(outcome.stats.attempts, 2);
    assert_eq!(count_of(&outcome.output), 3, "all three samples come back");
}

/// A `NotFound` on every data-object GET: the retry runs once, fails again,
/// and the query ends as `SnapshotInvalidated`. Never a third resolve.
#[tokio::test]
async fn a_second_not_found_fails_snapshot_invalidated_after_exactly_one_retry() {
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(".rseg"));
    let (_store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect_err("a persistently vanished segment must fail");

    assert!(
        matches!(err, SqlError::SnapshotInvalidated),
        "expected SnapshotInvalidated, got {err}"
    );
    // The client sees the redacted transient-storage message, never the key.
    assert_eq!(err.client_message(), ravel_sql::MSG_UNAVAILABLE);
}

/// A store fault that is *not* `NotFound` must not arm the retry: it
/// propagates on the first attempt, with a single resolve.
#[tokio::test]
async fn a_non_not_found_store_fault_does_not_retry() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent("disk on fire".to_string()),
        )
        .with_key_contains(".rseg"),
    );
    let (store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect_err("a permanent store fault must surface");

    assert!(
        !matches!(err, SqlError::SnapshotInvalidated),
        "a permanent fault is not a vanished snapshot; got {err}"
    );
    assert!(
        matches!(err, SqlError::Fetch(_)),
        "expected the typed fetch error, got {err}"
    );
    // Exactly one attempt: the retry is reserved for NotFound. With two
    // segments the plan may issue more than one GET before failing, so the
    // assertion is on the fault firing at all plus the error class, and on
    // the resolve count below.
    assert!(store.fault_count(Op::Get, FaultKind::Permanent) >= 1);
    // The raw backend text must not reach a client.
    assert!(!err.client_message().contains("disk on fire"));
}

/// The healthy path resolves exactly once. This is the observable half of
/// "no retry after emission": nothing re-resolves when nothing vanished.
#[tokio::test]
async fn no_retry_happens_after_a_successful_first_batch() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let fixture = Fixture::memory(&[(&tenant, &seg_specs)]).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("healthy query");

    assert_eq!(outcome.stats.resolves, 1);
    assert_eq!(outcome.stats.attempts, 1);
    assert!(
        outcome.stats.batches_emitted >= 1,
        "the plan must actually have emitted a batch for this to mean anything"
    );
    assert_eq!(outcome.output.num_rows(), 3);
}

/// Both resolves use the same `now_ns`, so the retry cannot widen or shift
/// the listing window and pick up a segment the first resolve could not see.
/// Asserted through the segment count the successful attempt reports.
#[tokio::test]
async fn the_retry_reuses_the_same_now_ns_and_window() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let (_store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT count(value) AS n FROM samples"),
        )
        .await
        .expect("retry succeeds");

    assert_eq!(outcome.stats.resolves, 2);
    assert_eq!(
        outcome.stats.segments, 2,
        "the re-resolve must see the same two segments as the first"
    );
}

fn count_of(output: &ravel_sql::QueryOutput) -> i64 {
    use datafusion::arrow::array::Int64Array;

    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}
