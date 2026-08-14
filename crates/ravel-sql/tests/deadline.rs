//! Wall-deadline behavior ("Session config,
//! memory pool, budgets").
//!
//! On expiry the plan's streams are dropped, which releases every
//! `MemoryReservation`; the pool forwards each drop-time shrink to the tenant
//! accountant, so a timed-out query leaks no tenant budget.
//! Partial state is discarded, never returned.
//!
//! The stall comes from [`util::StallingStore`], not from a timing race:
//! `FaultStore` has no slow-response fault, and a deadline test whose outcome
//! depends on whether an in-memory fetch beats a timer is a flake generator.
//! Here the segment read genuinely never completes, so the only possible
//! outcome is the deadline firing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{ErrorClass, SqlConfig, SqlError};
use util::{Fixture, SegSpec, SeriesSpec, StallingStore, full_window, tenant_id};

const NOW_NS: i64 = 4 * util::NS_PER_HOUR;

fn specs() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("m", vec![(100, 1.0), (200, 2.0)])],
    )]
}

/// Publish onto a plain store first, then wrap it so the segment reads stall.
/// Wrapping before publish would hang the fixture itself.
async fn stalled_fixture() -> Fixture {
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = tenant_id("acme");
    let seg_specs = specs();

    // Publish through the plain store...
    let plain = Fixture::build(
        Arc::clone(&inner),
        &[(&tenant, &seg_specs)],
        SqlConfig::default(),
        1 << 30,
    )
    .await;
    drop(plain);

    // ...then build the executor over the stalling wrapper, with nothing
    // published through it.
    let stalling: Arc<dyn ObjectStoreBackend> =
        StallingStore::new(inner) as Arc<dyn ObjectStoreBackend>;
    Fixture::build(stalling, &[], SqlConfig::default(), 1 << 30).await
}

fn request_with_deadline(sql: &str, deadline: Duration) -> ravel_sql::SqlRequest {
    ravel_sql::SqlRequest {
        sql: sql.to_string(),
        window: full_window(),
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline,
    }
}

#[tokio::test]
async fn a_stalled_segment_read_trips_the_wall_deadline() {
    let fixture = stalled_fixture().await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request_with_deadline("SELECT ts, value FROM samples", Duration::from_millis(50)),
        )
        .await
        .expect_err("the deadline must fire");

    assert_eq!(err.class(), ErrorClass::Timeout);
    match err {
        SqlError::DeadlineExceeded { millis } => assert_eq!(millis, 50),
        other => panic!("expected DeadlineExceeded, got {other}"),
    }
}

/// A timed-out query leaves the tenant accountant at zero.
///
/// Scoped honestly: this segment stalls inside the fetch, before the scan
/// grows its reservation, so what is proven here is that the abort path
/// itself adds nothing and releases the empty reservation the stream
/// registered. The load-bearing cancellation test -- a *mid-scan* stream
/// dropped after real bytes were reserved -- lives in B2's
/// tests/pushdown_memory.rs and in tests/memory_accounting.rs.
#[tokio::test]
async fn a_timed_out_query_leaks_no_tenant_budget() {
    let fixture = stalled_fixture().await;
    let tenant = tenant_id("acme");
    let budget = fixture.executor.tenant_budget(tenant.hash());

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request_with_deadline("SELECT ts, value FROM samples", Duration::from_millis(50)),
        )
        .await
        .expect_err("the deadline must fire");
    assert!(matches!(err, SqlError::DeadlineExceeded { .. }));

    assert_eq!(
        budget.reserved(),
        0,
        "a timed-out query must return every reserved byte"
    );
}

/// The deadline message carries only a duration, so it is safe to show a
/// client unredacted, and it says nothing about the segment that stalled.
#[tokio::test]
async fn the_deadline_message_carries_no_storage_detail() {
    let fixture = stalled_fixture().await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request_with_deadline("SELECT ts, value FROM samples", Duration::from_millis(50)),
        )
        .await
        .expect_err("the deadline must fire");

    let message = err.client_message();
    assert!(message.contains("50 ms"), "{message}");
    assert!(!message.contains(".rseg"), "{message}");
    assert!(!message.contains(&tenant.hash().to_hex()), "{message}");
}
