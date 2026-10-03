//! The live accounting handle a caller hands to
//! [`SqlExecutor::execute_accounted`] must equal the query's own final
//! accounting on the success path, so the same figures the server records on a
//! timeout or a dropped future (read from that handle) are the query's real
//! cost and not an independent, drifting count.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use ravel_sql::LiveAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// On a successful query the live handle's snapshot equals
/// `SqlOutcome::accounting` field-for-field: the handle the guard reads on the
/// non-success exits is the very same counter block the success path reports.
#[tokio::test]
async fn success_snapshot_equals_the_live_handle_at_completion() {
    let tenant = tenant_id("acme");
    let fixture = Fixture::memory(&[(
        &tenant,
        &[SegSpec::new(
            10,
            1,
            1,
            vec![SeriesSpec::new("m", vec![(100, 1.0), (200, 2.5)])],
        )],
    )])
    .await;

    let live = LiveAccounting::new();
    let outcome = fixture
        .executor
        .execute_accounted(
            tenant.hash(),
            &request("SELECT ts, value FROM samples ORDER BY ts"),
            &live,
        )
        .await
        .expect("query succeeds");

    // Meaningful only if the query actually touched the store.
    assert!(
        outcome.accounting.total_s3_requests() >= 1,
        "the query must have issued at least one store request"
    );
    assert_eq!(
        live.snapshot(),
        outcome.accounting,
        "the guard's live snapshot equals the success snapshot field-for-field"
    );
}
