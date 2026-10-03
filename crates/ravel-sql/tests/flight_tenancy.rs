//! Flight SQL tenancy isolation, ticket replay, and stream cancellation
//! (security invariant 2).
//!
//! flight.rs proves the individual rules at the unit level. This
//! file is the systematic tenancy isolation test: tenant A cannot read
//! tenant B through any Flight SQL method, including catalog/metadata
//! methods:
//!
//! - **Statement isolation.** Two tenants with disjoint data run the identical
//!   SQL; each sees only its own rows, and tenant A's result never contains a
//!   value only tenant B wrote. The pinned-snapshot ticket is checked against
//!   the redeeming tenant, so A's ticket in B's hands (and B's in A's) is
//!   denied before the snapshot is touched -- both directions.
//! - **Metadata isolation.** Every catalog/metadata method is answered from a
//!   compile-time constant, so its response is byte-identical for A and B. That
//!   is the strongest possible non-disclosure: there is no tenant-derived bit
//!   for one tenant to read another's schema, table set, or existence out of.
//!   The suite asserts the identity for all five methods on both RPCs.
//! - **Ticket replay after expiry** fails `SnapshotInvalidated`, never with
//!   data read under a pin the GC no longer protects.
//! - **Stream cancellation** mid-transfer returns the tenant's reserved bytes
//!   to zero through the delegating pool's drop-forwarding path,
//!   asserted against the shared `TenantMemoryAccountant`.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use arrow_flight::Ticket;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables,
};
use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use ravel_sql::MSG_UNAVAILABLE;
use ravel_types::TenantId;
use tonic::Request;
use util::flight_harness::{Harness, TOKEN_KEY, collect, insert, statement_ticket};
use util::gate::{Cell, Row, rows_from_batches};
use util::{SegSpec, SeriesSpec, tenant_id};

const A: &str = "acme";
const B: &str = "globex";

/// Two tenants whose data is disjoint in both metric name and value, so any
/// cross-tenant read shows up as a wrong row count or a foreign value.
fn tenants() -> (TenantId, Vec<SegSpec>, TenantId, Vec<SegSpec>) {
    let a = tenant_id(A);
    let a_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("cpu", vec![(100, 1.0), (200, 2.0)])],
    )];
    let b = tenant_id(B);
    let b_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "ram",
            vec![(100, 900.0), (200, 901.0), (300, 902.0)],
        )],
    )];
    (a, a_specs, b, b_specs)
}

async fn harness() -> Harness {
    let (a, a_specs, b, b_specs) = tenants();
    Harness::memory(&[(&a, &a_specs), (&b, &b_specs)]).await
}

/// One `Request<Ticket>` carrying `token` as the tenant credential.
fn ticketed(token: &str) -> Request<Ticket> {
    let mut request = Request::new(Ticket::new(Vec::new()));
    insert(request.metadata_mut(), TOKEN_KEY, token);
    request
}

// ---------------------------------------------------------------------------
// Statement isolation
// ---------------------------------------------------------------------------

/// Each tenant's aggregate is computed over its own rows only. If A could see
/// B's segment, its count and sum would change; they do not.
#[tokio::test]
async fn statement_aggregates_are_scoped_to_the_calling_tenant() {
    let harness = harness().await;
    let sql = "SELECT count(value), sum(value) FROM samples";

    let a_rows = flight_rows(&harness, A, sql).await;
    assert_eq!(
        a_rows,
        vec![vec![Cell::Int(2), Cell::float(3.0)]],
        "tenant A sees only its two samples summing to 3.0"
    );

    let b_rows = flight_rows(&harness, B, sql).await;
    assert_eq!(
        b_rows,
        vec![vec![Cell::Int(3), Cell::float(2703.0)]],
        "tenant B sees only its three samples summing to 2703.0"
    );
}

/// A projection as tenant A returns exactly A's values and none of B's. This
/// is the row-level counterpart to the aggregate test: a leak would surface as
/// one of B's distinctive values (900/901/902) appearing in A's result.
#[tokio::test]
async fn a_projection_never_returns_the_other_tenants_values() {
    let harness = harness().await;
    let sql = "SELECT value FROM samples ORDER BY series_id, ts";

    let a_rows = flight_rows(&harness, A, sql).await;
    assert_eq!(a_rows.len(), 2, "tenant A has exactly two samples");
    let forbidden = [Cell::float(900.0), Cell::float(901.0), Cell::float(902.0)];
    for row in &a_rows {
        for cell in row {
            assert!(
                !forbidden.contains(cell),
                "tenant A must never see one of tenant B's values: {cell:?}"
            );
        }
    }
    assert_eq!(a_rows, vec![vec![Cell::float(1.0)], vec![Cell::float(2.0)]]);
}

/// A ticket is bound to the tenant that minted it. Presenting A's ticket with
/// B's credentials, and B's with A's, is denied before the pinned snapshot is
/// touched -- both directions, so neither tenant can borrow the other's pin.
#[tokio::test]
async fn a_ticket_is_rejected_when_redeemed_by_the_other_tenant() {
    let harness = harness().await;
    let sql = "SELECT ts, value FROM samples ORDER BY series_id, ts";

    let a_ticket = harness
        .get_flight_info(A, sql)
        .await
        .expect("A flight info");
    let denied = harness
        .do_get(B, &a_ticket)
        .await
        .expect_err("A's ticket in B's hands is denied");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let b_ticket = harness
        .get_flight_info(B, sql)
        .await
        .expect("B flight info");
    let denied = harness
        .do_get(A, &b_ticket)
        .await
        .expect_err("B's ticket in A's hands is denied");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

// ---------------------------------------------------------------------------
// Metadata isolation: every method is tenant-independent
// ---------------------------------------------------------------------------

/// Every catalog/metadata `DoGet` returns the identical batch for A and for B.
/// A tenant-independent answer is one no tenant can read another's existence
/// out of. Asserted per method, on the `DoGet` that carries the actual data.
#[tokio::test]
async fn metadata_do_get_responses_are_identical_for_every_tenant() {
    let harness = harness().await;

    macro_rules! assert_identical {
        ($method:ident, $cmd:expr) => {{
            let for_a: Vec<RecordBatch> = collect(
                harness
                    .service
                    .$method($cmd, ticketed(A))
                    .await
                    .expect(concat!(stringify!($method), " for A"))
                    .into_inner(),
            )
            .await
            .expect("A decodes");
            let for_b: Vec<RecordBatch> = collect(
                harness
                    .service
                    .$method($cmd, ticketed(B))
                    .await
                    .expect(concat!(stringify!($method), " for B"))
                    .into_inner(),
            )
            .await
            .expect("B decodes");
            assert!(
                !for_a.is_empty(),
                concat!(stringify!($method), " returns rows")
            );
            assert_eq!(
                for_a, for_b,
                concat!(stringify!($method), " must not depend on the tenant"),
            );
        }};
    }

    assert_identical!(do_get_catalogs, CommandGetCatalogs::default());
    assert_identical!(do_get_schemas, CommandGetDbSchemas::default());
    assert_identical!(do_get_tables, CommandGetTables::default());
    assert_identical!(do_get_table_types, CommandGetTableTypes::default());
    assert_identical!(do_get_sql_info, CommandGetSqlInfo::default());
}

/// The `GetFlightInfo` half of every metadata method is tenant-independent
/// too: same result schema and same (constant) ticket bytes for A and B, so
/// the planning RPC discloses nothing tenant-specific either.
#[tokio::test]
async fn metadata_get_flight_info_is_identical_for_every_tenant() {
    let harness = harness().await;

    macro_rules! assert_info_identical {
        ($method:ident, $cmd:expr) => {{
            let info_a = harness
                .service
                .$method($cmd, descriptor(A))
                .await
                .expect(concat!(stringify!($method), " for A"))
                .into_inner();
            let info_b = harness
                .service
                .$method($cmd, descriptor(B))
                .await
                .expect(concat!(stringify!($method), " for B"))
                .into_inner();
            assert_eq!(
                info_a.schema, info_b.schema,
                concat!(stringify!($method), " schema must not depend on the tenant"),
            );
            let ticket_a = info_a.endpoint.first().and_then(|e| e.ticket.clone());
            let ticket_b = info_b.endpoint.first().and_then(|e| e.ticket.clone());
            assert_eq!(
                ticket_a, ticket_b,
                concat!(stringify!($method), " ticket must not depend on the tenant"),
            );
        }};
    }

    assert_info_identical!(get_flight_info_catalogs, CommandGetCatalogs::default());
    assert_info_identical!(get_flight_info_schemas, CommandGetDbSchemas::default());
    assert_info_identical!(get_flight_info_tables, CommandGetTables::default());
    assert_info_identical!(get_flight_info_table_types, CommandGetTableTypes::default());
    assert_info_identical!(get_flight_info_sql_info, CommandGetSqlInfo::default());
}

// ---------------------------------------------------------------------------
// Ticket replay after deadline expiry
// ---------------------------------------------------------------------------

/// A ticket used successfully once, replayed after its embedded deadline has
/// passed, fails `SnapshotInvalidated` -- `UNAVAILABLE` with the redacted
/// storage-unavailable message, never data read under an unprotected pin.
#[tokio::test]
async fn a_ticket_replayed_after_its_deadline_fails_snapshot_invalidated() {
    let harness = harness().await;
    let sql = "SELECT ts, value FROM samples ORDER BY series_id, ts";

    let ticket = harness.get_flight_info(A, sql).await.expect("flight info");

    // First redemption, inside the deadline, returns tenant A's rows.
    let first = harness.do_get(A, &ticket).await.expect("first redemption");
    assert_eq!(
        rows_from_batches(&first),
        vec![
            vec![Cell::Int(100), Cell::float(1.0)],
            vec![Cell::Int(200), Cell::float(2.0)],
        ],
        "the first, valid redemption returns tenant A's samples"
    );

    // Move past the 30 s deadline the ticket carries, then replay it.
    harness.clock.advance(31 * 1_000_000_000);
    let status = harness
        .do_get(A, &ticket)
        .await
        .expect_err("a replay past the deadline is refused");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "expiry maps to SnapshotInvalidated / UNAVAILABLE"
    );
    assert_eq!(
        status.message(),
        MSG_UNAVAILABLE,
        "the refusal carries the redacted transient-storage message, no pin detail"
    );
}

// ---------------------------------------------------------------------------
// Stream cancellation releases the tenant reservation
// ---------------------------------------------------------------------------

/// Dropping a `DoGet` stream mid-transfer returns every reserved byte to the
/// tenant. The stream owns the session, which owns the delegating pool; when it
/// drops, each `MemoryReservation` shrinks through the pool into the shared
/// `TenantMemoryAccountant`, with no explicit release path. Asserted directly
/// against that accountant.
#[tokio::test]
async fn dropping_a_do_get_stream_mid_transfer_zeroes_the_tenant_reservation() {
    let harness = harness().await;
    let tenant = tenant_id(A);
    let budget = harness.executor.tenant_budget(tenant.hash());
    assert_eq!(budget.reserved(), 0, "nothing reserved before the query");

    let sql = "SELECT ts, value FROM samples ORDER BY series_id, ts";
    let ticket = harness.get_flight_info(A, sql).await.expect("flight info");
    let statement = statement_ticket(&ticket);
    let mut request = Request::new(ticket.clone());
    insert(request.metadata_mut(), TOKEN_KEY, A);

    let mut stream = harness
        .service
        .do_get_statement(statement, request)
        .await
        .expect("do get")
        .into_inner();

    // Pull the first message so the query is genuinely in flight, then abandon
    // the stream the way a disconnecting client does -- before it has drained.
    let first = stream.next().await.expect("a first message");
    assert!(first.is_ok(), "the stream is live when it is dropped");
    drop(stream);

    assert_eq!(
        budget.reserved(),
        0,
        "a dropped stream must return every reserved byte to the tenant"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run `sql` as `token` through `GetFlightInfo` + `DoGet` and reduce the result
/// to comparable rows.
async fn flight_rows(harness: &Harness, token: &str, sql: &str) -> Vec<Row> {
    let ticket = harness
        .get_flight_info(token, sql)
        .await
        .unwrap_or_else(|e| panic!("GetFlightInfo failed for {token}: {sql}\n{e}"));
    let batches = harness
        .do_get(token, &ticket)
        .await
        .unwrap_or_else(|e| panic!("DoGet failed for {token}: {sql}\n{e}"));
    rows_from_batches(&batches)
}

/// A `Request<FlightDescriptor>` carrying `token` as the tenant credential.
fn descriptor(token: &str) -> Request<arrow_flight::FlightDescriptor> {
    let mut request = Request::new(arrow_flight::FlightDescriptor::new_cmd(Vec::new()));
    insert(request.metadata_mut(), TOKEN_KEY, token);
    request
}
