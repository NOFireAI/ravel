//! Flight SQL service tests (ticket C1d, issue #152).
//!
//! These drive [`RavelFlightSqlService`] directly rather than through a tonic
//! listener. The trait methods are the contract this ticket delivers, and
//! calling them in-process keeps the assertions about *behaviour* -- which
//! rows, which status, which tenant -- instead of about transport plumbing,
//! which services/ravel-server's own Flight test covers end to end over a
//! real channel.
//!
//! The single most important assertion here is the first one: a Flight query
//! returns exactly what `SqlExecutor::execute` returns for the equivalent
//! HTTP request, batch for batch. Everything else in this file is a rule
//! about what must *not* happen -- a foreign tenant redeeming a ticket, an
//! expired ticket still reading, a metadata method answering an
//! unauthenticated caller.
//!
//! The broader cross-cutting differential/tenancy/e2e suite is issue #153
//! (tests/flight_differential.rs and tests/flight_tenancy.rs); these are this
//! ticket's own tests. The shared in-process harness lives in
//! [`util::flight_harness`], which those suites reuse.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, CommandGetCatalogs,
    CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandStatementQuery, ProstMessageExt, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{FlightClock, FlightSqlConfig, FlightTicket, RavelFlightSqlService};
use tonic::Request;
use tonic::metadata::MetadataMap;
use util::flight_harness::{
    Harness, TOKEN_KEY, TestAuth, collect, insert, merged, specs, statement_ticket,
};
use util::{NOW_NS, SegSpec, SeriesSpec, StallingStore, request, tenant_id};

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY series_id, ts";

// ---------------------------------------------------------------------------
// Parity with the HTTP path
// ---------------------------------------------------------------------------

/// The load-bearing test: the two RPCs together return exactly what
/// `SqlExecutor::execute` returns for the equivalent HTTP request.
#[tokio::test]
async fn get_flight_info_then_do_get_returns_the_http_rows() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let flight = harness.do_get("acme", &ticket).await.expect("do get");

    let http = harness
        .executor
        .execute(tenant.hash(), &request(QUERY))
        .await
        .expect("http execute");

    assert!(http.output.num_rows() > 0, "fixture must produce rows");
    assert_eq!(
        merged(&flight),
        merged(http.output.batches()),
        "flight and http must return identical rows"
    );
}

/// The pin is what makes the two RPCs one query: a commit landing between
/// `GetFlightInfo` and `DoGet` must not appear in the results, because the
/// ticket already fixed the snapshot. A `DoGet` that re-resolved would return
/// the new row and quietly give two different answers to one query.
#[tokio::test]
async fn a_commit_landing_between_the_two_rpcs_is_not_visible() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");

    // A new segment is committed after the ticket was minted.
    let extra = SegSpec::new(30, 1, 3, vec![SeriesSpec::new("cpu", vec![(400, 4.0)])]);
    util::publish_segment(harness.store.as_ref(), &tenant, 99, &extra).await;

    let pinned = harness.do_get("acme", &ticket).await.expect("do get");
    let pinned_rows: usize = pinned.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(pinned_rows, 4, "the ticket's pinned segment set, unchanged");

    // A fresh GetFlightInfo resolves again and does see it, which is what
    // makes the assertion above about pinning rather than about caching.
    let fresh = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let fresh_rows: usize = harness
        .do_get("acme", &fresh)
        .await
        .expect("do get")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(fresh_rows, 5, "a new resolve sees the new commit");
}

// ---------------------------------------------------------------------------
// Tenancy
// ---------------------------------------------------------------------------

/// The security decision this ticket enforces: the ticket's embedded tenant is
/// a value to check, never a source of authority. A ticket minted for one
/// tenant and presented with another's credentials is denied before the pinned
/// snapshot is touched.
#[tokio::test]
async fn a_ticket_redeemed_by_another_tenant_is_denied() {
    let acme = tenant_id("acme");
    let other = tenant_id("other");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&acme, &seg_specs), (&other, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let status = harness
        .do_get("other", &ticket)
        .await
        .expect_err("cross-tenant redemption is denied");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn an_unauthenticated_statement_request_is_rejected() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let status = harness
        .get_flight_info("nope", QUERY)
        .await
        .expect_err("rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

// ---------------------------------------------------------------------------
// Ticket expiry
// ---------------------------------------------------------------------------

/// Replaying a ticket after its deadline fails `SnapshotInvalidated`
/// (`UNAVAILABLE`), never with data read under a pin the GC no longer
/// protects.
#[tokio::test]
async fn an_expired_ticket_is_rejected() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    // Valid right now.
    harness.do_get("acme", &ticket).await.expect("do get");

    // Past the 30 s deadline the ticket carries.
    harness.clock.advance(31 * 1_000_000_000);
    let status = harness
        .do_get("acme", &ticket)
        .await
        .expect_err("expired ticket is refused");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

/// The deadline is minted from the clock at `GetFlightInfo` and bounded by the
/// GC protection horizon, so a request cannot buy a longer pin than the
/// horizon allows.
#[tokio::test]
async fn the_ticket_deadline_is_bounded_by_the_gc_protection_horizon() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let mut harness = Harness::memory(&[(&tenant, &seg_specs)]).await;
    harness.service = RavelFlightSqlService::new(
        Arc::clone(&harness.executor),
        TestAuth::new(&[("acme", &tenant)]),
        Arc::clone(&harness.clock) as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            gc_protection_horizon: Duration::from_secs(5),
            ..FlightSqlConfig::default()
        },
    );

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let decoded = FlightTicket::decode(
        &statement_ticket(&ticket).statement_handle,
        harness.service.ticket_key(),
    )
    .expect("ticket decodes");
    assert_eq!(decoded.deadline_ns, NOW_NS + 5_000_000_000);
    assert_eq!(decoded.tenant, tenant.hash());
    assert_eq!(decoded.now_ns, NOW_NS);
    assert!(!decoded.segments.is_empty());
}

/// Issue #185: a ticket tampered with after minting -- here, its deadline
/// extended -- and re-signed under a key an attacker does not have (they
/// cannot have the server's in-process secret, so any key they pick stands
/// in for that) is refused at `DoGet` with a MAC mismatch, never silently
/// honored.
#[tokio::test]
async fn a_tampered_ticket_is_rejected_by_do_get() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let mut decoded = FlightTicket::decode(
        &statement_ticket(&ticket).statement_handle,
        harness.service.ticket_key(),
    )
    .expect("ticket decodes");

    // Tamper with the pinned deadline, then re-encode under a key that is
    // not the server's: an attacker without the in-process secret cannot
    // reproduce the real MAC over the modified bytes.
    decoded.deadline_ns += 1_000_000_000_000;
    let attacker_key = [0xAAu8; ravel_sql::TICKET_KEY_LEN];
    let forged = decoded.encode(&attacker_key).expect("encode");
    let forged_ticket = Ticket::new(
        TicketStatementQuery {
            statement_handle: forged.into(),
        }
        .as_any()
        .encode_to_vec(),
    );

    let status = harness
        .do_get("acme", &forged_ticket)
        .await
        .expect_err("tampered ticket is refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Issue #186: a ticket honestly carrying a deadline far beyond this
/// deployment's GC protection horizon -- re-signed with the real key, not
/// tampered -- must not hand `DoGet` a multi-hour wall budget. The read never
/// completes ([`StallingStore`]), so the only way this call can end is a
/// deadline firing; before the fix, `DoGet` trusted the ticket's own
/// `deadline_ns` verbatim and this would have hung far past any reasonable
/// test bound. The fix re-derives the deployment's own horizon-bounded budget
/// at redemption and takes the minimum with the ticket's value, so the
/// embedded deadline can only ever shorten the effective budget, never
/// lengthen it past what this deployment allows today.
#[tokio::test]
async fn do_get_bounds_a_stalled_read_to_the_gc_horizon_not_the_tickets_own_deadline() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();

    // Publish through a plain store...
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let plain = Harness::build(Arc::clone(&inner), &[(&tenant, &seg_specs)]).await;
    drop(plain);

    // ...then redeem through a wrapper whose segment reads never complete, so
    // the only possible outcome is a deadline firing, not a race against how
    // fast an in-memory read happens to be.
    let stalling: Arc<dyn ObjectStoreBackend> = StallingStore::new(inner);
    let mut harness = Harness::build(stalling, &[]).await;
    harness.service = RavelFlightSqlService::new(
        Arc::clone(&harness.executor),
        TestAuth::new(&[("acme", &tenant)]),
        Arc::clone(&harness.clock) as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            gc_protection_horizon: Duration::from_millis(50),
            ..FlightSqlConfig::default()
        },
    );

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let mut decoded = FlightTicket::decode(
        &statement_ticket(&ticket).statement_handle,
        harness.service.ticket_key(),
    )
    .expect("ticket decodes");
    // Honestly far beyond the 50 ms horizon -- simulates a ticket this same
    // server signed under a looser policy than the one redemption enforces
    // today.
    decoded.deadline_ns = NOW_NS + 3_600_000_000_000;
    let re_encoded = decoded
        .encode(harness.service.ticket_key())
        .expect("re-encode under the real key");
    let far_future_ticket = Ticket::new(
        TicketStatementQuery {
            statement_handle: re_encoded.into(),
        }
        .as_any()
        .encode_to_vec(),
    );

    let status = harness
        .do_get("acme", &far_future_ticket)
        .await
        .expect_err("bounded by the gc horizon, not the ticket's own deadline");
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
}

// ---------------------------------------------------------------------------
// The snapshot retry contract on the pinned path
// ---------------------------------------------------------------------------

/// A pinned segment that blips missing before the first batch is retried
/// exactly once, against the *same* pin, and the query then succeeds.
#[tokio::test]
async fn a_vanished_segment_before_the_first_batch_retries_once_and_succeeds() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(ravel_object_store::fault::FaultStore::new(
        MemoryStore::new(),
        plan,
    ));
    let harness = Harness::build(store, &[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let batches = harness
        .do_get("acme", &ticket)
        .await
        .expect("the retry against the same pin succeeds");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 4, "every fixture sample survives the retry");
}

/// A pinned segment that stays missing fails `SnapshotInvalidated`. There is
/// no re-resolve to fall back on: the ticket fixed the snapshot, and
/// substituting a different one is exactly what the pin forbids.
#[tokio::test]
async fn a_segment_that_stays_missing_fails_snapshot_invalidated() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Always),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(ravel_object_store::fault::FaultStore::new(
        MemoryStore::new(),
        plan,
    ));
    let harness = Harness::build(store, &[(&tenant, &seg_specs)]).await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let status = harness
        .do_get("acme", &ticket)
        .await
        .expect_err("a permanently missing segment fails");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

// ---------------------------------------------------------------------------
// Cancellation (review F13)
// ---------------------------------------------------------------------------

/// Dropping a `DoGet` stream mid-flight returns the tenant's reserved bytes,
/// with no explicit release path: the stream owns the session, the session
/// owns the delegating pool, and every reservation shrinks through it into the
/// tenant accountant on the way down.
#[tokio::test]
async fn dropping_a_do_get_stream_returns_the_tenant_reservation() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;
    let budget = harness.executor.tenant_budget(tenant.hash());

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");
    let statement = statement_ticket(&ticket);
    let mut request = Request::new(ticket.clone());
    insert(request.metadata_mut(), TOKEN_KEY, "acme");

    let mut stream = harness
        .service
        .do_get_statement(statement, request)
        .await
        .expect("do get")
        .into_inner();
    // Pull one message, then abandon the stream the way a disconnecting
    // client does.
    let _first = stream.next().await.expect("a first message");
    drop(stream);

    assert_eq!(
        budget.reserved(),
        0,
        "a dropped stream must return every reserved byte to the tenant"
    );
}

// ---------------------------------------------------------------------------
// Catalog / metadata
// ---------------------------------------------------------------------------

/// Every catalog/metadata method denies a caller with no valid tenant
/// credentials. The payloads are constants, but default-deny is the invariant
/// regardless (review F17).
#[tokio::test]
async fn catalog_and_metadata_methods_reject_a_request_without_a_tenant() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;
    let service = &harness.service;

    macro_rules! deny {
        ($call:expr) => {{
            let status = $call
                .await
                .map(|_| ())
                .expect_err("rejected without a tenant");
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }};
    }

    let descriptor = || Request::new(FlightDescriptor::new_cmd(Vec::new()));
    let ticket = || Request::new(Ticket::new(Vec::new()));

    deny!(service.get_flight_info_catalogs(CommandGetCatalogs::default(), descriptor()));
    deny!(service.get_flight_info_schemas(CommandGetDbSchemas::default(), descriptor()));
    deny!(service.get_flight_info_tables(CommandGetTables::default(), descriptor()));
    deny!(service.get_flight_info_table_types(CommandGetTableTypes::default(), descriptor()));
    deny!(service.get_flight_info_sql_info(CommandGetSqlInfo::default(), descriptor()));
    deny!(service.do_get_catalogs(CommandGetCatalogs::default(), ticket()));
    deny!(service.do_get_schemas(CommandGetDbSchemas::default(), ticket()));
    deny!(service.do_get_tables(CommandGetTables::default(), ticket()));
    deny!(service.do_get_table_types(CommandGetTableTypes::default(), ticket()));
    deny!(service.do_get_sql_info(CommandGetSqlInfo::default(), ticket()));
    // The statement path too, on both RPCs.
    deny!(service.get_flight_info_statement(
        CommandStatementQuery {
            query: QUERY.to_string(),
            transaction_id: None,
        },
        descriptor(),
    ));
    deny!(service.do_get_statement(TicketStatementQuery::default(), ticket()));
}

/// An authenticated caller sees exactly one catalog, one schema, one table,
/// and one table type.
#[tokio::test]
async fn catalog_and_metadata_methods_answer_an_authenticated_caller() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

    let authed = |request: &mut MetadataMap| insert(request, TOKEN_KEY, "acme");

    let mut request = Request::new(Ticket::new(Vec::new()));
    authed(request.metadata_mut());
    let batches = collect(
        harness
            .service
            .do_get_tables(CommandGetTables::default(), request)
            .await
            .expect("tables")
            .into_inner(),
    )
    .await
    .expect("decodes");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "exactly the samples table");

    let mut request = Request::new(FlightDescriptor::new_cmd(Vec::new()));
    authed(request.metadata_mut());
    let info = harness
        .service
        .get_flight_info_catalogs(CommandGetCatalogs::default(), request)
        .await
        .expect("catalogs info")
        .into_inner();
    assert_eq!(info.endpoint.len(), 1);
    assert!(info.endpoint[0].ticket.is_some());
}

// ---------------------------------------------------------------------------
// Out-of-scope surfaces
// ---------------------------------------------------------------------------

/// Prepared statements are out of the v1 scope and say so explicitly.
#[tokio::test]
async fn prepared_statement_methods_are_unimplemented() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;
    let service = &harness.service;

    // Authenticated on purpose: the refusal must not depend on credentials
    // being absent.
    let mut descriptor = Request::new(FlightDescriptor::new_cmd(Vec::new()));
    insert(descriptor.metadata_mut(), TOKEN_KEY, "acme");
    let status = service
        .get_flight_info_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: Vec::new().into(),
            },
            descriptor,
        )
        .await
        .map(|_| ())
        .expect_err("prepared statements must be refused");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    let mut ticket = Request::new(Ticket::new(Vec::new()));
    insert(ticket.metadata_mut(), TOKEN_KEY, "acme");
    let status = service
        .do_get_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: Vec::new().into(),
            },
            ticket,
        )
        .await
        .map(|_| ())
        .expect_err("prepared statements must be refused");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    let mut action = Request::new(arrow_flight::Action::new("CreatePreparedStatement", vec![]));
    insert(action.metadata_mut(), TOKEN_KEY, "acme");
    let status = service
        .do_action_create_prepared_statement(
            ActionCreatePreparedStatementRequest {
                query: QUERY.to_string(),
                transaction_id: None,
            },
            action,
        )
        .await
        .map(|_| ())
        .expect_err("prepared statements must be refused");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    let mut action = Request::new(arrow_flight::Action::new("ClosePreparedStatement", vec![]));
    insert(action.metadata_mut(), TOKEN_KEY, "acme");
    let status = service
        .do_action_close_prepared_statement(
            ActionClosePreparedStatementRequest {
                prepared_statement_handle: Vec::new().into(),
            },
            action,
        )
        .await
        .map(|_| ())
        .expect_err("prepared statements must be refused");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
}

/// The security gate runs before the catalog is touched, on this transport
/// too: a write statement is refused at `GetFlightInfo` with no LIST.
#[tokio::test]
async fn a_write_statement_is_rejected_before_any_catalog_work() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let store = util::CountingStore::new(Arc::new(MemoryStore::new()));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let harness = Harness::build(backend, &[(&tenant, &seg_specs)]).await;
    let before = store.total_ops();

    let status = harness
        .get_flight_info("acme", "INSERT INTO samples VALUES (1, 2)")
        .await
        .expect_err("rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        store.total_ops(),
        before,
        "a rejected statement must cost no store operation"
    );
}
