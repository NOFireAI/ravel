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
//! The broader cross-cutting differential/tenancy/e2e suite is a separate
//! later ticket; these are this ticket's own tests.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, CommandGetCatalogs,
    CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandStatementQuery, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use futures::{StreamExt, TryStreamExt};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{
    FlightAuth, FlightClock, FlightSqlConfig, FlightTicket, RavelFlightSqlService, SqlConfig,
    SqlExecutor,
};
use ravel_types::{CommitToken, TenantHash, TenantId};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use util::{Fixture, NOW_NS, SegSpec, SeriesSpec, full_window, request, tenant_id};

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY series_id, ts";
const TOKEN_KEY: &str = "authorization";

// ---------------------------------------------------------------------------
// Test doubles for the two traits the deployment owns
// ---------------------------------------------------------------------------

/// A tenant resolver over a fixed token-to-tenant map, deny by default.
struct TestAuth {
    tenants: HashMap<String, TenantHash>,
}

impl TestAuth {
    fn new(pairs: &[(&str, &TenantId)]) -> Arc<Self> {
        Arc::new(TestAuth {
            tenants: pairs
                .iter()
                .map(|(token, tenant)| ((*token).to_string(), tenant.hash()))
                .collect(),
        })
    }
}

impl FlightAuth for TestAuth {
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, Status> {
        let raw = metadata
            .get(TOKEN_KEY)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("invalid or missing tenant credentials"))?;
        self.tenants
            .get(raw)
            .copied()
            .ok_or_else(|| Status::unauthenticated("invalid or missing tenant credentials"))
    }

    fn min_commit_tokens(&self, _metadata: &MetadataMap) -> Result<Vec<CommitToken>, Status> {
        Ok(Vec::new())
    }
}

/// A clock the test moves by hand, so ticket expiry is deterministic rather
/// than a sleep.
struct TestClock(AtomicI64);

impl TestClock {
    fn at(now_ns: i64) -> Arc<Self> {
        Arc::new(TestClock(AtomicI64::new(now_ns)))
    }

    fn advance(&self, by_ns: i64) {
        self.0.fetch_add(by_ns, Ordering::AcqRel);
    }
}

impl FlightClock for TestClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn specs() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![
                SeriesSpec::new("cpu", vec![(100, 1.0), (200, 2.0)]),
                SeriesSpec::new("mem", vec![(150, 7.5)]),
            ],
        ),
        SegSpec::new(20, 1, 2, vec![SeriesSpec::new("cpu", vec![(300, 3.0)])]),
    ]
}

struct Harness {
    service: RavelFlightSqlService,
    executor: Arc<SqlExecutor>,
    clock: Arc<TestClock>,
    store: Arc<dyn ObjectStoreBackend>,
}

impl Harness {
    async fn build(
        store: Arc<dyn ObjectStoreBackend>,
        tenants: &[(&TenantId, &[SegSpec])],
    ) -> Self {
        let fixture =
            Fixture::build(Arc::clone(&store), tenants, SqlConfig::default(), 1 << 30).await;
        let executor = Arc::new(SqlExecutor::new(
            Arc::clone(&fixture.catalog),
            fixture.fetcher.clone(),
            SqlConfig::default(),
            1 << 30,
        ));
        let auth = TestAuth::new(
            &tenants
                .iter()
                .map(|(tenant, _)| (tenant.as_str(), *tenant))
                .collect::<Vec<_>>(),
        );
        let clock = TestClock::at(NOW_NS);
        // The window the fixture's data lives in is not "the last hour before
        // NOW_NS", so the request names it explicitly through the metadata
        // keys, exactly as an HTTP caller names `start`/`end`.
        let service = RavelFlightSqlService::new(
            Arc::clone(&executor),
            auth,
            Arc::clone(&clock) as Arc<dyn FlightClock>,
            FlightSqlConfig {
                max_deadline: Duration::from_secs(30),
                ..FlightSqlConfig::default()
            },
        );
        Harness {
            service,
            executor,
            clock,
            store,
        }
    }

    async fn memory(tenants: &[(&TenantId, &[SegSpec])]) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        Harness::build(store, tenants).await
    }

    /// `GetFlightInfo` for `sql` as `token`, over the fixture's full window.
    async fn get_flight_info(&self, token: &str, sql: &str) -> Result<Ticket, Status> {
        let mut request = Request::new(FlightDescriptor::new_cmd(sql.as_bytes().to_vec()));
        insert(request.metadata_mut(), TOKEN_KEY, token);
        window_metadata(request.metadata_mut());

        let info = self
            .service
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: sql.to_string(),
                    transaction_id: None,
                },
                request,
            )
            .await?
            .into_inner();
        let endpoint = info.endpoint.first().expect("one endpoint");
        Ok(endpoint.ticket.clone().expect("endpoint carries a ticket"))
    }

    /// `DoGet` for a ticket as `token`, returning the decoded batches.
    async fn do_get(&self, token: &str, ticket: &Ticket) -> Result<Vec<RecordBatch>, Status> {
        let statement = statement_ticket(ticket);
        let mut request = Request::new(ticket.clone());
        insert(request.metadata_mut(), TOKEN_KEY, token);

        let stream = self
            .service
            .do_get_statement(statement, request)
            .await?
            .into_inner();
        collect(stream).await
    }
}

/// Re-read the `TicketStatementQuery` out of the opaque ticket bytes, which is
/// what arrow-flight's `do_get` dispatcher does before calling
/// `do_get_statement`.
fn statement_ticket(ticket: &Ticket) -> TicketStatementQuery {
    use prost::Message;
    let any = arrow_flight::sql::Any::decode(&*ticket.ticket).expect("ticket is an Any");
    any.unpack::<TicketStatementQuery>()
        .expect("ticket decodes")
        .expect("ticket is a TicketStatementQuery")
}

fn insert(metadata: &mut MetadataMap, key: &'static str, value: &str) {
    metadata.insert(key, value.parse().expect("ascii metadata value"));
}

/// The fixture window, expressed the way a Flight client expresses it.
fn window_metadata(metadata: &mut MetadataMap) {
    let window = full_window();
    insert(
        metadata,
        ravel_sql::flight::START_KEY,
        &format!("{}", window.start_ns as f64 / 1e9),
    );
    insert(
        metadata,
        ravel_sql::flight::END_KEY,
        &format!("{}", window.end_ns as f64 / 1e9),
    );
}

async fn collect(
    stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>,
    >,
) -> Result<Vec<RecordBatch>, Status> {
    let decoded = FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|status| FlightError::Tonic(Box::new(status))),
    );
    decoded
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| match err {
            FlightError::Tonic(status) => *status,
            other => Status::internal(other.to_string()),
        })
}

/// One batch holding every row, so a comparison never depends on how either
/// side happened to chunk its output.
fn merged(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, batches).expect("concat")
}

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
    let decoded =
        FlightTicket::decode(&statement_ticket(&ticket).statement_handle).expect("ticket decodes");
    assert_eq!(decoded.deadline_ns, NOW_NS + 5_000_000_000);
    assert_eq!(decoded.tenant, tenant.hash());
    assert_eq!(decoded.now_ns, NOW_NS);
    assert!(!decoded.segments.is_empty());
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
