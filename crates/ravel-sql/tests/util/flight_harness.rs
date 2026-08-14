//! In-process Flight SQL harness shared across the Flight test suites
//! (flight.rs, flight_differential.rs / flight_tenancy.rs).
//!
//! It drives [`RavelFlightSqlService`] directly rather than over a tonic
//! listener: the trait methods are the contract, and calling them in-process
//! keeps the assertions about *behaviour* -- which rows, which status, which
//! tenant -- instead of transport plumbing, which services/ravel-server's own
//! Flight test covers end to end over a real channel.
//!
//! [`TestAuth`] resolves a fixed token-to-tenant map (deny by default) and
//! [`TestClock`] is moved by hand so ticket expiry is deterministic rather than
//! a sleep. Both are the doubles for the two traits the deployment owns.

#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use futures::TryStreamExt;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService, SqlConfig, SqlExecutor,
};
use ravel_types::accounting::{NoopQueryCostRecorder, QueryCostRecorder};
use ravel_types::{CommitToken, TenantHash, TenantId};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use super::{Fixture, NOW_NS, SegSpec, SeriesSpec, full_window};

/// The metadata key the harness carries the tenant credential in.
pub const TOKEN_KEY: &str = "authorization";

// ---------------------------------------------------------------------------
// Test doubles for the two traits the deployment owns
// ---------------------------------------------------------------------------

/// A tenant resolver over a fixed token-to-tenant map, deny by default.
pub struct TestAuth {
    tenants: HashMap<String, TenantHash>,
}

impl TestAuth {
    pub fn new(pairs: &[(&str, &TenantId)]) -> Arc<Self> {
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
pub struct TestClock(AtomicI64);

impl TestClock {
    pub fn at(now_ns: i64) -> Arc<Self> {
        Arc::new(TestClock(AtomicI64::new(now_ns)))
    }

    pub fn advance(&self, by_ns: i64) {
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

/// A two-series, two-segment fixture dataset for one tenant.
pub fn specs() -> Vec<SegSpec> {
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

pub struct Harness {
    pub service: RavelFlightSqlService,
    pub executor: Arc<SqlExecutor>,
    pub clock: Arc<TestClock>,
    pub store: Arc<dyn ObjectStoreBackend>,
}

impl Harness {
    pub async fn build(
        store: Arc<dyn ObjectStoreBackend>,
        tenants: &[(&TenantId, &[SegSpec])],
    ) -> Self {
        Harness::build_with_recorder(store, tenants, Arc::new(NoopQueryCostRecorder)).await
    }

    /// Like [`Harness::build`], but with an explicit cost recorder so a test
    /// can inspect what each Flight RPC folded into the aggregator.
    pub async fn build_with_recorder(
        store: Arc<dyn ObjectStoreBackend>,
        tenants: &[(&TenantId, &[SegSpec])],
        recorder: Arc<dyn QueryCostRecorder>,
    ) -> Self {
        let fixture =
            Fixture::build(Arc::clone(&store), tenants, SqlConfig::default(), 1 << 30).await;
        let executor = Arc::new(SqlExecutor::new(
            Arc::clone(&fixture.catalog),
            fixture.fetcher.clone(),
            LogSegmentFetcher::new(Arc::clone(&fixture.store)),
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
            recorder,
            ravel_query::QueryAdmissionController::shared(
                ravel_query::QueryConcurrencyLimit::Unlimited,
            ),
        );
        Harness {
            service,
            executor,
            clock,
            store,
        }
    }

    pub async fn memory(tenants: &[(&TenantId, &[SegSpec])]) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        Harness::build(store, tenants).await
    }

    /// `GetFlightInfo` for `sql` as `token`, over the fixture's full window.
    pub async fn get_flight_info(&self, token: &str, sql: &str) -> Result<Ticket, Status> {
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
    pub async fn do_get(&self, token: &str, ticket: &Ticket) -> Result<Vec<RecordBatch>, Status> {
        let stream = self.do_get_stream(token, ticket).await?;
        collect(stream).await
    }

    /// `DoGet` for a ticket as `token`, returning the raw, undrained
    /// `FlightData` stream instead of collecting it. A test holds this to keep
    /// the execution's concurrency permit (ADR-0061 decision 2) live: the permit
    /// lives inside the stream's `RecordOnStreamEnd` guard, so it is released the
    /// moment the returned value is dropped or fully drained, and held until then.
    pub async fn do_get_stream(
        &self,
        token: &str,
        ticket: &Ticket,
    ) -> Result<DoGetRawStream, Status> {
        let statement = statement_ticket(ticket);
        let mut request = Request::new(ticket.clone());
        insert(request.metadata_mut(), TOKEN_KEY, token);

        Ok(self
            .service
            .do_get_statement(statement, request)
            .await?
            .into_inner())
    }
}

/// The concrete undrained `DoGet` stream type, held open by concurrency tests.
pub type DoGetRawStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>,
>;

/// Re-read the `TicketStatementQuery` out of the opaque ticket bytes, which is
/// what arrow-flight's `do_get` dispatcher does before calling
/// `do_get_statement`.
pub fn statement_ticket(ticket: &Ticket) -> TicketStatementQuery {
    use prost::Message;
    let any = arrow_flight::sql::Any::decode(&*ticket.ticket).expect("ticket is an Any");
    any.unpack::<TicketStatementQuery>()
        .expect("ticket decodes")
        .expect("ticket is a TicketStatementQuery")
}

pub fn insert(metadata: &mut MetadataMap, key: &'static str, value: &str) {
    metadata.insert(key, value.parse().expect("ascii metadata value"));
}

/// The fixture window, expressed the way a Flight client expresses it.
pub fn window_metadata(metadata: &mut MetadataMap) {
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

pub async fn collect(
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
pub fn merged(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, batches).expect("concat")
}
