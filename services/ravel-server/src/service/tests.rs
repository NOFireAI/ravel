//! The control ordering every query operation in this module promises.
//!
//! Each test here pins one step of the seven, on the path where that step used
//! to be missing or out of order: analytics and exemplars ran outside the
//! admission ceiling, exemplars recorded no cost at all, and only the SQL
//! surface recorded a spend when a client disconnected mid-query. The
//! wire-level behavior of each surface stays pinned by its own end-to-end
//! suite; what is pinned here is the order, which no end-to-end assertion can
//! see.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Mutex;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_maintain::{AuditEvent, MaintainError, NoopQueryAuditSink};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_query::{EngineConfig, QueryConcurrencyLimit};
use ravel_tenant_resolve::StaticBearerTokenResolver;
use ravel_types::TenantId;
use ravel_types::accounting::QueryWorkloadClass;

use super::*;

const TOKEN: &str = "acme-token";
const NOW_NS: i64 = 1_700_000_000_000_000_000;
const NOW_MS: i64 = NOW_NS / 1_000_000;

/// The failure of a call whose success value is not `Debug` (an outcome
/// carries a whole matrix), so a test can assert on the error without the
/// service's outcome types having to derive `Debug` for it.
fn err_of<T>(result: Result<T, ServiceError>) -> ServiceError {
    match result {
        Ok(_) => panic!("expected a failure"),
        Err(err) => err,
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
    );
    headers
}

/// Every usage record the guard folded, in order, with the outcome status and
/// the spend it carried.
#[derive(Default)]
struct RecordingUsage {
    records: Mutex<Vec<(TenantHash, UsageStatus, QueryAccountingSnapshot)>>,
}

impl RecordingUsage {
    fn records(&self) -> Vec<(TenantHash, UsageStatus, QueryAccountingSnapshot)> {
        self.records.lock().expect("lock").clone()
    }
}

impl QueryUsageSink for RecordingUsage {
    fn record_usage(
        &self,
        tenant_hash: TenantHash,
        status: UsageStatus,
        accounting: &QueryAccountingSnapshot,
        _estimate: &CostEstimate,
    ) {
        self.records
            .lock()
            .expect("lock")
            .push((tenant_hash, status, *accounting));
    }
}

/// Every completed-query cost record, which only a query that produced an
/// answer folds.
#[derive(Default)]
struct RecordingCost {
    records: Mutex<Vec<(TenantHash, QueryAccountingSnapshot)>>,
}

impl RecordingCost {
    fn records(&self) -> Vec<(TenantHash, QueryAccountingSnapshot)> {
        self.records.lock().expect("lock").clone()
    }
}

impl QueryCostRecorder for RecordingCost {
    fn record(
        &self,
        accounting: &QueryAccountingSnapshot,
        _estimate: &CostEstimate,
        tenant_hash: TenantHash,
        _workload_class: QueryWorkloadClass,
    ) {
        self.records
            .lock()
            .expect("lock")
            .push((tenant_hash, *accounting));
    }
}

/// An audit pipeline that cannot accept the event, which is what
/// `audit_mode=required` surfaces when its flush fails.
struct FailingAuditSink;

#[async_trait::async_trait]
impl QueryAuditSink for FailingAuditSink {
    async fn submit(&self, _event: AuditEvent) -> Result<(), MaintainError> {
        Err(MaintainError::Write("audit pipeline stopped".to_string()))
    }
}

/// One service over one store, with the sinks readable afterwards.
struct Harness {
    service: QueryService,
    usage: Arc<RecordingUsage>,
    cost: Arc<RecordingCost>,
    admission: Arc<QueryAdmissionController>,
    tenant_hash: TenantHash,
}

/// The shape every test here builds: a service over `store` with both query
/// surfaces attached, a recording usage sink and cost recorder, and whatever
/// admission ceiling, audit sink, and federation the test needs.
fn harness(
    store: Arc<dyn ObjectStoreBackend>,
    limit: QueryConcurrencyLimit,
    audit_sink: Arc<dyn QueryAuditSink>,
    federation: Option<ravel_query::distrib::Federation>,
) -> Harness {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let config = EngineConfig::default();
    let engine = QueryEngine::new(Arc::clone(&catalog), Arc::clone(&store), config);
    let engine = match federation {
        Some(federation) => engine.with_federation(Arc::new(federation)),
        None => engine,
    };
    let engine = Arc::new(engine);

    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let resolver: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));
    let clock: Arc<dyn Clock> = Arc::new(FixedClock);
    let admission = QueryAdmissionController::shared(limit);
    let usage = Arc::new(RecordingUsage::default());
    let cost = Arc::new(RecordingCost::default());

    let analytics = crate::analytics::AnalyticsState {
        engine: Arc::clone(&engine),
        tenant_resolver: Arc::clone(&resolver),
        clock: Arc::clone(&clock),
        query_accounting: default_query_accounting(),
        audit_sink: Arc::clone(&audit_sink),
        query_admission: Arc::clone(&admission),
    };
    let exemplars = crate::exemplars::ExemplarsState::from_engine(
        engine.as_ref(),
        catalog,
        Arc::clone(&store),
        Arc::clone(&resolver),
        Arc::clone(&clock),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );

    let service = QueryService::new(
        resolver,
        clock,
        Arc::clone(&admission),
        Arc::clone(&cost) as Arc<dyn QueryCostRecorder>,
        Arc::clone(&usage) as Arc<dyn QueryUsageSink>,
        audit_sink,
    )
    .with_engine(engine)
    .with_analytics(analytics)
    .with_exemplars(exemplars);

    Harness {
        service,
        usage,
        cost,
        admission,
        tenant_hash: tenant().hash(),
    }
}

fn memory_harness(limit: QueryConcurrencyLimit) -> Harness {
    harness(
        Arc::new(MemoryStore::new()),
        limit,
        Arc::new(NoopQueryAuditSink),
        None,
    )
}

/// A one-minute range over the fixed clock's now, opted out of partial
/// coverage unless the test says otherwise.
fn analytics_request(query: &str, allow_partial: bool) -> AnalyticsRequest {
    AnalyticsRequest {
        query: query.to_string(),
        start_ms: NOW_MS - 60_000,
        end_ms: NOW_MS,
        step_ms: 60_000,
        min_tokens: Vec::new(),
        deadline: Duration::from_secs(30),
        allow_partial,
    }
}

fn exemplars_request() -> ExemplarsRequest {
    ExemplarsRequest {
        query: "up".to_string(),
        start_ns: NOW_NS - 60_000_000_000,
        end_ns: NOW_NS,
        min_tokens: Vec::new(),
        deadline: Duration::from_secs(30),
    }
}

/// A federation over one unavailable, skippable remote cluster, dialed by the
/// production fetcher exactly as `--remote-cluster` wires it. The fan-out skips
/// it and marks the coverage partial, which is the only way an evaluation can
/// come back partial at all (intra-cluster execution never does).
async fn dead_federation() -> ravel_query::distrib::Federation {
    use ravel_query::distrib::{Federation, RemoteCluster};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let endpoint = listener.local_addr().expect("local addr").to_string();
    drop(listener);

    let config = crate::config::RemoteClusterConfig {
        name: "west".to_string(),
        endpoint,
        credential: "operator-west-credential".to_string(),
        tls: false,
        tls_ca_file: None,
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    };
    let fetcher = crate::distrib::FederationSliceFetcher::connect(&config)
        .expect("federation client connects lazily");
    Federation::new(vec![RemoteCluster {
        name: "west".to_string(),
        fetcher: Arc::new(fetcher),
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    }])
}

/// Step 1 on the analytics surface, which used to run entirely outside the
/// fleet-global ceiling. The refusal is the same 503 body every other query
/// surface returns, and it costs no usage record: nothing executed.
#[tokio::test]
async fn analytics_takes_a_query_admission_permit() {
    let h = memory_harness(QueryConcurrencyLimit::Bounded(1));
    let held = h.admission.try_admit().expect("the only permit");

    let err = err_of(
        h.service
            .analytics(h.tenant_hash, &analytics_request("up", false))
            .await,
    );
    // the ceiling is full
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.kind, ServiceErrorKind::Unavailable);
    assert_eq!(err.message, ravel_query::http::MSG_CONCURRENCY);
    assert_eq!(h.usage.records().len(), 0);

    drop(held);
    h.service
        .analytics(h.tenant_hash, &analytics_request("up", false))
        .await
        .expect("the released permit admits the next query");
    assert_eq!(h.usage.records().len(), 1);
}

/// Step 4 and the cost fold on the exemplars surface, which recorded neither
/// before. The two records agree: the completed-query cost is the same spend
/// the usage record carries.
#[tokio::test]
async fn exemplars_records_cost() {
    let h = memory_harness(QueryConcurrencyLimit::Unlimited);

    let outcome = h
        .service
        .exemplars(h.tenant_hash, &exemplars_request())
        .await
        .expect("empty tenant resolves to an empty result");
    assert_eq!(outcome.series.len(), 0);

    let cost = h.cost.records();
    assert_eq!(cost.len(), 1);
    assert_eq!(cost[0].0, h.tenant_hash);

    let usage = h.usage.records();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].1, UsageStatus::Success);
    assert_eq!(
        cost[0].1.total_s3_requests(),
        usage[0].2.total_s3_requests()
    );
    // The exact spend of a resolve against an empty tenant. Pinned rather than
    // bounded: a cost record built from a fresh handle instead of the query's
    // own would read zero here and still satisfy any lower bound.
    assert_eq!(cost[0].1.total_s3_requests(), 3);
}

/// Step 4 before step 5: the audit submission fails after the query already
/// ran and spent, so the usage record exists even though the caller gets a
/// retryable failure. The completed-query cost fold is on the far side of the
/// audit and does not happen: no answer was released.
#[tokio::test]
async fn usage_is_recorded_before_audit_failure_maps_to_error() {
    let h = harness(
        Arc::new(MemoryStore::new()),
        QueryConcurrencyLimit::Unlimited,
        Arc::new(FailingAuditSink),
        None,
    );

    let err = err_of(
        h.service
            .analytics(h.tenant_hash, &analytics_request("up", false))
            .await,
    );
    // an unauditable read is refused
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.kind, ServiceErrorKind::Unavailable);

    let usage = h.usage.records();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].0, h.tenant_hash);
    assert_eq!(usage[0].1, UsageStatus::Success);
    assert_eq!(h.cost.records().len(), 0);
}

/// Step 4 before step 6: a query whose coverage came back partial without the
/// caller's consent is refused, and the work it did on the way there is still
/// recorded.
#[tokio::test]
async fn usage_is_recorded_before_partial_refusal() {
    let h = harness(
        Arc::new(MemoryStore::new()),
        QueryConcurrencyLimit::Unlimited,
        Arc::new(NoopQueryAuditSink),
        Some(dead_federation().await),
    );

    let err = err_of(
        h.service
            .analytics(h.tenant_hash, &analytics_request("up", false))
            .await,
    );
    // unconsented partial coverage is refused
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.kind, ServiceErrorKind::Unavailable);
    assert!(err.message.contains("set allow_partial=true"));

    let usage = h.usage.records();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].1, UsageStatus::Success);
    assert_eq!(h.cost.records().len(), 0);

    // The same evaluation with consent passes the gate, which is what makes the
    // refusal above the gate's decision and not a failed evaluation.
    let outcome = h
        .service
        .analytics(h.tenant_hash, &analytics_request("up", true))
        .await
        .expect("consented partial coverage is served");
    assert!(outcome.partial);
    assert_eq!(h.usage.records().len(), 2);
    assert_eq!(h.cost.records().len(), 1);
}

/// Step 4 before the evaluation failure becomes a response. A rejected query
/// records what it spent under the failed status rather than vanishing.
#[tokio::test]
async fn usage_is_recorded_before_evaluation_error() {
    let h = memory_harness(QueryConcurrencyLimit::Unlimited);

    let err = err_of(
        h.service
            .analytics(h.tenant_hash, &analytics_request("sum(", false))
            .await,
    );
    // An unparseable expression is rejected during evaluation, so it carries
    // the validation class rather than the request-parameter one.
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert_eq!(err.kind, ServiceErrorKind::Validation);

    let usage = h.usage.records();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].0, h.tenant_hash);
    assert_eq!(usage[0].1, UsageStatus::Error);
    assert_eq!(h.cost.records().len(), 0);
}

/// The dropped-future path: a client that disconnects mid-query leaves exactly
/// one trace, the usage record, and no error is ever mapped because there is no
/// caller left to map one for.
///
/// The hold gate is what makes the drop land mid-flight rather than racing the
/// query's completion: the operation is parked inside a store call whose
/// occurrence the test asserts before it drops the future.
#[tokio::test]
async fn usage_is_recorded_on_cancel() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let gate: GateHandle = fault_store.hold(Op::List, None, Occurrence::Nth(1));
    let h = harness(
        fault_store,
        QueryConcurrencyLimit::Unlimited,
        Arc::new(NoopQueryAuditSink),
        None,
    );

    let request = exemplars_request();
    // Boxed rather than `tokio::pin!`, because dropping a `Pin<&mut Future>`
    // drops the borrow and leaves the future itself alive: the cancellation
    // this test is about would never happen.
    let mut query = Box::pin(h.service.exemplars(h.tenant_hash, &request));

    tokio::select! {
        _ = &mut query => panic!("the query is held inside the store call"),
        () = gate.wait_until_held(1) => {}
    }
    assert_eq!(gate.held_count(), 1);

    drop(query);

    let usage = h.usage.records();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].0, h.tenant_hash);
    assert_eq!(usage[0].1, UsageStatus::Canceled);
    // The exact spend the resolve had reached when the gate held it: the two
    // requests it issued before its first listing. Not a lower bound. Zero here
    // would mean the drop path read a fresh handle instead of the live one.
    assert_eq!(usage[0].2.total_s3_requests(), 2);
    // No answer was released, so nothing folded into the completed-query cost,
    // and no error mapping ran: the usage record is the whole trace.
    assert_eq!(h.cost.records().len(), 0);
}

/// Authentication is outside the seven steps and runs before step 1, so an
/// anonymous caller cannot take a permit from the fleet-global ceiling. With a
/// ceiling of one, a rejected request followed by a valid one both succeed at
/// what they are supposed to do.
#[tokio::test]
async fn unauthenticated_request_consumes_no_permit() {
    let h = memory_harness(QueryConcurrencyLimit::Bounded(1));

    let err = h
        .service
        .authenticate(&bearer("not-a-token"))
        .expect_err("an unknown token resolves to no tenant");
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    assert_eq!(err.kind, ServiceErrorKind::Unauthorized);
    assert_eq!(h.usage.records().len(), 0);

    let tenant_hash = h
        .service
        .authenticate(&bearer(TOKEN))
        .expect("the configured token resolves");
    assert_eq!(tenant_hash, h.tenant_hash);
    h.service
        .analytics(tenant_hash, &analytics_request("up", false))
        .await
        .expect("the ceiling still has its one permit");
    assert_eq!(h.usage.records().len(), 1);
}
