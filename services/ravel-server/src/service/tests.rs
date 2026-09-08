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
    /// Each transport's own state, so a test can drive its router end to end
    /// rather than call the service layer directly. What a handler does before
    /// it reaches the service call is invisible from the service call itself,
    /// and the order of authentication against admission is exactly that.
    transports: Transports,
}

/// The router states of the four query transports this crate serves, all over
/// the harness's one store, one resolver, and one admission controller.
struct Transports {
    promql: ravel_query::http::AppState,
    analytics: crate::analytics::AnalyticsState,
    exemplars: crate::exemplars::ExemplarsState,
    #[cfg(feature = "sql")]
    sql: crate::sql::SqlState,
}

/// The `SqlConfig` every harness here builds its executor with. The three
/// request budgets are bounded rather than `EngineConfig::default()`'s, so a
/// clamp against them is a real lowering a test can assert on.
#[cfg(feature = "sql")]
fn sql_config() -> ravel_sql::SqlConfig {
    let base = ravel_sql::SqlConfig::default();
    ravel_sql::SqlConfig {
        engine: EngineConfig {
            max_bytes_scanned: ravel_query::ByteLimit::Bounded(64 << 20),
            max_s3_requests: ravel_query::RequestLimit::Bounded(4_096),
            max_segments: 512,
            ..base.engine
        },
        ..base
    }
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
    harness_with_sql_deadline(
        store,
        limit,
        audit_sink,
        federation,
        Duration::from_secs(30),
    )
}

/// [`harness`] with the SQL surface's wall-deadline ceiling chosen by the
/// caller, for the tests that assert a request deadline is clamped to it.
fn harness_with_sql_deadline(
    store: Arc<dyn ObjectStoreBackend>,
    limit: QueryConcurrencyLimit,
    audit_sink: Arc<dyn QueryAuditSink>,
    federation: Option<ravel_query::distrib::Federation>,
    sql_max_deadline: Duration,
) -> Harness {
    // The SQL surface is behind a feature; without it there is no state to
    // carry the ceiling.
    #[cfg(not(feature = "sql"))]
    let _ = sql_max_deadline;

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
        Arc::clone(&catalog),
        Arc::clone(&store),
        Arc::clone(&resolver),
        Arc::clone(&clock),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );

    #[cfg(feature = "sql")]
    let sql = crate::sql::SqlState {
        executor: Arc::new(ravel_sql::SqlExecutor::new(
            Arc::clone(&catalog),
            ravel_query::SegmentFetcher::new(Arc::clone(&store)),
            ravel_query::LogSegmentFetcher::new(Arc::clone(&store)),
            ravel_sql::SpanSegmentFetcher::new(Arc::clone(&store)),
            sql_config(),
            1 << 30,
        )),
        tenant_resolver: Arc::clone(&resolver),
        store: Arc::clone(&store),
        audit_sink: Arc::clone(&audit_sink),
        clock: Arc::clone(&clock),
        max_deadline: sql_max_deadline,
        query_accounting: default_query_accounting(),
        query_admission: Arc::clone(&admission),
    };

    let transports = Transports {
        promql: crate::query::build_app_state(
            Arc::clone(&catalog),
            Arc::clone(&store),
            Arc::clone(&resolver),
            None,
            config,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
            default_query_accounting(),
            Arc::clone(&admission),
            None,
            None,
            None,
        ),
        analytics: analytics.clone(),
        exemplars: exemplars.clone(),
        #[cfg(feature = "sql")]
        sql: sql.clone(),
    };

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

    #[cfg(feature = "sql")]
    let service = service.with_sql(sql);

    Harness {
        service,
        usage,
        cost,
        admission,
        tenant_hash: tenant().hash(),
        transports,
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

/// The PromQL range request the analytics one mirrors, over the same window.
fn range_request(query: &str) -> RangeRequest {
    RangeRequest {
        query: query.to_string(),
        start_ms: NOW_MS - 60_000,
        end_ms: NOW_MS,
        step_ms: 60_000,
        min_tokens: Vec::new(),
        deadline: Duration::from_secs(30),
        allow_partial: false,
        now_ns: NOW_NS,
        budgets: None,
    }
}

/// A statement over the same window, asking for nothing the server ceilings do
/// not already allow.
#[cfg(feature = "sql")]
fn sql_request(sql: &str) -> ravel_sql::SqlRequest {
    ravel_sql::SqlRequest {
        sql: sql.to_string(),
        window: ravel_types::TimeRange {
            start_ns: NOW_NS - 60_000_000_000,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The metadata family's equivalent of [`analytics_request`]: one selector
/// over the same one-minute window.
fn metadata_request(allow_partial: bool) -> ravel_query::http::MetadataRequest {
    ravel_query::http::MetadataRequest {
        selectors: vec!["up".to_string()],
        window: ravel_types::TimeRange {
            start_ns: NOW_NS - 60_000_000_000,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        deadline: Duration::from_secs(30),
        allow_partial,
        now_ns: NOW_NS,
        budgets: None,
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

/// The wire contract of an audit-trail failure: the caller is told the audit
/// is unavailable, not that storage is. The two are different operational
/// events, and the storage string would send an operator reading a client
/// report at the object store instead of the audit pipeline.
///
/// Asserted on three surfaces at once because they reach the same control
/// through different operations; a per-surface copy of the message is exactly
/// the drift this pins.
#[tokio::test]
async fn audit_failure_reports_the_audit_string_not_storage() {
    let h = harness(
        Arc::new(MemoryStore::new()),
        QueryConcurrencyLimit::Unlimited,
        Arc::new(FailingAuditSink),
        None,
    );

    let analytics = err_of(
        h.service
            .analytics(h.tenant_hash, &analytics_request("up", false))
            .await,
    );
    assert_eq!(analytics.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(analytics.message, ravel_query::http::MSG_AUDIT_UNAVAILABLE);
    assert_ne!(analytics.message, ravel_query::http::MSG_UNAVAILABLE);

    let promql = err_of(
        h.service
            .promql_range(h.tenant_hash, &range_request("up"))
            .await,
    );
    assert_eq!(promql.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(promql.message, ravel_query::http::MSG_AUDIT_UNAVAILABLE);

    #[cfg(feature = "sql")]
    {
        let sql = err_of(
            h.service
                .sql_execute(h.tenant_hash, &sql_request("SELECT * FROM metrics"))
                .await,
        );
        assert_eq!(sql.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(sql.message, ravel_query::http::MSG_AUDIT_UNAVAILABLE);
    }
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

    // The metadata family runs the same order on a path with no value to
    // return, where "record what it spent" is the only thing the refusal can
    // leave behind. `/api/v1/labels` and `/api/v1/series` share one
    // implementation, so one of each pair of assertions covers both.
    let err = err_of(
        h.service
            .labels(h.tenant_hash, &metadata_request(false))
            .await,
    );
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.kind, ServiceErrorKind::Unavailable);
    assert!(err.message.contains("set allow_partial=true"));
    let usage = h.usage.records();
    assert_eq!(usage.len(), 3);
    assert_eq!(usage[2].1, UsageStatus::Success);
    assert_eq!(h.cost.records().len(), 1);

    let outcome = h
        .service
        .labels(h.tenant_hash, &metadata_request(true))
        .await
        .expect("consented partial coverage is served");
    assert!(outcome.partial);
    assert_eq!(h.usage.records().len(), 4);
    assert_eq!(h.cost.records().len(), 2);
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

/// The dropped-future path on a Prometheus-shaped route, which reads its spend
/// from the engine's live accounting view rather than from a result it never
/// gets. Before that view existed, this record was all zeros: the engine owned
/// its `QueryAccounting` internally and returned it only on success.
#[tokio::test]
async fn usage_is_recorded_on_cancel_for_promql() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let gate: GateHandle = fault_store.hold(Op::List, None, Occurrence::Nth(1));
    let h = harness(
        fault_store,
        QueryConcurrencyLimit::Unlimited,
        Arc::new(NoopQueryAuditSink),
        None,
    );

    let request = range_request("up");
    let mut query = Box::pin(h.service.promql_range(h.tenant_hash, &request));

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
    // is what a guard reading an unobserved live handle records.
    assert_eq!(usage[0].2.total_s3_requests(), 2);
    assert_eq!(h.cost.records().len(), 0);
}

/// The same, on the analytics surface, which reaches the engine through its
/// own state rather than through the shared PromQL operations.
#[tokio::test]
async fn usage_is_recorded_on_cancel_for_analytics() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let gate: GateHandle = fault_store.hold(Op::List, None, Occurrence::Nth(1));
    let h = harness(
        fault_store,
        QueryConcurrencyLimit::Unlimited,
        Arc::new(NoopQueryAuditSink),
        None,
    );

    let request = analytics_request("up", false);
    let mut query = Box::pin(h.service.analytics(h.tenant_hash, &request));

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
    assert_eq!(usage[0].2.total_s3_requests(), 2);
    assert_eq!(h.cost.records().len(), 0);
}

/// The sink `build_app_state` wires. The whole PromQL route family folded its
/// usage records into a discarding default before, so `ravel_query_outcome_*`
/// stayed empty for PromQL traffic no matter how many queries ran; only the
/// completed-query cost family moved.
#[tokio::test]
async fn promql_routes_record_usage_through_the_wired_sink() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let query_accounting = default_query_accounting();
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let resolver: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));

    let state = crate::query::build_app_state(
        catalog,
        Arc::clone(&store),
        resolver,
        None,
        EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        Arc::clone(&query_accounting),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
        None,
        None,
        None,
    );

    let request = Request::builder()
        .uri(range_uri())
        .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request");
    let response = ravel_query::http::router(state)
        .oneshot(request)
        .await
        .expect("the route answers");
    assert_eq!(response.status(), StatusCode::OK);

    let rows = query_accounting.outcome_snapshot();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, crate::metrics::QueryOutcomeStatus::Success);
    assert_eq!(rows[0].counters.queries, 1);
}

/// The dropped-future path: a client that disconnects mid-query leaves exactly
/// one trace, the usage record, and no error is ever mapped because there is no
/// caller left to map one for.
///
/// The hold gate is what makes the drop land mid-flight rather than racing the
/// query's completion: the operation is parked inside a store call whose
/// occurrence the test asserts before it drops the future.
#[tokio::test]
async fn usage_is_recorded_on_cancel_for_exemplars() {
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

/// Drive one router with one request and answer with the status it produced.
async fn route_status(
    router: axum::Router,
    request: axum::http::Request<axum::body::Body>,
) -> StatusCode {
    use tower::ServiceExt;

    router
        .oneshot(request)
        .await
        .expect("the route answers")
        .status()
}

/// The PromQL range URI every ordering test drives, over the fixed clock's now.
fn range_uri() -> String {
    let start_s = NOW_MS / 1_000 - 60;
    let end_s = NOW_MS / 1_000;
    format!("/api/v1/query_range?query=up&start={start_s}&end={end_s}&step=60")
}

/// Authentication is outside the seven steps and runs before step 1, so an
/// anonymous caller cannot take a permit from the fleet-global ceiling.
///
/// Calling `QueryService::authenticate` on its own cannot show this: that
/// method takes no permit by construction, whatever order the handler that
/// wraps it uses. What shows it is a saturated ceiling. With the one permit
/// held, admission can only reject, so a handler that admitted before it
/// authenticated would answer 503 to an anonymous request. Every transport
/// answers 401 instead, and the positive control below proves 503 was the
/// reachable alternative rather than an impossible one.
#[tokio::test]
async fn unauthenticated_request_consumes_no_permit() {
    use axum::body::Body;
    use axum::http::Request;

    let h = memory_harness(QueryConcurrencyLimit::Bounded(1));

    let held = h.admission.try_admit().expect("the ceiling's one permit");
    assert_eq!(h.admission.in_flight(), 1);

    let anonymous = |uri: String, body: &'static str| {
        Request::builder()
            .method(if body.is_empty() { "GET" } else { "POST" })
            .uri(uri)
            .header(axum::http::header::AUTHORIZATION, "Bearer not-a-token")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("request")
    };

    assert_eq!(
        route_status(
            ravel_query::http::router(h.transports.promql.clone()),
            anonymous(range_uri(), ""),
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "promql",
    );
    assert_eq!(
        route_status(
            crate::analytics::router(h.transports.analytics.clone()),
            anonymous("/api/v1/analytics".to_string(), "{}"),
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "analytics",
    );
    assert_eq!(
        route_status(
            crate::exemplars::router(h.transports.exemplars.clone()),
            anonymous("/api/v1/query_exemplars?query=up".to_string(), ""),
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "exemplars",
    );
    #[cfg(feature = "sql")]
    assert_eq!(
        route_status(
            crate::sql::router(h.transports.sql.clone()),
            anonymous("/api/v1/sql".to_string(), "{}"),
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "sql",
    );

    // None of them reached admission, so the stock is still exactly the one
    // permit this test holds.
    assert_eq!(h.admission.in_flight(), 1);
    assert_eq!(h.usage.records().len(), 0);

    // The positive control: the same route, the same saturated ceiling, a token
    // that does resolve. This is the answer the assertions above would have got
    // had authentication run second.
    let authenticated = Request::builder()
        .uri(range_uri())
        .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request");
    assert_eq!(
        route_status(
            ravel_query::http::router(h.transports.promql.clone()),
            authenticated,
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE,
    );

    // And once the ceiling frees up, the same authenticated request is served.
    drop(held);
    assert_eq!(h.admission.in_flight(), 0);
    let tenant_hash = h
        .service
        .authenticate(&bearer(TOKEN))
        .expect("the configured token resolves");
    assert_eq!(tenant_hash, h.tenant_hash);
    h.service
        .analytics(tenant_hash, &analytics_request("up", false))
        .await
        .expect("the ceiling has its permit back");
    assert_eq!(h.usage.records().len(), 1);
}

/// Step 2 on the SQL surface, which used to skip it: `sql_execute` and
/// `sql_explain` took the request's own deadline and budgets as given and
/// relied on the HTTP transport having clamped them first.
///
/// The clamp is asserted on the request the layer builds, not on an executed
/// statement, because the executor re-clamps the same budgets against the same
/// config through `RequestBudgets::clamp`, which is idempotent: no outcome
/// distinguishes a service that clamped from one that did not. What the layer
/// owes is that the clamp holds for a caller that is not the HTTP transport,
/// and the MCP adapter (issue #1381) building a `SqlRequest` directly is
/// exactly that caller.
#[cfg(feature = "sql")]
#[tokio::test]
async fn sql_explain_clamps_the_deadline_and_budgets() {
    let h = harness_with_sql_deadline(
        Arc::new(MemoryStore::new()),
        QueryConcurrencyLimit::Unlimited,
        Arc::new(NoopQueryAuditSink),
        None,
        Duration::from_secs(5),
    );
    let state = &h.transports.sql;

    // A caller asking for more than the server allows on every dimension. Each
    // value comes back as the server's own, exactly.
    let greedy = ravel_sql::SqlRequest {
        deadline: Duration::from_secs(600),
        budgets: Some(ravel_query::RequestBudgets {
            max_bytes_scanned: Some(ravel_query::ByteLimit::Unlimited),
            max_store_requests: Some(ravel_query::RequestLimit::Unlimited),
            max_segments: Some(1_000_000),
        }),
        ..sql_request("SELECT 1")
    };
    let clamped = h.service.clamped_sql_request(state, &greedy);
    assert_eq!(clamped.deadline, Duration::from_secs(5));
    let budgets = clamped
        .budgets
        .expect("the layer resolves budgets rather than leaving them absent");
    assert_eq!(
        budgets.max_bytes_scanned,
        Some(ravel_query::ByteLimit::Bounded(64 << 20)),
    );
    assert_eq!(
        budgets.max_store_requests,
        Some(ravel_query::RequestLimit::Bounded(4_096)),
    );
    assert_eq!(budgets.max_segments, Some(512));

    // Lowering only: a caller under every ceiling keeps its own values.
    let modest = ravel_sql::SqlRequest {
        deadline: Duration::from_secs(2),
        budgets: Some(ravel_query::RequestBudgets {
            max_bytes_scanned: Some(ravel_query::ByteLimit::Bounded(1 << 20)),
            max_store_requests: Some(ravel_query::RequestLimit::Bounded(7)),
            max_segments: Some(3),
        }),
        ..sql_request("SELECT 1")
    };
    let clamped = h.service.clamped_sql_request(state, &modest);
    assert_eq!(clamped.deadline, Duration::from_secs(2));
    let budgets = clamped.budgets.expect("budgets");
    assert_eq!(
        budgets.max_bytes_scanned,
        Some(ravel_query::ByteLimit::Bounded(1 << 20)),
    );
    assert_eq!(
        budgets.max_store_requests,
        Some(ravel_query::RequestLimit::Bounded(7)),
    );
    assert_eq!(budgets.max_segments, Some(3));

    // A caller that named no budgets at all still runs under concrete ones.
    let clamped = h
        .service
        .clamped_sql_request(state, &sql_request("SELECT 1"));
    let budgets = clamped.budgets.expect("budgets");
    assert_eq!(
        budgets.max_bytes_scanned,
        Some(ravel_query::ByteLimit::Bounded(64 << 20)),
    );
    assert_eq!(
        budgets.max_store_requests,
        Some(ravel_query::RequestLimit::Bounded(4_096)),
    );
    assert_eq!(budgets.max_segments, Some(512));

    // And both operations run the clamped request rather than the caller's:
    // the greedy deadline above is beyond the ceiling, and neither call
    // reports one.
    h.service
        .sql_explain(h.tenant_hash, &greedy)
        .await
        .expect("an explain over an empty store");
    h.service
        .sql_execute(h.tenant_hash, &greedy)
        .await
        .expect("a statement over an empty store");
}
