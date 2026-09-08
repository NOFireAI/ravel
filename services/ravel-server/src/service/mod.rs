//! The one query service layer every query transport in this process calls
//! (ADR-1374 decision 3).
//!
//! A query surface used to be one handler that did everything: resolve the
//! tenant, take an admission permit, clamp the deadline, run the engine,
//! record the cost, audit the read, gate partial coverage, redact the error.
//! Five surfaces meant five copies, and they had drifted: two of them took no
//! admission permit, one recorded no cost, and only one recorded cost when the
//! client disconnected mid-query.
//!
//! [`QueryService`] is where those controls now live, exactly once. A
//! transport parses its request, authenticates it with [`authenticate`], calls
//! one operation here, and encodes the outcome. The controls themselves are
//! [`ravel_query::http::QueryControls`], shared with the Prometheus-shaped
//! operations that must live in `ravel-query` (a crate that cannot depend on
//! this one), so there is one implementation and not two.
//!
//! Every operation runs the same seven steps in the same order:
//!
//! 1. acquire an admission permit from the fleet-global controller, before any
//!    resolve or GET;
//! 2. clamp the wall deadline and the request budgets against the server
//!    ceilings, lowering only;
//! 3. run the engine call;
//! 4. finalize usage through a drop guard on every exit path, the dropped one
//!    included;
//! 5. submit the audit event and await its durability;
//! 6. apply the partial-coverage consent gate;
//! 7. map the failure through the existing redaction.
//!
//! Step 4 precedes every way an operation can turn into an error, so an audit
//! failure, a partial refusal, an evaluation failure, and a deadline all leave
//! a usage record of what the query spent before it failed. A cancellation
//! (the caller's future dropped) never reaches step 5: it produces no result,
//! and its usage record is its only trace. Nothing here spawns, so dropping
//! the transport's future cancels the engine call itself.
//!
//! Authentication is deliberately not one of the steps: it is a transport
//! concern, it runs before step 1, and it takes no permit. [`authenticate`] is
//! the one function every transport calls for it, so an anonymous caller
//! cannot consume a permit from the fleet-global concurrency ceiling.
//!
//! The second caller of this layer is the MCP adapter (issue #1381). The HTTP
//! transports are the first, and the unchanged end-to-end suites over them are
//! the reachability proof that every existing query route runs through here.

pub mod error;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::HeaderMap;
use ravel_ingest::Clock;
use ravel_maintain::{QueryAuditSink, QueryStatus};
use ravel_promql::Value;
use ravel_query::http::service as core;
use ravel_query::http::{
    InstantOutcome, InstantRequest, LabelValuesOutcome, LabelsOutcome, LiveUsage, MetadataOutcome,
    MetadataRequest, QueryControls, QueryUsageSink, RangeOutcome, RangeRequest, TenantResolver,
    UsageStatus,
};
use ravel_query::{Coverage, QueryAdmissionController, QueryEngine, QueryStats};
use ravel_types::accounting::{
    CostEstimate, QueryAccounting, QueryAccountingSnapshot, QueryCostRecorder,
};
use ravel_types::{CommitToken, TenantHash};
use tracing::Instrument;

use crate::metrics::{QueryAccountingMetrics, QueryOutcomeStatus};

pub use error::{ApiError, ServiceError, ServiceErrorKind};

/// Resolve the caller's credentials to a tenant. The one authentication step
/// every query transport runs, before it asks the service for anything.
///
/// It is outside every operation below (they take an already-authenticated
/// [`TenantHash`]) and it takes no admission permit, so an anonymous request
/// cannot consume one from the fleet-global concurrency ceiling.
pub fn authenticate(
    resolver: &dyn TenantResolver,
    headers: &HeaderMap,
) -> Result<TenantHash, ServiceError> {
    resolver
        .resolve(headers)
        .map(|tenant| tenant.hash())
        .map_err(|_| ServiceError::unauthorized())
}

/// A shared, cloneable view of the [`QueryAccounting`] handle a running query
/// is spending through.
///
/// The drop path of a cancelled query has no outcome to read counters from, so
/// it reads them here instead: a query that fetched objects for two minutes and
/// was then abandoned records what it actually spent, not zeros. The mirror of
/// `ravel_sql::LiveAccounting` for the surfaces that do not go through
/// `ravel-sql` (`ravel-sql` is behind the `sql` feature, and its type is not
/// reachable from a default build).
#[derive(Clone, Default)]
pub struct LiveCost(Arc<Mutex<QueryAccounting>>);

impl LiveCost {
    /// A live view whose counters are all zero until an attempt installs its
    /// handle.
    pub fn new() -> Self {
        LiveCost::default()
    }

    /// Point this view at `accounting` (the attempt about to run). Clones the
    /// handle, so the two share one atomic counter block and every increment
    /// the attempt makes is visible through [`LiveUsage::snapshot`].
    pub fn install(&self, accounting: &QueryAccounting) {
        *self.lock() = accounting.clone();
    }

    /// Lock the inner slot, recovering a poisoned guard. The slot holds one
    /// cheap-to-clone handle and no torn state, so recovering is strictly
    /// better than failing every later snapshot.
    fn lock(&self) -> std::sync::MutexGuard<'_, QueryAccounting> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl LiveUsage for LiveCost {
    fn snapshot(&self) -> QueryAccountingSnapshot {
        self.lock().snapshot()
    }
}

/// The `/metrics` outcome aggregator, as the service layer's usage sink. This
/// is the seam that makes the drop-guard record land in the same
/// `ravel_query_outcome_*` family `/api/v1/sql` has always folded into, for
/// every surface rather than one.
impl QueryUsageSink for QueryAccountingMetrics {
    fn record_usage(
        &self,
        tenant_hash: TenantHash,
        status: UsageStatus,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    ) {
        self.record_outcome(tenant_hash, outcome_status(status), accounting, estimate);
    }
}

fn outcome_status(status: UsageStatus) -> QueryOutcomeStatus {
    match status {
        UsageStatus::Success => QueryOutcomeStatus::Success,
        UsageStatus::Error => QueryOutcomeStatus::Error,
        UsageStatus::Timeout => QueryOutcomeStatus::Timeout,
        UsageStatus::Canceled => QueryOutcomeStatus::Canceled,
    }
}

/// The default `/metrics` aggregator for a surface state built without one:
/// an empty per-tenant allowlist, so every tenant folds into the `other`
/// bucket and nothing allocates a row of its own.
pub fn default_query_accounting() -> Arc<QueryAccountingMetrics> {
    Arc::new(QueryAccountingMetrics::new(HashSet::new()))
}

/// One analytics evaluation, already parsed and authenticated. The op itself
/// is not here: the service runs the range evaluation the analytic is applied
/// to, and applying it is the transport's encoding step.
#[derive(Debug, Clone)]
pub struct AnalyticsRequest {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
    pub allow_partial: bool,
}

/// The matrix an analytics call will be applied to, with its coverage already
/// consented to and its cost already recorded.
pub struct AnalyticsOutcome {
    pub value: Value,
    pub stats: QueryStats,
    pub partial: bool,
    /// Per-slice fragment observability for a distributed run (ADR-0071
    /// `stats.fragments[]`), empty for a run the cost gate did not fan out.
    pub fragments: Vec<crate::distrib::FragmentStatEntry>,
}

/// One exemplar query, already parsed and authenticated.
#[derive(Debug, Clone)]
pub struct ExemplarsRequest {
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
}

/// The exemplars a query matched, with its cost already recorded.
pub struct ExemplarsOutcome {
    pub series: Vec<crate::exemplars::ExemplarSeriesJson>,
    pub stats: crate::exemplars::QueryStatsJson,
}

/// The query service layer: one value holding the shared controls plus the
/// state of every query surface this process serves.
///
/// Cheap to clone (every field is an `Arc` or a clone-by-`Arc` state), so a
/// transport that holds its own surface state can build the façade per request
/// rather than threading a second handle through its router state.
///
/// An operation whose surface is not configured returns
/// [`ServiceErrorKind::Unsupported`]: a `Mode::Ingest` server genuinely serves
/// no query surface, and that is a typed refusal rather than a panic.
#[derive(Clone)]
pub struct QueryService {
    tenant_resolver: Arc<dyn TenantResolver>,
    controls: QueryControls,
    clock: Arc<dyn Clock>,
    engine: Option<Arc<QueryEngine>>,
    analytics: Option<crate::analytics::AnalyticsState>,
    exemplars: Option<crate::exemplars::ExemplarsState>,
    #[cfg(feature = "sql")]
    sql: Option<crate::sql::SqlState>,
}

impl QueryService {
    /// The service with the shared controls and no surface configured. Each
    /// `with_*` below attaches one; a surface's own state carries the pieces
    /// its engine call needs, and the controls are shared across all of them.
    pub fn new(
        tenant_resolver: Arc<dyn TenantResolver>,
        clock: Arc<dyn Clock>,
        admission: Arc<QueryAdmissionController>,
        cost_recorder: Arc<dyn QueryCostRecorder>,
        usage_sink: Arc<dyn QueryUsageSink>,
        audit_sink: Arc<dyn QueryAuditSink>,
    ) -> Self {
        QueryService {
            tenant_resolver,
            controls: QueryControls {
                admission,
                cost_recorder,
                usage_sink,
                audit_sink,
            },
            clock,
            engine: None,
            analytics: None,
            exemplars: None,
            #[cfg(feature = "sql")]
            sql: None,
        }
    }

    /// The service built from one process-global `/metrics` aggregator, which
    /// is both the completed-query cost recorder and the every-exit usage
    /// sink.
    pub fn with_metrics(
        tenant_resolver: Arc<dyn TenantResolver>,
        clock: Arc<dyn Clock>,
        admission: Arc<QueryAdmissionController>,
        query_accounting: Arc<QueryAccountingMetrics>,
        audit_sink: Arc<dyn QueryAuditSink>,
    ) -> Self {
        QueryService::new(
            tenant_resolver,
            clock,
            admission,
            Arc::clone(&query_accounting) as Arc<dyn QueryCostRecorder>,
            query_accounting as Arc<dyn QueryUsageSink>,
            audit_sink,
        )
    }

    pub fn with_engine(mut self, engine: Arc<QueryEngine>) -> Self {
        self.engine = Some(engine);
        self
    }

    pub fn with_analytics(mut self, state: crate::analytics::AnalyticsState) -> Self {
        self.analytics = Some(state);
        self
    }

    pub fn with_exemplars(mut self, state: crate::exemplars::ExemplarsState) -> Self {
        self.exemplars = Some(state);
        self
    }

    #[cfg(feature = "sql")]
    pub fn with_sql(mut self, state: crate::sql::SqlState) -> Self {
        self.sql = Some(state);
        self
    }

    /// The tenant resolver a transport authenticates against before it calls
    /// any operation here.
    pub fn tenant_resolver(&self) -> &dyn TenantResolver {
        self.tenant_resolver.as_ref()
    }

    /// Resolve the caller's credentials, the transport's step before step 1.
    pub fn authenticate(&self, headers: &HeaderMap) -> Result<TenantHash, ServiceError> {
        authenticate(self.tenant_resolver.as_ref(), headers)
    }

    /// The shared controls, for a surface that runs its own engine call and
    /// needs the same seven steps around it.
    pub fn controls(&self) -> &QueryControls {
        &self.controls
    }

    fn engine(&self) -> Result<&QueryEngine, ServiceError> {
        match &self.engine {
            Some(engine) => Ok(engine.as_ref()),
            None => Err(ServiceError::unsupported_surface("PromQL")),
        }
    }

    /// `/api/v1/query`.
    pub async fn promql_instant(
        &self,
        tenant_hash: TenantHash,
        request: &InstantRequest,
    ) -> Result<InstantOutcome, ServiceError> {
        let engine = self.engine()?;
        Ok(core::promql_instant(&self.controls, engine, tenant_hash, request).await?)
    }

    /// `/api/v1/query_range`.
    pub async fn promql_range(
        &self,
        tenant_hash: TenantHash,
        request: &RangeRequest,
    ) -> Result<RangeOutcome, ServiceError> {
        let engine = self.engine()?;
        Ok(core::promql_range(&self.controls, engine, tenant_hash, request).await?)
    }

    /// `/api/v1/labels`.
    pub async fn labels(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<LabelsOutcome, ServiceError> {
        let engine = self.engine()?;
        Ok(core::labels(&self.controls, engine, tenant_hash, request).await?)
    }

    /// `/api/v1/label/{name}/values`.
    pub async fn label_values(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
        name: &str,
        include_log_metric_names: bool,
    ) -> Result<LabelValuesOutcome, ServiceError> {
        let engine = self.engine()?;
        Ok(core::label_values(
            &self.controls,
            engine,
            tenant_hash,
            request,
            name,
            include_log_metric_names,
        )
        .await?)
    }

    /// `/api/v1/series`.
    pub async fn series(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<MetadataOutcome, ServiceError> {
        let engine = self.engine()?;
        Ok(core::series(&self.controls, engine, tenant_hash, request).await?)
    }

    /// `/api/v1/analytics`: the range evaluation an analytic is applied to,
    /// under the same seven steps as every other operation.
    ///
    /// The evaluation itself is `range_with_stats`, the same call
    /// `/api/v1/query_range` made before per-request budgets existed, because
    /// this surface exposes no per-request budget knob: step 2 clamps the wall
    /// deadline and the engine's configured ceilings alone govern the rest.
    pub async fn analytics(
        &self,
        tenant_hash: TenantHash,
        request: &AnalyticsRequest,
    ) -> Result<AnalyticsOutcome, ServiceError> {
        let state = self
            .analytics
            .as_ref()
            .ok_or_else(|| ServiceError::unsupported_surface("analytics"))?;

        let _permit = self.controls.admit()?;
        let deadline = self
            .controls
            .clamp_deadline(request.deadline, state.engine.config().deadline);
        let now_ns = self.clock.now_ns();
        let live = ravel_query::LiveQueryAccounting::new();
        let guard = self
            .controls
            .usage_guard(tenant_hash, Arc::new(live.clone()));
        let engine = state.engine.with_live_usage(&live);

        // Per-slice fragment observability (ADR-0071 `stats.fragments[]`). The
        // sink is installed in task-local storage so every distributed slice
        // the engine dispatches records into it, which requires wrapping the
        // engine call itself rather than the transport around it.
        let fragment_sink = crate::distrib::FragmentStatsSink::new();
        let span = query_span("analytics_query", tenant_hash);
        let eval = crate::distrib::with_fragment_stats(
            fragment_sink.clone(),
            engine
                .range_with_stats(
                    tenant_hash,
                    &request.query,
                    request.start_ms,
                    request.end_ms,
                    request.step_ms,
                    &request.min_tokens,
                    now_ns,
                    deadline,
                )
                .instrument(span.clone()),
        )
        .await
        .map_err(ServiceError::from_query);

        let status = finish_usage(guard, &eval, |(_, stats): &(Value, QueryStats)| {
            (stats.accounting, stats.estimate)
        });

        self.controls
            .audit(
                tenant_hash,
                now_ns,
                &request.query,
                "analytics",
                (ms_to_ns(request.start_ms), ms_to_ns(request.end_ms)),
                status,
            )
            .await?;
        let (value, stats) = eval?;

        let coverage = Coverage::from_stats(&stats);
        self.controls
            .gate_partial(&coverage, request.allow_partial)?;
        record_span_cost(&span, &stats.accounting);
        self.controls
            .record_cost(tenant_hash, &stats.accounting, &stats.estimate);
        Ok(AnalyticsOutcome {
            value,
            partial: coverage.is_partial(),
            stats,
            fragments: fragment_sink.take(),
        })
    }

    /// `/api/v1/query_exemplars`.
    ///
    /// This surface reports no [`CostEstimate`] (an estimate is only ever
    /// computed from a real resolved snapshot, and this path resolves without
    /// one), so its usage and cost records carry the zero estimate and the
    /// counters the read actually issued.
    pub async fn exemplars(
        &self,
        tenant_hash: TenantHash,
        request: &ExemplarsRequest,
    ) -> Result<ExemplarsOutcome, ServiceError> {
        let state = self
            .exemplars
            .as_ref()
            .ok_or_else(|| ServiceError::unsupported_surface("exemplars"))?;

        let _permit = self.controls.admit()?;
        let deadline = self
            .controls
            .clamp_deadline(request.deadline, state.deadline);
        let now_ns = state.clock.now_ns();

        // The live handle each read attempt installs its own accounting into,
        // so a cancelled or timed-out exemplar query records the requests and
        // bytes it issued rather than zeros.
        let live = LiveCost::new();
        let guard = self
            .controls
            .usage_guard(tenant_hash, Arc::new(live.clone()));

        let collected =
            crate::exemplars::collect_within_deadline(state, tenant_hash, request, deadline, &live)
                .await;

        let status = finish_usage(guard, &collected, |_| {
            (live.snapshot(), core::zero_estimate())
        });

        self.controls
            .audit(
                tenant_hash,
                now_ns,
                &request.query,
                "exemplars",
                (request.start_ns, request.end_ns),
                status,
            )
            .await?;
        let (series, stats) = collected?;

        self.controls
            .record_cost(tenant_hash, &live.snapshot(), &core::zero_estimate());
        Ok(ExemplarsOutcome { series, stats })
    }

    /// Step 2 for the SQL surface: the request as it will actually run, with
    /// its wall deadline lowered to `SqlState::max_deadline` and its budgets
    /// lowered to the executor's configured ceilings.
    ///
    /// The HTTP transport clamps the deadline too, and the executor re-clamps
    /// the budgets through `RequestBudgets::clamp`, which is idempotent. Both
    /// stay. What the layer owes is that the clamp holds for every caller of
    /// the operation, not only for the ones that happen to clamp first: the
    /// MCP adapter (issue #1381) builds a `SqlRequest` directly, and an
    /// operation whose only clamp lives in one of its transports has no clamp
    /// at all from the others.
    #[cfg(feature = "sql")]
    pub(crate) fn clamped_sql_request(
        &self,
        state: &crate::sql::SqlState,
        request: &ravel_sql::SqlRequest,
    ) -> ravel_sql::SqlRequest {
        ravel_sql::SqlRequest {
            deadline: self
                .controls
                .clamp_deadline(request.deadline, state.max_deadline),
            budgets: Some(
                self.controls
                    .clamp_budgets(request.budgets.as_ref(), &state.executor.config().engine),
            ),
            ..request.clone()
        }
    }

    /// `POST /api/v1/sql`: one read-only SQL statement.
    #[cfg(feature = "sql")]
    pub async fn sql_execute(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
    ) -> Result<ravel_sql::SqlOutcome, ServiceError> {
        let state = self
            .sql
            .as_ref()
            .ok_or_else(|| ServiceError::unsupported_surface("SQL"))?;

        let _permit = self.controls.admit()?;
        let request = &self.clamped_sql_request(state, request);
        let live = ravel_sql::LiveAccounting::new();
        let guard = self
            .controls
            .usage_guard(tenant_hash, Arc::new(SqlLiveUsage(live.clone())));

        let span = query_span("sql_query", tenant_hash);
        let result = state
            .executor
            .execute_accounted(tenant_hash, request, &live)
            .instrument(span.clone())
            .await
            .map_err(|err| ServiceError::from_sql(err, tenant_hash));

        let status = finish_usage(guard, &result, |outcome: &ravel_sql::SqlOutcome| {
            (outcome.accounting, outcome.estimate)
        });

        self.controls
            .audit(
                tenant_hash,
                request.now_ns,
                &request.sql,
                "sql",
                (request.window.start_ns, request.window.end_ns),
                status,
            )
            .await?;
        let outcome = result?;

        // The SQL surface has no federated fan-out and therefore no partial
        // coverage to consent to; step 6 is vacuous and step 7 already ran in
        // `ServiceError::from_sql`.
        record_span_cost(&span, &outcome.accounting);
        self.controls
            .record_cost(tenant_hash, &outcome.accounting, &outcome.estimate);
        Ok(outcome)
    }

    /// The plan and cost of a statement, without running it. The MCP adapter
    /// (issue #1381) is its caller; no HTTP route exposes it.
    #[cfg(feature = "sql")]
    pub async fn sql_explain(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
    ) -> Result<ravel_sql::ExplainReport, ServiceError> {
        let state = self
            .sql
            .as_ref()
            .ok_or_else(|| ServiceError::unsupported_surface("SQL"))?;

        let _permit = self.controls.admit()?;
        let request = &self.clamped_sql_request(state, request);
        // An explain resolves a snapshot, so it spends store requests and is
        // accounted like any other read.
        let accounting = QueryAccounting::new();
        let live = LiveCost::new();
        live.install(&accounting);
        let guard = self
            .controls
            .usage_guard(tenant_hash, Arc::new(live.clone()));

        let result = state
            .executor
            .explain_accounted(tenant_hash, request, &accounting)
            .await
            .map_err(|err| ServiceError::from_sql(err, tenant_hash));

        let status = finish_usage(guard, &result, |_| (live.snapshot(), core::zero_estimate()));

        self.controls
            .audit(
                tenant_hash,
                request.now_ns,
                &request.sql,
                "sql",
                (request.window.start_ns, request.window.end_ns),
                status,
            )
            .await?;
        let report = result?;

        self.controls
            .record_cost(tenant_hash, &live.snapshot(), &core::zero_estimate());
        Ok(report)
    }
}

/// `ravel-sql`'s live accounting view, as the service layer's [`LiveUsage`].
/// Both the trait and `LiveAccounting` are foreign to this crate, so the
/// newtype is what makes the impl legal.
#[cfg(feature = "sql")]
struct SqlLiveUsage(ravel_sql::LiveAccounting);

#[cfg(feature = "sql")]
impl LiveUsage for SqlLiveUsage {
    fn snapshot(&self) -> QueryAccountingSnapshot {
        self.0.snapshot()
    }
}

/// Step 4 for one operation: fold the usage record before the outcome becomes
/// anything else, and report the audit status the same outcome implies.
///
/// `cost_of` reads the success value's counters; a failure has none, so its
/// spend comes from the guard's live handle instead. Every operation runs this
/// before its `audit`, its `?`, and its consent gate, which is what makes "the
/// usage record exists even when the request ends as an error" mechanical
/// rather than remembered.
fn finish_usage<T>(
    guard: ravel_query::http::UsageGuard,
    result: &Result<T, ServiceError>,
    cost_of: impl FnOnce(&T) -> (QueryAccountingSnapshot, CostEstimate),
) -> QueryStatus {
    match result {
        Ok(value) => {
            let (accounting, estimate) = cost_of(value);
            guard.finish(UsageStatus::Success, &accounting, &estimate);
            QueryStatus::Ok
        }
        Err(err) => {
            guard.finish_failed(err.usage_status());
            QueryStatus::Error
        }
    }
}

/// A request-level span carrying only bounded values (ADR-0044 section 5): the
/// tenant hash, the workload class, and, once the query finishes, the final
/// store request and byte counts. No query text, label values, or object keys
/// ever become span fields. Every query over these transports is an
/// interactive, client-driven one.
fn query_span(name: &'static str, tenant_hash: TenantHash) -> tracing::Span {
    tracing::info_span!(
        "query",
        otel.name = name,
        tenant_hash = %tenant_hash.to_hex(),
        workload_class = crate::metrics::WorkloadClass::Interactive.name(),
        s3_requests = tracing::field::Empty,
        s3_bytes = tracing::field::Empty,
    )
}

fn record_span_cost(span: &tracing::Span, accounting: &QueryAccountingSnapshot) {
    span.record("s3_requests", accounting.total_s3_requests());
    span.record("s3_bytes", accounting.total_s3_bytes());
}

/// Nanosecond form of a millisecond timestamp for an audit window bound,
/// saturating rather than overflowing (the audit record's window is
/// informational; the query itself already validated its range).
fn ms_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}
