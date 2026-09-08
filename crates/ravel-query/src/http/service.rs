//! The query controls every query transport shares (ADR-1374 decision 3,
//! item 5).
//!
//! A query surface is two things: a transport (parse a request, encode a
//! response) and a set of controls that must run in one order around the
//! engine call. This module owns the controls; the transports own nothing but
//! parsing and encoding.
//!
//! The order every operation here follows is fixed:
//!
//! 1. acquire an admission permit from the fleet-global controller, before any
//!    resolve or GET;
//! 2. clamp the wall deadline and the request budgets against the server
//!    ceilings, lowering only;
//! 3. run the engine call;
//! 4. finalize usage through [`UsageGuard`] on every exit path, the dropped
//!    one included;
//! 5. submit the audit event and await its durability;
//! 6. apply the partial-coverage consent gate;
//! 7. map the outcome through the existing redaction.
//!
//! Usage is finalized at step 4, so an audit failure, a partial refusal, an
//! evaluation failure, and a deadline all record what the query spent before
//! the failure becomes a response. A cancellation (the caller's future
//! dropped) never reaches step 5: it produces no result, and its usage record
//! is its only trace. Nothing here spawns, so dropping the caller's future
//! cancels the engine call itself.
//!
//! `ravel-server`'s `service::QueryService` is the façade a transport calls;
//! it holds a [`QueryControls`] and delegates the five Prometheus-shaped
//! operations to the functions here, so the SQL, analytics, exemplars, PromQL,
//! and (issue #1381) MCP surfaces run one implementation of these controls
//! rather than five copies.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use ravel_maintain::{QueryAuditSink, QueryStatus, query_audit_event};
use ravel_promql::{Annotations, LabelMatcher, RangeValue, Value};
use ravel_types::accounting::{
    CostEstimate, QueryAccountingSnapshot, QueryCostRecorder, QueryWorkloadClass,
};
use ravel_types::{CommitToken, LabelSet, SeriesId, TenantHash, TimeRange};

use crate::engine::parse_match_selector;
use crate::http::error::{ApiError, MSG_AUDIT_UNAVAILABLE};
use crate::log_series;
use crate::request_budgets::RequestBudgets;
use crate::{
    Coverage, EngineConfig, QueryAdmissionController, QueryEngine, QueryPermit, QueryStats,
};

/// Resolve the caller's credentials to a tenant, the one authentication step
/// every query transport runs before it asks the service layer for anything.
///
/// Authentication is a transport concern and stays outside the operations
/// below: they take an already-authenticated [`TenantHash`]. It runs before
/// admission on purpose, so an anonymous caller cannot consume a permit from
/// the fleet-global concurrency ceiling.
pub fn authenticate(
    resolver: &dyn crate::http::tenant::TenantResolver,
    headers: &axum::http::HeaderMap,
) -> Result<TenantHash, ApiError> {
    Ok(resolver.resolve(headers)?.hash())
}

/// Client message for a query the fleet-global concurrency ceiling refused.
/// One constant so every transport's 503 body is the same string.
pub const MSG_CONCURRENCY: &str = "fleet query concurrency ceiling reached; retry";

/// How a query ended, for its usage record. Mirrors `ravel-server`'s
/// `QueryOutcomeStatus`, which is the aggregator this feeds and which lives in
/// the crate that owns `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageStatus {
    Success,
    Error,
    Timeout,
    Canceled,
}

/// The sink one finished query's usage record is folded into, tagged with how
/// the query ended.
///
/// Distinct from [`QueryCostRecorder`], which records the cost of a *completed*
/// query and carries no outcome: this one is written from a drop guard on every
/// exit path, so it also sees the cancelled, timed-out, and failed queries that
/// never produce a response.
pub trait QueryUsageSink: Send + Sync {
    fn record_usage(
        &self,
        tenant_hash: TenantHash,
        status: UsageStatus,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    );
}

/// Records nothing. The default for a caller with no `/metrics` aggregator.
pub struct NoopQueryUsageSink;

impl QueryUsageSink for NoopQueryUsageSink {
    fn record_usage(
        &self,
        _tenant_hash: TenantHash,
        _status: UsageStatus,
        _accounting: &QueryAccountingSnapshot,
        _estimate: &CostEstimate,
    ) {
    }
}

/// A running query's spend, readable at any instant.
///
/// The drop path has no outcome to read counters from, so it reads them here
/// instead: a query that fetched objects for two minutes and was then abandoned
/// records what it actually spent, not zeros. Implemented over `ravel-sql`'s
/// `LiveAccounting` for the SQL path and over a live `QueryAccounting` handle
/// for the exemplars path.
pub trait LiveUsage: Send + Sync {
    fn snapshot(&self) -> QueryAccountingSnapshot;
}

/// A live-usage handle for a call that exposes none. Its snapshot is all
/// zeros, so a cancelled query on that path records the cancellation itself
/// without claiming a spend it cannot measure.
///
/// The Prometheus-shaped engine entry points build and own their
/// `QueryAccounting` internally and return it only on success, so this is what
/// they use. Replacing it needs a live handle on `QueryEngine`, which is a
/// change to `engine.rs`.
pub struct UnobservedUsage;

impl LiveUsage for UnobservedUsage {
    fn snapshot(&self) -> QueryAccountingSnapshot {
        QueryAccountingSnapshot::default()
    }
}

/// Folds exactly one usage record for one query, on whichever path it exits.
///
/// [`UsageGuard::finish`] records the real outcome and consumes the guard, so a
/// second call is a compile error rather than a double fold. The only way not
/// to call it is the guard itself being dropped without the operation reaching
/// that line, which is the whole operation future being dropped mid-await:
/// `Drop::drop` runs unconditionally and folds a [`UsageStatus::Canceled`]
/// record whose cost is read live from [`LiveUsage`].
pub struct UsageGuard {
    sink: Arc<dyn QueryUsageSink>,
    tenant_hash: TenantHash,
    live: Arc<dyn LiveUsage>,
    finished: bool,
}

impl UsageGuard {
    pub fn new(
        sink: Arc<dyn QueryUsageSink>,
        tenant_hash: TenantHash,
        live: Arc<dyn LiveUsage>,
    ) -> Self {
        UsageGuard {
            sink,
            tenant_hash,
            live,
            finished: false,
        }
    }

    /// Record the real outcome. Consumes the guard.
    pub fn finish(
        mut self,
        status: UsageStatus,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    ) {
        self.sink
            .record_usage(self.tenant_hash, status, accounting, estimate);
        self.finished = true;
    }

    /// Record a failed outcome whose spend is only knowable from the live
    /// handle: the estimate is a success-path value, so it folds as zero.
    pub fn finish_failed(self, status: UsageStatus) {
        let snapshot = self.live.snapshot();
        self.finish(status, &snapshot, &zero_estimate());
    }
}

impl Drop for UsageGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.sink.record_usage(
                self.tenant_hash,
                UsageStatus::Canceled,
                &self.live.snapshot(),
                &zero_estimate(),
            );
        }
    }
}

/// The controls every query transport in the process shares: one admission
/// controller, one cost recorder, one usage sink, one audit sink.
///
/// It carries no engine and no config. The server ceilings a request is clamped
/// against are passed in per operation, because the SQL surface's ceilings come
/// from its own state rather than from a [`QueryEngine`] config, and one
/// controls value has to serve both.
#[derive(Clone)]
pub struct QueryControls {
    pub admission: Arc<QueryAdmissionController>,
    pub cost_recorder: Arc<dyn QueryCostRecorder>,
    pub usage_sink: Arc<dyn QueryUsageSink>,
    pub audit_sink: Arc<dyn QueryAuditSink>,
}

impl QueryControls {
    /// Step 1: a permit from the fleet-global ceiling (ADR-0061 decision 2),
    /// taken before any resolve or GET and released by its `Drop` on every
    /// exit, the dropped-future one included.
    pub fn admit(&self) -> Result<QueryPermit, ApiError> {
        self.admission
            .try_admit()
            .map_err(|_| ApiError::Unavailable(MSG_CONCURRENCY.to_string()))
    }

    /// Step 2, deadlines: a caller may only lower the server's wall deadline.
    pub fn clamp_deadline(&self, requested: Duration, ceiling: Duration) -> Duration {
        requested.min(ceiling)
    }

    /// Step 2, budgets: a caller may only lower the server's per-request
    /// budgets. The engine re-resolves the same clamp through
    /// [`RequestBudgets::clamp_optional`], which is idempotent, so clamping
    /// here makes the lowering visible at the service boundary without
    /// changing what the query runs under.
    pub fn clamp_budgets(
        &self,
        requested: Option<&RequestBudgets>,
        config: &EngineConfig,
    ) -> RequestBudgets {
        let effective = RequestBudgets::clamp_optional(requested, config);
        RequestBudgets {
            max_bytes_scanned: Some(effective.max_bytes_scanned),
            max_store_requests: Some(effective.max_store_requests),
            max_segments: Some(effective.max_segments),
        }
    }

    /// Step 4: the drop guard that folds this query's usage on every exit.
    pub fn usage_guard(&self, tenant_hash: TenantHash, live: Arc<dyn LiveUsage>) -> UsageGuard {
        UsageGuard::new(Arc::clone(&self.usage_sink), tenant_hash, live)
    }

    /// Step 5: submit one evidential audit event for a query that reached
    /// execution for a resolved tenant and await its durability (ADR-0062
    /// §2a).
    ///
    /// A submission failure (`audit_mode=required` surfacing a flush error, or
    /// a stopped pipeline) fails the request closed with a retryable 503 rather
    /// than releasing an unaudited answer. In best-effort mode the pipeline
    /// resolves the submission to `Ok` and the response is released. A request
    /// rejected before execution never calls this: there is no executed read to
    /// attribute.
    pub async fn audit(
        &self,
        tenant_hash: TenantHash,
        now_ns: i64,
        query_text: &str,
        language: &str,
        window: (i64, i64),
        status: QueryStatus,
    ) -> Result<(), ApiError> {
        let event = query_audit_event(
            &tenant_hash,
            now_ns,
            query_text,
            language,
            status,
            window.0,
            window.1,
        );
        self.audit_sink.submit(event).await.map_err(|err| {
            tracing::warn!(
                tenant = %tenant_hash.to_hex(),
                error = %err,
                language,
                "query audit submission failed; failing the request closed",
            );
            ApiError::Unavailable(MSG_AUDIT_UNAVAILABLE.to_string())
        })?;
        Ok(())
    }

    /// Step 6: the partial-coverage consent gate (ADR-0071 amendment
    /// decision 1). Partial coverage the caller did not opt into is refused
    /// with the typed 503 every query surface uses, so a consumer that never
    /// asked for a partial answer fails safe instead of silently accepting one.
    pub fn gate_partial(&self, coverage: &Coverage, allow_partial: bool) -> Result<(), ApiError> {
        if coverage.is_partial() && !allow_partial {
            return Err(ApiError::Unavailable(partial_refusal_message(
                coverage.skipped(),
            )));
        }
        Ok(())
    }

    /// Fold a completed query's cost into the `/metrics` aggregate
    /// (ADR-0044 section 4). Separate from [`Self::usage_guard`]: this is the
    /// pre-existing `ravel_query_*` family, fed only by a query that produced
    /// an answer.
    pub fn record_cost(
        &self,
        tenant_hash: TenantHash,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    ) {
        self.cost_recorder.record(
            accounting,
            estimate,
            tenant_hash,
            QueryWorkloadClass::Interactive,
        );
    }
}

/// The refusal message for partial coverage the caller did not opt into. It
/// names the degraded clusters (the same operator-facing, already-redacted
/// names the `warnings` array carries) and names `allow_partial` as the
/// remedy, so the fix is in the failure itself.
pub fn partial_refusal_message(skipped: &[String]) -> String {
    let clusters = if skipped.is_empty() {
        "one or more federated clusters were skipped".to_string()
    } else {
        skipped.join("; ")
    };
    format!(
        "partial results: coverage is incomplete ({clusters}); \
         set allow_partial=true to receive partial results"
    )
}

/// Builds a [`Coverage`] from the metadata path's accumulated partial flag and
/// its skipped-cluster warnings. The value-bearing operations derive coverage
/// from a [`QueryStats`] via [`Coverage::from_stats`]; the metadata operations
/// carry no `QueryStats` on their envelope, so they thread the flag and
/// warnings through directly and reconstruct the same shape here.
fn coverage_of(partial: bool, skipped: &[String]) -> Coverage {
    if partial {
        Coverage::Partial {
            skipped: skipped.to_vec(),
        }
    } else {
        Coverage::Complete
    }
}

/// Nanosecond form of a millisecond timestamp for an audit window bound,
/// saturating rather than overflowing (the audit record is best-effort
/// informational for the window; the query itself already validated its
/// range).
fn ms_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

/// One instant PromQL query, already parsed and authenticated.
#[derive(Debug, Clone)]
pub struct InstantRequest {
    pub query: String,
    pub time_ms: i64,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
    pub allow_partial: bool,
    pub now_ns: i64,
    pub budgets: Option<RequestBudgets>,
}

/// One range PromQL query, already parsed and authenticated.
#[derive(Debug, Clone)]
pub struct RangeRequest {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
    pub allow_partial: bool,
    pub now_ns: i64,
    pub budgets: Option<RequestBudgets>,
}

/// One metadata query (`labels`, `label_values`, `series`), already parsed and
/// authenticated.
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub selectors: Vec<String>,
    pub window: TimeRange,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
    pub allow_partial: bool,
    pub now_ns: i64,
    pub budgets: Option<RequestBudgets>,
}

/// The answer to an instant query, with its coverage already consented to.
pub struct InstantOutcome {
    pub value: Value,
    pub stats: QueryStats,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub infos: Vec<String>,
}

/// The answer to a range query, with its coverage already consented to.
pub struct RangeOutcome {
    pub value: RangeValue,
    pub stats: QueryStats,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub infos: Vec<String>,
}

/// The matched series behind a metadata query, with its coverage already
/// consented to.
pub struct MetadataOutcome {
    pub series: Vec<(SeriesId, LabelSet)>,
    pub partial: bool,
    pub warnings: Vec<String>,
}

/// The label names a `labels` query matched.
pub struct LabelsOutcome {
    pub names: Vec<String>,
    pub partial: bool,
    pub warnings: Vec<String>,
}

/// The label values a `label_values` query matched.
pub struct LabelValuesOutcome {
    pub values: Vec<String>,
    pub partial: bool,
    pub warnings: Vec<String>,
}

/// `/api/v1/query`: one instant PromQL evaluation under the shared controls.
pub async fn promql_instant(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &InstantRequest,
) -> Result<InstantOutcome, ApiError> {
    let _permit = controls.admit()?;
    let deadline = controls.clamp_deadline(request.deadline, engine.config().deadline);
    let budgets = controls.clamp_budgets(request.budgets.as_ref(), engine.config());
    let guard = controls.usage_guard(tenant_hash, Arc::new(UnobservedUsage));

    let exec = engine
        .instant_with_budgets(
            tenant_hash,
            &request.query,
            request.time_ms,
            &request.min_tokens,
            request.now_ns,
            deadline,
            Some(&budgets),
        )
        .await
        .map_err(ApiError::from);

    let status = finish_usage(
        guard,
        &exec,
        |(_, _, stats): &(Value, Annotations, QueryStats)| (stats.accounting, stats.estimate),
    );

    let time_ns = ms_to_ns(request.time_ms);
    controls
        .audit(
            tenant_hash,
            request.now_ns,
            &request.query,
            "promql",
            (time_ns, time_ns),
            status,
        )
        .await?;
    let (value, annotations, stats) = exec?;

    let coverage = Coverage::from_stats(&stats);
    controls.gate_partial(&coverage, request.allow_partial)?;
    let (warnings, infos) = merge_annotations(annotations, &stats);
    controls.record_cost(tenant_hash, &stats.accounting, &stats.estimate);
    Ok(InstantOutcome {
        value,
        partial: coverage.is_partial(),
        stats,
        warnings,
        infos,
    })
}

/// `/api/v1/query_range`: one range PromQL evaluation under the shared
/// controls.
pub async fn promql_range(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &RangeRequest,
) -> Result<RangeOutcome, ApiError> {
    let _permit = controls.admit()?;
    let deadline = controls.clamp_deadline(request.deadline, engine.config().deadline);
    let budgets = controls.clamp_budgets(request.budgets.as_ref(), engine.config());
    let guard = controls.usage_guard(tenant_hash, Arc::new(UnobservedUsage));

    let exec = engine
        .range_hist_with_budgets(
            tenant_hash,
            &request.query,
            request.start_ms,
            request.end_ms,
            request.step_ms,
            &request.min_tokens,
            request.now_ns,
            deadline,
            Some(&budgets),
        )
        .await
        .map_err(ApiError::from);

    let status = finish_usage(
        guard,
        &exec,
        |(_, _, stats): &(RangeValue, Annotations, QueryStats)| (stats.accounting, stats.estimate),
    );

    controls
        .audit(
            tenant_hash,
            request.now_ns,
            &request.query,
            "promql",
            (ms_to_ns(request.start_ms), ms_to_ns(request.end_ms)),
            status,
        )
        .await?;
    let (value, annotations, stats) = exec?;

    let coverage = Coverage::from_stats(&stats);
    controls.gate_partial(&coverage, request.allow_partial)?;
    let (warnings, infos) = merge_annotations(annotations, &stats);
    controls.record_cost(tenant_hash, &stats.accounting, &stats.estimate);
    Ok(RangeOutcome {
        value,
        partial: coverage.is_partial(),
        stats,
        warnings,
        infos,
    })
}

/// `/api/v1/labels`: the label names carried by the matched series.
pub async fn labels(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &MetadataRequest,
) -> Result<LabelsOutcome, ApiError> {
    let outcome = metadata(controls, engine, tenant_hash, request, "labels").await?;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, labels) in &outcome.series {
        for label in labels.iter() {
            names.insert(label.name.clone());
        }
    }
    Ok(LabelsOutcome {
        names: names.into_iter().collect(),
        partial: outcome.partial,
        warnings: outcome.warnings,
    })
}

/// `/api/v1/label/{name}/values`: the values of `name` across the matched
/// series.
///
/// `include_log_metric_names` is the caller's already-parsed answer to ADR-1103:
/// the two reserved log metric names appear in no stored postings, so they only
/// surface when the request's selectors ask about the logs signal at all.
pub async fn label_values(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &MetadataRequest,
    name: &str,
    include_log_metric_names: bool,
) -> Result<LabelValuesOutcome, ApiError> {
    let outcome = metadata(controls, engine, tenant_hash, request, "labels").await?;
    let mut values: BTreeSet<String> = BTreeSet::new();
    for (_, labels) in &outcome.series {
        if let Some(v) = labels.get(name) {
            values.insert(v.to_string());
        }
    }
    if include_log_metric_names {
        values.insert(log_series::LOG_LINES_METRIC.to_string());
        values.insert(log_series::LOG_BYTES_METRIC.to_string());
    }
    Ok(LabelValuesOutcome {
        values: values.into_iter().collect(),
        partial: outcome.partial,
        warnings: outcome.warnings,
    })
}

/// `/api/v1/series`: the matched series themselves.
pub async fn series(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &MetadataRequest,
) -> Result<MetadataOutcome, ApiError> {
    metadata(controls, engine, tenant_hash, request, "series").await
}

/// The one metadata read behind `labels`, `label_values`, and `series`, under
/// the shared controls. `language` distinguishes the surface on the audit
/// record so the record shape stays one schema.
async fn metadata(
    controls: &QueryControls,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &MetadataRequest,
    language: &str,
) -> Result<MetadataOutcome, ApiError> {
    let _permit = controls.admit()?;
    let deadline = controls.clamp_deadline(request.deadline, engine.config().deadline);
    let budgets = controls.clamp_budgets(request.budgets.as_ref(), engine.config());
    let guard = controls.usage_guard(tenant_hash, Arc::new(UnobservedUsage));

    let resolved = resolve_matched_series(engine, tenant_hash, request, deadline, &budgets).await;
    let status = finish_usage(guard, &resolved, |resolved: &ResolvedSeries| {
        (resolved.accounting, resolved.estimate)
    });

    let selectors_text = request.selectors.join("; ");
    controls
        .audit(
            tenant_hash,
            request.now_ns,
            &selectors_text,
            language,
            (request.window.start_ns, request.window.end_ns),
            status,
        )
        .await?;
    let resolved = resolved?;

    let coverage = coverage_of(resolved.partial, &resolved.warnings);
    controls.gate_partial(&coverage, request.allow_partial)?;
    controls.record_cost(tenant_hash, &resolved.accounting, &resolved.estimate);
    Ok(MetadataOutcome {
        series: resolved.series,
        partial: resolved.partial,
        warnings: resolved.warnings,
    })
}

/// One metadata request's matched series and its summed cost.
struct ResolvedSeries {
    series: Vec<(SeriesId, LabelSet)>,
    partial: bool,
    warnings: Vec<String>,
    /// The field-wise sum of every selector's snapshot: one metadata request is
    /// one query, so the whole request folds into the aggregator once.
    accounting: QueryAccountingSnapshot,
    estimate: CostEstimate,
}

async fn resolve_matched_series(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    request: &MetadataRequest,
    deadline: Duration,
    budgets: &RequestBudgets,
) -> Result<ResolvedSeries, ApiError> {
    // The wall deadline is a per-query budget (docs/query-engine.md
    // "Budgets"), and one metadata request is one query. Convert the duration
    // into a single absolute instant computed once here, then hand each
    // resolve_series call only the time still remaining. Without this, each
    // match[] selector would be granted the full `deadline` afresh, so N
    // selectors would get N times the documented budget with no aggregate cap.
    let request_deadline = tokio::time::Instant::now() + deadline;

    let mut combined: Option<(QueryAccountingSnapshot, CostEstimate)> = None;
    // Cross-cluster federation partial-coverage warnings (ADR-0071),
    // accumulated across selectors and deduplicated so the client sees one
    // warning per skipped cluster regardless of how many selectors it sent.
    let mut warnings: Vec<String> = Vec::new();
    // The whole request is partial if ANY selector's federated resolve skipped
    // a remote.
    let mut partial = false;

    if request.selectors.is_empty() {
        let remaining = remaining_budget(request_deadline, deadline)?;
        let (series, stats) = engine
            .resolve_series_with_budgets(
                tenant_hash,
                &[],
                request.window,
                &request.min_tokens,
                request.now_ns,
                remaining,
                Some(budgets),
            )
            .await?;
        accumulate_cost(&mut combined, &stats);
        partial |= stats.partial;
        for w in stats.warnings {
            if !warnings.contains(&w) {
                warnings.push(w);
            }
        }
        let (accounting, estimate) =
            combined.unwrap_or((QueryAccountingSnapshot::default(), zero_estimate()));
        return Ok(ResolvedSeries {
            series,
            partial,
            warnings,
            accounting,
            estimate,
        });
    }

    // Deliberate deviation: each match[] selector resolves its own snapshot
    // independently, rather than one shared snapshot for the whole request. The
    // shared wall budget above is orthogonal to that: snapshots stay
    // per-selector, but all selectors draw down one deadline.
    //
    // Per-selector segment stats are not surfaced on this path: the
    // labels/label_values/series endpoints have no established response
    // envelope for it (only the value-bearing endpoints do), and aggregating
    // per-selector counts here would double count segments any two selectors
    // both matched.
    let mut by_id: HashMap<SeriesId, LabelSet> = HashMap::new();
    for selector in &request.selectors {
        let matchers: Vec<LabelMatcher> = parse_match_selector(selector)?;
        let remaining = remaining_budget(request_deadline, deadline)?;
        let (series, stats) = engine
            .resolve_series_with_budgets(
                tenant_hash,
                &matchers,
                request.window,
                &request.min_tokens,
                request.now_ns,
                remaining,
                Some(budgets),
            )
            .await?;
        accumulate_cost(&mut combined, &stats);
        partial |= stats.partial;
        for w in stats.warnings {
            if !warnings.contains(&w) {
                warnings.push(w);
            }
        }
        for (id, labels) in series {
            by_id.entry(id).or_insert(labels);
        }
    }
    let (accounting, estimate) =
        combined.unwrap_or((QueryAccountingSnapshot::default(), zero_estimate()));
    Ok(ResolvedSeries {
        series: by_id.into_iter().collect(),
        partial,
        warnings,
        accounting,
        estimate,
    })
}

/// Fold one sub-query's [`QueryStats`] into the request's running cost total.
/// The counters sum and the estimate sums; the first call seeds the total
/// because [`CostEstimate`] has no zero value (an estimate is only ever a real
/// resolved snapshot's, ADR-0044).
fn accumulate_cost(
    combined: &mut Option<(QueryAccountingSnapshot, CostEstimate)>,
    stats: &QueryStats,
) {
    *combined = Some(match combined.take() {
        None => (stats.accounting, stats.estimate),
        Some((accounting, estimate)) => (
            accounting.saturating_add(&stats.accounting),
            estimate.saturating_add(&stats.estimate),
        ),
    });
}

/// Time left in the shared request wall budget, or a `DeadlineExceeded` error
/// once it is spent. `configured` is the whole-request budget and is reported
/// in the error so the client sees the query's deadline, not the residual slice
/// handed to the last selector.
fn remaining_budget(
    request_deadline: tokio::time::Instant,
    configured: Duration,
) -> Result<Duration, ApiError> {
    let remaining = request_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(crate::QueryError::DeadlineExceeded {
            deadline: configured,
        }
        .into());
    }
    Ok(remaining)
}

/// Merge the federation fan-out's partial-coverage warnings (ADR-0071) into the
/// top-level `warnings` array alongside the evaluator's own annotations, so a
/// `skip_unavailable=true` federated query returns a response a client can tell
/// is partial and that names the skipped cluster. These are already redacted at
/// the federation seam: they name the operator-facing cluster only, never its
/// endpoint or transport error text.
fn merge_annotations(annotations: Annotations, stats: &QueryStats) -> (Vec<String>, Vec<String>) {
    let (mut warnings, infos) = annotations.into_parts();
    for w in &stats.warnings {
        if !warnings.contains(w) {
            warnings.push(w.clone());
        }
    }
    (warnings, infos)
}

/// Step 4 for one operation: fold the usage record before the outcome is turned
/// into anything else, and report the audit status the same outcome implies.
///
/// `cost_of` reads the success value's counters; a failure has none, so its
/// spend comes from the guard's live handle instead. Every caller runs this
/// before its `audit`, its `?`, and its consent gate, which is what makes "the
/// usage record exists even when the request ends as an error" mechanical
/// rather than remembered.
fn finish_usage<T>(
    guard: UsageGuard,
    result: &Result<T, ApiError>,
    cost_of: impl FnOnce(&T) -> (QueryAccountingSnapshot, CostEstimate),
) -> QueryStatus {
    match result {
        Ok(value) => {
            let (accounting, estimate) = cost_of(value);
            guard.finish(UsageStatus::Success, &accounting, &estimate);
            QueryStatus::Ok
        }
        Err(err) => {
            guard.finish_failed(usage_status_of(err));
            QueryStatus::Error
        }
    }
}

/// The usage status a failed operation records. A wall-deadline trip is a
/// [`UsageStatus::Timeout`]; every other failure is a [`UsageStatus::Error`].
/// The cost recorded alongside it is read from the live handle, not derived
/// from the error, so a deadline carries the requests and bytes the query
/// actually issued.
pub fn usage_status_of(err: &ApiError) -> UsageStatus {
    match err {
        ApiError::Timeout(_) => UsageStatus::Timeout,
        _ => UsageStatus::Error,
    }
}

/// The zero cost estimate. [`CostEstimate`] has no zero value of its own: an
/// estimate is only ever a real resolved snapshot's (ADR-0044), so a path with
/// no successful resolve folds this instead.
pub fn zero_estimate() -> CostEstimate {
    CostEstimate::new(0, 0, 0, 0, 0)
}
