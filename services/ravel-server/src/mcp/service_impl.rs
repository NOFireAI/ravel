//! The [`QueryBackend`] port over [`QueryService`].
//!
//! Every method here is one call into the query service and one conversion of
//! its outcome into a D4 envelope, except the two SQL methods and
//! `describe_data`: `sql_execute` stops before the caps and returns a
//! [`Draft`] beside the snapshot's pin inputs, `sql_explain` returns an
//! [`ExplainOutcome`] for the explain tool to render, and `describe_data`
//! maps the service's `DescribeData` field by field into a
//! [`DescribeOutcome`], because only `ravel_describe_data` knows whether it
//! is serving a first page or a cursor's later one. `labels` and `label_values` also stop before the caps,
//! because `ravel_find_labels` filters the list and the caps run after the
//! filter (ADR-1374 amendment 2026-09-12 item 6: the tool layer owns
//! envelope finishing), and so do `promql_instant` and `promql_range`,
//! because `ravel_query_promql` groups the per-sample rows into one row per
//! series and caps series, not samples. `analytics` also stops before the
//! caps, after it runs the op over each series of the range result: its rows
//! are already one per series, and `ravel_analyze_timeseries` caps them and
//! labels the result a heuristic. `trace_lookup` stops before the caps too,
//! because `ravel_get_trace` labels each span's parent over the rows that
//! survive the byte cap, and only the tool can order those two steps.
//! Nothing else: the admission permit, the
//! deadline clamp, the cost record, the usage guard, the audit submission, the
//! partial-coverage gate, and the error redaction all live inside the service
//! layer. Each service call takes its own permit and releases it when it
//! returns. Every tool makes one service call except `ravel_search_logs` with
//! predicates, which reads the declared columns before its statement and so
//! takes two permits one after the other, never two at once (ADR-1374
//! decision 7 as amended 2026-11-03). Admission never waits: the service
//! layer's `try_admit` rejects at the ceiling. So taking a permit here as
//! well would hold it while the service layer asks for its own, and on a
//! deployment whose ceiling is 1 every tool call would fail as
//! `unavailable`.
//!
//! One deadline bounds the whole tool call. [`ServiceBackend::with_call_start`]
//! fixes it at the call's start, and each service call runs under what
//! remains of it, so a second call gets only what the first left; a call
//! that would start with nothing left fails as `deadline` without running or
//! taking a permit.
//!
//! Nothing here spawns: the service call is awaited on the same task the tool
//! future runs on, so cancelling that future cancels the engine call and the
//! usage guard inside the service bills what it had already spent. Which
//! client actions reach that future is a per-revision property of rmcp's
//! transport, stated in [`super`].
//!
//! Two figures the service outcomes do not carry are absent rather than
//! invented. The metadata operations (`labels`, `label_values`, `series`)
//! return no accounting snapshot, so their envelopes report the ceilings they
//! ran under and no `actual` block; and no outcome but SQL's carries the
//! resolved snapshot id or watermark hour, so `visibility` reports only the
//! commit tokens the caller required. A zero in either place would read as a
//! measurement. A SQL [`Draft`] writes the watermark and leaves the snapshot
//! id to [`Draft::finish_page`], which derives it from the tool's pin scope.
//! D4 types the identity fields as strings, so the unmeasured
//! ones cannot be left out of the JSON at all: [`super::envelope::finish`]
//! names each of them in `warnings` instead, and an empty string there is
//! never a value the caller has to interpret.

use std::time::Duration;

use ravel_analytics::{AnalyticsError, ChangeKind, ChangePointParams, SummaryParams};
use ravel_catalog::MetricKind;
use ravel_mcp::budget::McpEffectiveBudgets;
use ravel_mcp::catalog::AnalyzeOp;
use ravel_mcp::draft::{Draft, EstimateComponent, ExplainColumn, ExplainOutcome, cursor_signal};
use ravel_mcp::envelope::{AnyJson, Cell, Column, Data, Envelope, Failure, FailureClass, Row};
use ravel_mcp::service::{
    DescribeOutcome, DescribeRequest, DescribedColumn, DescribedCoverage, DescribedFamily,
    QueryBackend, TraceLookupRequest,
};
use ravel_mcp::tools::analyze_timeseries::{
    AnalyticsCall, CHANGE_POINT_MIN_POINTS, SUMMARY_MIN_POINTS,
};
use ravel_promql::{RangeMatrix, RangeValue, Value as PromValue};
use ravel_query::http::{InstantRequest, MetadataRequest, RangeRequest};
use ravel_query::{PhaseAccountingSnapshot, QueryPhase, QueryStats, RequestBudgets};
use ravel_types::{CommitToken, Signal, TenantHash};
use rmcp::model::{ProgressNotificationParam, ProgressToken};
use rmcp::service::{Peer, RoleServer};
use serde::Deserialize;

use crate::analytics;
use crate::service::describe::{DescribeColumn, DescribeDataRequest, IndexedKeysSource};
use crate::service::{AnalyticsRequest, ExemplarsRequest, QueryService, ServiceError};
use crate::traces::TraceLookup;

use super::envelope::{self as d4, Spend};

/// The three phase boundaries a progress-tracking client is told about
/// (ADR-1374 decision 7).
const PROGRESS_PHASES: [QueryPhase; 3] = [QueryPhase::Resolve, QueryPhase::Plan, QueryPhase::Scan];

/// The MCP tool layer's view of one query surface.
///
/// Built per request, because two of the three things it holds are
/// per-request: the budgets the call clamped to, and the progress reporter
/// the client's `progressToken` (if any) named.
pub struct ServiceBackend {
    service: QueryService,
    budgets: McpEffectiveBudgets,
    progress: Option<ProgressReporter>,
    call_deadline: Option<tokio::time::Instant>,
    audit_call: Option<(String, String)>,
}

impl ServiceBackend {
    /// A backend whose every service call gets the whole effective deadline
    /// from its own start. The adapter adds [`Self::with_call_start`], which
    /// bounds a tool call's service calls together.
    pub fn new(service: QueryService, budgets: McpEffectiveBudgets) -> Self {
        ServiceBackend {
            service,
            budgets,
            progress: None,
            call_deadline: None,
            audit_call: None,
        }
    }

    /// Audit every service call this backend makes as one call of `tool`:
    /// `query.language = mcp:<tool>` and `query.id = query_id` on each
    /// record (ADR-1374 decision 3 item 6). A tool that makes two service
    /// calls writes two records under the one id.
    pub fn with_audit_call(mut self, tool: &str, query_id: &str) -> Self {
        self.audit_call = Some((format!("mcp:{tool}"), query_id.to_string()));
        self
    }

    /// The service the next call runs on, with this backend's audit call
    /// stamped on it, and `text` replacing the engine's audit text when set.
    fn audited(&self, text: Option<String>) -> QueryService {
        match &self.audit_call {
            Some((language, query_id)) => {
                self.service
                    .clone()
                    .with_audit_call(ravel_maintain::AuditCall {
                        language: language.clone(),
                        query_id: query_id.clone(),
                        text,
                    })
            }
            None => self.service.clone(),
        }
    }

    /// Bound every service call this backend makes by one deadline, the
    /// effective deadline measured from `started`, the tool call's start.
    /// A later call gets only what an earlier one left, and a call that
    /// would start with nothing left fails as `deadline` without running.
    pub fn with_call_start(mut self, started: tokio::time::Instant) -> Self {
        self.call_deadline = started.checked_add(self.budgets.deadline);
        self
    }

    /// The deadline the next service call runs under: what remains of the
    /// tool call's deadline, never above the effective ceiling.
    fn remaining(&self) -> Result<Duration, Failure> {
        let Some(at) = self.call_deadline else {
            return Ok(self.budgets.deadline);
        };
        let left = at.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return Err(Failure {
                class: FailureClass::Deadline,
                message: format!("query exceeded its deadline of {:?}", self.budgets.deadline),
                counter: None,
            });
        }
        Ok(left.min(self.budgets.deadline))
    }

    /// `request` with its deadline lowered to what the tool call has left.
    fn bounded<T: Clone>(
        &self,
        request: &T,
        deadline: impl FnOnce(&mut T) -> &mut Duration,
    ) -> Result<T, Failure> {
        let remaining = self.remaining()?;
        let mut bounded = request.clone();
        let field = deadline(&mut bounded);
        *field = (*field).min(remaining);
        Ok(bounded)
    }

    /// Report phase progress to the client that supplied a `progressToken`.
    /// Without this, no notification is ever sent: an unrequested
    /// notification stream is traffic the client did not ask for.
    pub fn with_progress(mut self, progress: Option<ProgressReporter>) -> Self {
        self.progress = progress;
        self
    }

    /// Notify the three D7 phase boundaries, with each phase's own requests
    /// and wire bytes.
    ///
    /// The figures come from the completed operation's per-phase accounting,
    /// which is where they first exist: the phases run inside one engine call
    /// that reports its split at the end, and no notification may be awaited
    /// from a spawned task (a spawn between the transport and the engine would
    /// survive the client's cancellation, which decision 7 forbids). So the
    /// boundaries are reported in order once the figures are real, rather than
    /// predicted while they are not.
    async fn report_phases(&self, phases: &PhaseAccountingSnapshot) {
        let Some(progress) = &self.progress else {
            return;
        };
        let figures = d4::phase_figures(phases);
        let mut step = 0.0;
        for (phase, requests, wire_bytes) in figures {
            if !PROGRESS_PHASES.contains(&phase) {
                continue;
            }
            step += 1.0;
            progress
                .notify(
                    step,
                    PROGRESS_PHASES.len() as f64,
                    format!(
                        "{}: {requests} store requests, {wire_bytes} wire bytes",
                        phase.name()
                    ),
                )
                .await;
        }
    }

    /// The envelope of an operation whose outcome carries a full
    /// [`QueryStats`]: its spend, its per-phase split, and its coverage,
    /// before the caps, for a tool that reshapes the rows before it caps
    /// them. See [`stats_unbounded_components`] for which estimate components
    /// it names unbounded.
    fn uncapped_stats_envelope(
        &self,
        mut envelope: Envelope,
        stats: &QueryStats,
        partial: bool,
        warnings: Vec<String>,
    ) -> Envelope {
        d4::coverage(&mut envelope, partial, warnings);
        d4::spend(
            &mut envelope,
            &self.budgets,
            &Spend {
                accounting: &stats.accounting,
                estimate: &stats.estimate,
                phases: Some(&stats.phase_accounting),
                unbounded: stats_unbounded_components(stats),
            },
        );
        envelope
    }

    /// One PromQL result value, as this envelope's data block. A scalar or a
    /// string result is one row; a vector or a matrix is one row per element.
    fn value_data(value: PromValue) -> ravel_mcp::envelope::Data {
        match value {
            PromValue::Scalar(scalar) => d4::scalar_data(Cell::Float(scalar), "double"),
            PromValue::String(text) => d4::scalar_data(Cell::Str(text), "string"),
            PromValue::Vector(vector) => d4::instant_vector_data(vector),
            PromValue::Matrix(matrix) => d4::matrix_data(matrix),
        }
    }
}

/// A client's `progressToken` and the peer to send notifications to.
///
/// Cloneable and cheap: the peer is a handle, not a connection.
#[derive(Clone)]
pub struct ProgressReporter {
    peer: Peer<RoleServer>,
    token: ProgressToken,
}

impl ProgressReporter {
    pub fn new(peer: Peer<RoleServer>, token: ProgressToken) -> Self {
        ProgressReporter { peer, token }
    }

    /// Send one progress notification. A notification that cannot be
    /// delivered is logged and dropped: the client's stream is gone or its
    /// buffer is full, and neither is a reason to fail a query that already
    /// ran.
    async fn notify(&self, progress: f64, total: f64, message: String) {
        let mut param = ProgressNotificationParam::new(self.token.clone(), progress);
        param.total = Some(total);
        param.message = Some(message);
        if let Err(error) = self.peer.notify_progress(param).await {
            tracing::debug!(%error, "mcp progress notification not delivered");
        }
    }
}

/// ADR-0028 decision 4's per-call series cap, the bound and the text `POST
/// /api/v1/analytics` applies before any op runs. More is refused as
/// `budget_exceeded`, never truncated.
fn analytics_series_cap(series: usize) -> Result<(), Failure> {
    if series > analytics::MAX_SERIES {
        return Err(Failure {
            class: FailureClass::BudgetExceeded,
            message: analytics::series_cap_message(series),
            counter: Some("series".to_string()),
        });
    }
    Ok(())
}

/// The exemplar query's request, as it crosses the port: the caller-visible
/// shape of [`ExemplarsRequest`], whose own type is not reachable from
/// `ravel-mcp`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExemplarsArgs {
    query: String,
    start_ns: i64,
    end_ns: i64,
    #[serde(default)]
    min_tokens: Vec<String>,
    #[serde(default)]
    deadline_ms: Option<u64>,
}

/// The commit tokens a caller required, decoded. An undecodable token is the
/// caller's mistake and is named as such rather than dropped: silently
/// ignoring it would serve a snapshot that does not include the write the
/// caller was waiting for.
fn decode_tokens(encoded: &[String]) -> Result<Vec<CommitToken>, Failure> {
    encoded
        .iter()
        .map(|token| {
            CommitToken::decode(token)
                .map_err(|error| d4::invalid_argument(format!("min_tokens: {error}")))
        })
        .collect()
}

/// The deadline of one operation: what the caller asked for, never above the
/// effective ceiling the call already clamped to.
fn deadline(requested_ms: Option<u64>, ceiling: Duration) -> Duration {
    match requested_ms {
        Some(ms) => Duration::from_millis(ms).min(ceiling),
        None => ceiling,
    }
}

/// One row per series of `op`'s output, in [`AnalyzeOp::columns`] order.
///
/// `change_point` always downsamples a series over the 2000-point cap, so a
/// long series is a row rather than a refusal. A series below the op's
/// minimum point count is a row with `too_few_points: true` and null
/// figures, not a failed call. The op reads each sample's `ts_ns`, which the
/// evaluator already emits in nanoseconds.
fn analysis_data(op: AnalyzeOp, matrix: RangeMatrix) -> Result<Data, Failure> {
    let rows = matrix
        .into_iter()
        .map(|(labels, samples)| {
            let points: Vec<(i64, f64)> = samples
                .into_iter()
                .map(|sample| (sample.ts_ns, sample.value))
                .collect();
            let series = d4::labels_cell(&labels);
            match op {
                AnalyzeOp::ChangePoint => change_point_row(series, &points),
                AnalyzeOp::Summary => summary_row(series, &points),
            }
        })
        .collect::<Result<Vec<Row>, Failure>>()?;
    let types: &[&str] = match op {
        AnalyzeOp::ChangePoint => &[
            "json",
            "string",
            "timestamp",
            "double",
            "int64",
            "int64",
            "bool",
            "int64",
            "bool",
        ],
        AnalyzeOp::Summary => &[
            "json", "double", "double", "double", "double", "int64", "int64", "bool", "int64",
            "bool",
        ],
    };
    Ok(Data {
        columns: op
            .columns()
            .iter()
            .zip(types)
            .map(|(name, r#type)| Column {
                name: (*name).to_string(),
                r#type: (*r#type).to_string(),
            })
            .collect(),
        row_count: rows.len() as u64,
        rows,
    })
}

fn change_point_row(series: Cell, points: &[(i64, f64)]) -> Result<Row, Failure> {
    let min_points = Cell::Int(i128::from(CHANGE_POINT_MIN_POINTS));
    let result =
        match ravel_analytics::change_point(points, &ChangePointParams { downsample: true }) {
            Ok(result) => result,
            Err(AnalyticsError::EmptySeries) => {
                return Ok(vec![
                    series,
                    Cell::Str(change_kind(ChangeKind::Indeterminable).to_string()),
                    Cell::Null,
                    Cell::Null,
                    Cell::Int(0),
                    Cell::Int(0),
                    Cell::Bool(false),
                    min_points,
                    Cell::Bool(true),
                ]);
            }
            Err(error) => return Err(d4::internal(format!("change_point: {error}"))),
        };
    let too_few = (result.original_points as u64) < CHANGE_POINT_MIN_POINTS;
    Ok(vec![
        series,
        Cell::Str(change_kind(result.kind).to_string()),
        result.ts_ns.map_or(Cell::Null, Cell::Timestamp),
        if too_few {
            Cell::Null
        } else {
            Cell::Float(result.score)
        },
        Cell::Int(result.original_points as i128),
        Cell::Int(result.nan_excluded as i128),
        Cell::Bool(result.downsampled),
        min_points,
        Cell::Bool(too_few),
    ])
}

fn summary_row(series: Cell, points: &[(i64, f64)]) -> Result<Row, Failure> {
    let min_points = Cell::Int(i128::from(SUMMARY_MIN_POINTS));
    let (figures, counted, nan_excluded) = match ravel_analytics::summary(
        points,
        &SummaryParams {
            percentiles: Vec::new(),
        },
    ) {
        Ok(result) => {
            let counted = points.len().saturating_sub(result.nan_excluded);
            let figures = [result.median, result.mad, result.stddev, result.variance];
            (Some(figures), counted, result.nan_excluded)
        }
        Err(AnalyticsError::EmptySeries) => (None, 0, 0),
        Err(error) => return Err(d4::internal(format!("summary: {error}"))),
    };
    let too_few = (counted as u64) < SUMMARY_MIN_POINTS;
    let mut row = vec![series];
    match figures {
        Some(figures) if !too_few => row.extend(figures.map(Cell::Float)),
        _ => row.extend([Cell::Null, Cell::Null, Cell::Null, Cell::Null]),
    }
    row.extend([
        Cell::Int(counted as i128),
        Cell::Int(nan_excluded as i128),
        Cell::Bool(false),
        min_points,
        Cell::Bool(too_few),
    ]);
    Ok(row)
}

/// The kind's name, spelled as `POST /api/v1/analytics` serializes it.
fn change_kind(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Spike => "spike",
        ChangeKind::Dip => "dip",
        ChangeKind::StepChange => "step_change",
        ChangeKind::TrendChange => "trend_change",
        ChangeKind::DistributionChange => "distribution_change",
        ChangeKind::Stationary => "stationary",
        ChangeKind::Indeterminable => "indeterminable",
    }
}

fn args_of<T: for<'de> Deserialize<'de>>(request: &AnyJson, tool: &str) -> Result<T, Failure> {
    serde_json::from_value(request.0.clone())
        .map_err(|error| d4::invalid_argument(format!("{tool} arguments: {error}")))
}

impl QueryBackend for ServiceBackend {
    async fn promql_instant(
        &self,
        tenant_hash: TenantHash,
        request: &InstantRequest,
    ) -> Result<Envelope, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(None)
            .promql_instant(tenant_hash, request)
            .await
            .map_err(failure)?;
        self.report_phases(&outcome.stats.phase_accounting).await;

        let mut envelope = d4::base("metrics", "metrics", &self.budgets);
        let instant_ns = request.time_ms.saturating_mul(1_000_000);
        d4::window(&mut envelope, instant_ns, instant_ns);
        d4::min_tokens(&mut envelope, &request.min_tokens);
        envelope.scope.predicates_applied = vec![request.query.clone()];
        envelope.data = Self::value_data(outcome.value);
        let mut warnings = outcome.warnings;
        warnings.extend(outcome.infos);
        Ok(self.uncapped_stats_envelope(envelope, &outcome.stats, outcome.partial, warnings))
    }

    async fn promql_range(
        &self,
        tenant_hash: TenantHash,
        request: &RangeRequest,
    ) -> Result<Envelope, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(None)
            .promql_range(tenant_hash, request)
            .await
            .map_err(failure)?;
        self.report_phases(&outcome.stats.phase_accounting).await;

        let mut envelope = d4::base("metrics", "metrics", &self.budgets);
        d4::window(
            &mut envelope,
            request.start_ms.saturating_mul(1_000_000),
            request.end_ms.saturating_mul(1_000_000),
        );
        d4::min_tokens(&mut envelope, &request.min_tokens);
        envelope.scope.predicates_applied = vec![request.query.clone()];
        envelope.data = match outcome.value {
            RangeValue::Scalar(scalar) => d4::scalar_data(Cell::Float(scalar), "double"),
            RangeValue::String(text) => d4::scalar_data(Cell::Str(text), "string"),
            RangeValue::Matrix(matrix) => d4::histogram_aware_matrix_data(matrix),
        };
        let mut warnings = outcome.warnings;
        warnings.extend(outcome.infos);
        Ok(self.uncapped_stats_envelope(envelope, &outcome.stats, outcome.partial, warnings))
    }

    async fn labels(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(Some(matcher_set_text(&request.selectors)))
            .labels(tenant_hash, request)
            .await
            .map_err(failure)?;

        let mut envelope = self.metadata_envelope("labels", request);
        envelope.data = d4::string_list_data("label", outcome.names);
        d4::coverage(&mut envelope, outcome.partial, outcome.warnings);
        Ok(envelope)
    }

    async fn label_values(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
        name: &str,
        include_log_metric_names: bool,
    ) -> Result<Envelope, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(Some(matcher_set_text(&request.selectors)))
            .label_values(tenant_hash, request, name, include_log_metric_names)
            .await
            .map_err(failure)?;

        let mut envelope = self.metadata_envelope("label_values", request);
        envelope
            .scope
            .predicates_applied
            .push(format!("label={name}"));
        envelope.data = d4::string_list_data("value", outcome.values);
        d4::coverage(&mut envelope, outcome.partial, outcome.warnings);
        Ok(envelope)
    }

    async fn series(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(Some(matcher_set_text(&request.selectors)))
            .series(tenant_hash, request)
            .await
            .map_err(failure)?;

        let mut envelope = self.metadata_envelope("series", request);
        envelope.data = d4::series_data(outcome.series);
        d4::coverage(&mut envelope, outcome.partial, outcome.warnings);
        Ok(d4::finish(envelope, &self.budgets))
    }

    async fn analytics(
        &self,
        tenant_hash: TenantHash,
        request: &AnyJson,
    ) -> Result<Envelope, Failure> {
        let args = AnalyticsCall::from_json(request)?;
        let native = AnalyticsRequest {
            query: args.query.clone(),
            start_ms: args.start_ms,
            end_ms: args.end_ms,
            step_ms: args.step_ms,
            min_tokens: decode_tokens(&args.min_tokens)?,
            deadline: deadline(args.deadline_ms, self.remaining()?),
            allow_partial: args.allow_partial,
        };

        let outcome = self
            .audited(None)
            .analytics(tenant_hash, &native)
            .await
            .map_err(failure)?;
        self.report_phases(&outcome.stats.phase_accounting).await;

        let mut envelope = d4::base("metrics", "metrics", &self.budgets);
        d4::window(
            &mut envelope,
            args.start_ms.saturating_mul(1_000_000),
            args.end_ms.saturating_mul(1_000_000),
        );
        d4::min_tokens(&mut envelope, &native.min_tokens);
        envelope.scope.predicates_applied = vec![args.query];
        envelope.coverage.fragments = outcome
            .fragments
            .iter()
            .map(|fragment| {
                format!(
                    "{} segments={} bytes={} status={}",
                    fragment.worker_endpoint,
                    fragment.segment_count,
                    fragment.bytes_reported,
                    fragment.status
                )
            })
            .collect();
        let matrix = match outcome.value {
            PromValue::Matrix(matrix) => matrix,
            other => {
                return Err(d4::invalid_argument(format!(
                    "query evaluates to {}, but ravel_analyze_timeseries requires a range vector",
                    other.type_name()
                )));
            }
        };
        analytics_series_cap(matrix.len())?;
        envelope.data = analysis_data(args.op, matrix)?;
        let warnings = outcome.stats.warnings.clone();
        Ok(self.uncapped_stats_envelope(envelope, &outcome.stats, outcome.partial, warnings))
    }

    async fn exemplars(
        &self,
        tenant_hash: TenantHash,
        request: &AnyJson,
    ) -> Result<Envelope, Failure> {
        let args: ExemplarsArgs = args_of(request, "ravel_get_trace")?;
        let native = ExemplarsRequest {
            query: args.query.clone(),
            start_ns: args.start_ns,
            end_ns: args.end_ns,
            min_tokens: decode_tokens(&args.min_tokens)?,
            deadline: deadline(args.deadline_ms, self.remaining()?),
        };

        let outcome = self
            .audited(None)
            .exemplars(tenant_hash, &native)
            .await
            .map_err(failure)?;

        let mut envelope = d4::base("metrics", "exemplars", &self.budgets);
        d4::window(&mut envelope, args.start_ns, args.end_ns);
        d4::min_tokens(&mut envelope, &native.min_tokens);
        envelope.scope.predicates_applied = vec![args.query];
        let series = outcome
            .series
            .iter()
            .map(|series| serde_json::to_value(series).unwrap_or(serde_json::Value::Null))
            .collect();
        envelope.data = d4::json_rows_data("series", series);
        let stats = serde_json::to_value(&outcome.stats).unwrap_or(serde_json::Value::Null);
        d4::spend_from_stats_json(&mut envelope, &self.budgets, &stats);
        Ok(d4::finish(envelope, &self.budgets))
    }

    async fn sql_execute(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
        resolve_at_ns: i64,
        order_columns: &[String],
    ) -> Result<Draft, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let outcome = self
            .audited(None)
            .sql_execute_at(tenant_hash, request, resolve_at_ns)
            .await
            .map_err(failure)?;
        let envelope = self.sql_envelope(request, &outcome)?;
        let signal = cursor_signal(
            outcome.target,
            outcome.signal_tables_named,
            outcome.reads_parquet,
        );
        let order_tuples = if order_columns.is_empty() {
            Vec::new()
        } else {
            outcome.output.resume_tuples(order_columns)
        };
        Ok(Draft::new(
            envelope,
            signal,
            outcome.pending_erasure,
            outcome.declared_columns,
            outcome.ingest_watermark_hour,
            Some(ravel_mcp::cursor::segment_set_digest(
                outcome.segment_set.iter().map(String::as_str),
            )),
            resolve_at_ns,
        )
        .with_order_tuples(order_tuples))
    }

    async fn trace_lookup(
        &self,
        tenant_hash: TenantHash,
        request: &TraceLookupRequest,
    ) -> Result<Envelope, Failure> {
        let query = &self.budgets.query;
        let lookup = TraceLookup {
            trace_id: request.trace_id,
            start_ns: request.window.map(|window| window.start_ns),
            end_ns: request.window.map(|window| window.end_ns),
            min_tokens: Vec::new(),
            deadline: self.remaining()?,
            // A window the caller named bounds the rows as well as the
            // listing, so a bounded call returns only spans that start in it.
            row_window: true,
            max_rows: Some(self.budgets.max_rows as usize),
            budgets: Some(RequestBudgets {
                max_bytes_scanned: Some(query.max_bytes_scanned),
                max_store_requests: Some(query.max_store_requests),
                max_segments: Some(query.max_segments),
            }),
        };
        let native = self
            .service
            .trace_request(&lookup)
            .map_err(|error| failure(error.into()))?;
        let outcome = self
            .audited(None)
            .sql_execute(tenant_hash, &native)
            .await
            .map_err(failure)?;
        let mut envelope = self.sql_envelope(&native, &outcome)?;
        envelope.scope.signal = "traces".to_string();
        envelope.scope.table = "spans".to_string();
        Ok(envelope)
    }

    fn time_left(&self) -> Option<Duration> {
        self.call_deadline
            .map(|at| at.saturating_duration_since(tokio::time::Instant::now()))
    }

    async fn sql_explain(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
    ) -> Result<ExplainOutcome, Failure> {
        let request = &self.bounded(request, |request| &mut request.deadline)?;
        let report = self
            .audited(None)
            .sql_explain(tenant_hash, request)
            .await
            .map_err(failure)?;
        let unbounded = estimate_components("sql explain", &report.unbounded_components)?;
        Ok(ExplainOutcome {
            target: report.target,
            plan_text: report.plan_text,
            window_predicate: report.window_predicate,
            segments_resolved: report.segments_resolved as u64,
            segments_admitted: report.segments_admitted,
            segments_recent_exempt: report.segments_recent_exempt,
            estimate: report.estimate,
            unbounded,
            columns: report
                .schema
                .fields()
                .iter()
                .map(|field| ExplainColumn {
                    name: field.name().clone(),
                    data_type: field.data_type().to_string(),
                })
                .collect(),
        })
    }

    async fn describe_data(
        &self,
        tenant_hash: TenantHash,
        request: &DescribeRequest,
    ) -> Result<DescribeOutcome, Failure> {
        let signal = match request.signal {
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
            Signal::Spans => "traces",
            other => {
                return Err(d4::invalid_argument(format!(
                    "describe_data: signal {other:?} is not described"
                )));
            }
        };
        let data = self
            .audited(None)
            .describe_data(
                tenant_hash,
                &DescribeDataRequest {
                    signal: signal.to_string(),
                    after_name: request.after_name.clone(),
                    limit: Some(request.limit),
                    resolve_at_ns: request.resolve_at_ns,
                    caller_now_ns: request.caller_now_ns,
                    deadline: self.remaining()?,
                },
            )
            .await
            .map_err(failure)?;
        let column = |column: DescribeColumn| DescribedColumn {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
        };
        Ok(DescribeOutcome {
            table: data.table.to_string(),
            fixed_columns: data.fixed_columns.into_iter().map(column).collect(),
            declared_columns: data.declared_columns.into_iter().map(column).collect(),
            declared_column_set: data.declared_column_set,
            indexed_keys: data.indexed_keys.keys,
            indexed_keys_source: match data.indexed_keys.source {
                IndexedKeysSource::TenantKeyIndex => "tenant_key_index",
                IndexedKeysSource::SpanFormat => "span_format",
                IndexedKeysSource::None => "none",
            }
            .to_string(),
            families: data
                .families
                .into_iter()
                .map(|family| DescribedFamily {
                    name: family.name,
                    kind: metric_kind_name(family.kind).to_string(),
                    unit: family.unit,
                    help: family.help,
                })
                .collect(),
            families_total: data.families_total,
            more_families: data.more_families,
            families_absent_reason: data.families_absent_reason.map(str::to_string),
            ingest_watermark_hour: data.ingest_watermark_hour,
            coverage: data.coverage.map(|window| DescribedCoverage {
                min_event_ts_ns: window.min_event_ts_ns,
                max_event_ts_ns: window.max_event_ts_ns,
            }),
            segments: data.segments,
            committed_rows: data.committed_rows,
            pending_erasure: data.pending_erasure,
            resolved_at_ns: data.resolved_at_ns,
            accounting: data.accounting,
        })
    }
}

/// The matcher set a metadata call reads, as its audit text: the selectors
/// as the caller sent them, joined by `; ` as the HTTP routes join them (and
/// the separator the audit redactor splits on), and `{}` for no selector,
/// which matches every series.
fn matcher_set_text(selectors: &[String]) -> String {
    if selectors.is_empty() {
        return "{}".to_string();
    }
    selectors.join("; ")
}

/// Prometheus' lowercase spelling of a metric family's type, as
/// `GET /api/v1/metadata` renders it.
fn metric_kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "counter",
        MetricKind::Gauge => "gauge",
        MetricKind::Histogram => "histogram",
        MetricKind::Summary => "summary",
        MetricKind::Unknown => "unknown",
    }
}

/// The estimate components a PromQL query cannot bound when its accounting
/// holds spend the estimate leaves out: all three.
const EXCLUDED_SPEND_UNBOUNDED_COMPONENTS: [EstimateComponent; 3] = [
    EstimateComponent::Requests,
    EstimateComponent::StoreBytes,
    EstimateComponent::DecompressedBytes,
];

/// The estimate components a PromQL operation reports unbounded. The PromQL
/// estimate covers the coordinator's own resolve of the metrics snapshot; a
/// query naming a log metric also spends through the log lane, and a
/// federated query also spends through each remote that answered (as does a
/// distributed fetch that fell back to fetching locally). None of that is in
/// the estimate, so such a query names every component, and any other query
/// names none.
fn stats_unbounded_components(stats: &QueryStats) -> &'static [EstimateComponent] {
    if stats.estimate_excludes_spend {
        &EXCLUDED_SPEND_UNBOUNDED_COMPONENTS
    } else {
        &[]
    }
}

/// ravel-sql's names for the estimate components it cannot bound, as the
/// envelope's [`EstimateComponent`]s. `sql_explain` and `sql_envelope` both
/// map the list ravel-sql computed over their own resolve with one rule, so
/// explain and execute name the same components for one statement.
fn estimate_components(
    operation: &str,
    names: &[&'static str],
) -> Result<Vec<EstimateComponent>, Failure> {
    names
        .iter()
        .map(|name| {
            EstimateComponent::from_field_name(name).ok_or_else(|| {
                d4::internal(format!("{operation}: unknown estimate component {name}"))
            })
        })
        .collect()
}

impl ServiceBackend {
    /// The envelope shell of a metadata operation. Its outcome carries no
    /// accounting snapshot, so the `budget` block reports the ceilings the
    /// call ran under and leaves `actual` empty (see the module doc).
    fn metadata_envelope(&self, table: &str, request: &MetadataRequest) -> Envelope {
        let mut envelope = d4::base("metrics", table, &self.budgets);
        d4::window(
            &mut envelope,
            request.window.start_ns,
            request.window.end_ns,
        );
        d4::min_tokens(&mut envelope, &request.min_tokens);
        envelope.scope.predicates_applied = request.selectors.clone();
        envelope
    }

    /// The D4 envelope of an executed SQL statement, before any cap runs on
    /// it.
    fn sql_envelope(
        &self,
        request: &ravel_sql::SqlRequest,
        outcome: &ravel_sql::SqlOutcome,
    ) -> Result<Envelope, Failure> {
        let mut envelope = self.sql_base(request);
        // A result the JSON encoding cannot hold is the caller's choice of
        // column, not an internal fault; anything else is one.
        let output = outcome.output.to_json().map_err(|error| match error {
            ravel_sql::SqlError::UnencodableResult {
                column,
                data_type,
                reason,
            } => d4::unencodable_sql_result(&column, &data_type, reason),
            error => d4::internal(format!("sql result encoding: {error}")),
        })?;
        envelope.data = d4::sql_data(&output);
        if let Some(predicate) = &outcome.stats.window_predicate {
            envelope.scope.predicates_applied.push(predicate.clone());
        }
        // The executor's own row cap stopped the stream, which is a cap this
        // envelope must report even when the D6 row cap did not fire.
        envelope.presentation.row_cap_hit = outcome.stats.row_cap_hit;
        let unbounded = estimate_components("sql execute", &outcome.unbounded_components)?;
        d4::spend(
            &mut envelope,
            &self.budgets,
            &Spend {
                accounting: &outcome.accounting,
                estimate: &outcome.estimate,
                phases: None,
                unbounded: &unbounded,
            },
        );
        Ok(envelope)
    }

    /// The envelope shell of a SQL operation. The signal is not resolved
    /// here: this layer does not parse the statement, and only the explain
    /// report names the table the statement targets.
    fn sql_base(&self, request: &ravel_sql::SqlRequest) -> Envelope {
        let mut envelope = d4::base("mixed", "sql", &self.budgets);
        d4::window(
            &mut envelope,
            request.window.start_ns,
            request.window.end_ns,
        );
        // In the wire form the caller sent, table qualifier included.
        envelope.visibility.min_commit_tokens_applied = request
            .min_tokens
            .iter()
            .map(ravel_sql::MinCommitToken::encode)
            .collect();
        envelope
    }
}

fn failure(error: ServiceError) -> Failure {
    d4::failure(&error)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No selector is the whole matcher set `{}`; selectors are recorded as
    /// the caller sent them, spacing included, joined the way the HTTP
    /// routes join theirs.
    #[test]
    fn the_matcher_set_text_is_verbatim_and_never_empty() {
        assert_eq!(matcher_set_text(&[]), "{}");
        let selectors = ["m{ k = \"v\" }".to_string(), "{job=\"a\"}".to_string()];
        assert_eq!(matcher_set_text(&selectors), "m{ k = \"v\" }; {job=\"a\"}");
        assert_eq!(matcher_set_text(&selectors), selectors.join("; "));
        assert_eq!(matcher_set_text(&["m{".to_string()]), "m{");
    }

    /// The adapter's series cap is the HTTP route's: `MAX_SERIES` series
    /// pass, one more is `budget_exceeded` on the `series` counter with the
    /// route's own refusal text.
    ///
    /// FLIP: a separate copy of the cap at another bound fails one of the
    /// two edges, and a separate copy of the message fails the text.
    #[test]
    fn the_series_cap_is_the_http_routes() {
        assert!(analytics_series_cap(analytics::MAX_SERIES).is_ok());
        let over = analytics::MAX_SERIES + 1;
        let Err(failure) = analytics_series_cap(over) else {
            panic!("{over} series passed the cap");
        };
        assert_eq!(failure.class, FailureClass::BudgetExceeded);
        assert_eq!(failure.message, analytics::series_cap_message(over));
        assert_eq!(failure.counter.as_deref(), Some("series"));
    }

    /// A PromQL operation whose accounting holds spend the estimate leaves
    /// out (a federated remote's, or the log lane's) names every estimate
    /// component unbounded; any other names none. mcp_e2e has no federation
    /// fixture, so this pins the federated case at the mapping.
    ///
    /// FLIP: returning the empty list unconditionally fails the first
    /// assertion; returning the full list unconditionally fails the second.
    #[test]
    fn excluded_spend_names_every_estimate_component_unbounded() {
        let excluded = QueryStats {
            estimate_excludes_spend: true,
            ..QueryStats::default()
        };
        assert_eq!(
            stats_unbounded_components(&excluded),
            [
                EstimateComponent::Requests,
                EstimateComponent::StoreBytes,
                EstimateComponent::DecompressedBytes,
            ]
        );
        assert!(stats_unbounded_components(&QueryStats::default()).is_empty());
    }
}
