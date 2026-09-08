//! The [`QueryBackend`] port over [`QueryService`].
//!
//! Every method here is one call into the query service and one conversion of
//! its outcome into a D4 envelope. Nothing else: the admission permit, the
//! deadline clamp, the cost record, the usage guard, the audit submission, the
//! partial-coverage gate, and the error redaction all live inside the service
//! layer, and a tool call takes exactly one permit because it makes exactly
//! one of these calls (ADR-1374 decision 7). Taking a second permit here
//! would deadlock a deployment whose ceiling is 1.
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
//! ran under and no `actual` block; and no outcome carries the resolved
//! snapshot id or watermark hour, so `visibility` reports only the commit
//! tokens the caller required. A zero in either place would read as a
//! measurement.

use std::time::Duration;

use ravel_mcp::budget::McpEffectiveBudgets;
use ravel_mcp::envelope::{AnyJson, Cell, Envelope, Failure};
use ravel_mcp::service::QueryBackend;
use ravel_promql::{RangeValue, Value as PromValue};
use ravel_query::http::{InstantRequest, MetadataRequest, RangeRequest};
use ravel_query::{PhaseAccountingSnapshot, QueryPhase, QueryStats};
use ravel_types::{CommitToken, TenantHash};
use rmcp::model::{ProgressNotificationParam, ProgressToken};
use rmcp::service::{Peer, RoleServer};
use serde::Deserialize;
use serde_json::json;

use crate::service::{AnalyticsRequest, ExemplarsRequest, QueryService, ServiceError};

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
}

impl ServiceBackend {
    pub fn new(service: QueryService, budgets: McpEffectiveBudgets) -> Self {
        ServiceBackend {
            service,
            budgets,
            progress: None,
        }
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
    /// [`QueryStats`]: its spend, its per-phase split, and its coverage.
    fn stats_envelope(
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
                estimate_is_upper_envelope: true,
            },
        );
        d4::finish(envelope, &self.budgets)
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

/// `ravel_analyze_timeseries`'s request, as it crosses the port: the
/// caller-visible shape of [`AnalyticsRequest`], whose own type is not
/// reachable from `ravel-mcp`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnalyticsArgs {
    query: String,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    #[serde(default)]
    min_tokens: Vec<String>,
    #[serde(default)]
    deadline_ms: Option<u64>,
    #[serde(default)]
    allow_partial: bool,
}

/// The exemplar query's request, same reachability reason as
/// [`AnalyticsArgs`].
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
        let outcome = self
            .service
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
        Ok(self.stats_envelope(envelope, &outcome.stats, outcome.partial, warnings))
    }

    async fn promql_range(
        &self,
        tenant_hash: TenantHash,
        request: &RangeRequest,
    ) -> Result<Envelope, Failure> {
        let outcome = self
            .service
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
        Ok(self.stats_envelope(envelope, &outcome.stats, outcome.partial, warnings))
    }

    async fn labels(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure> {
        let outcome = self
            .service
            .labels(tenant_hash, request)
            .await
            .map_err(failure)?;

        let mut envelope = self.metadata_envelope("labels", request);
        envelope.data = d4::string_list_data("label", outcome.names);
        d4::coverage(&mut envelope, outcome.partial, outcome.warnings);
        Ok(d4::finish(envelope, &self.budgets))
    }

    async fn label_values(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
        name: &str,
        include_log_metric_names: bool,
    ) -> Result<Envelope, Failure> {
        let outcome = self
            .service
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
        Ok(d4::finish(envelope, &self.budgets))
    }

    async fn series(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure> {
        let outcome = self
            .service
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
        let args: AnalyticsArgs = args_of(request, "ravel_analyze_timeseries")?;
        let native = AnalyticsRequest {
            query: args.query.clone(),
            start_ms: args.start_ms,
            end_ms: args.end_ms,
            step_ms: args.step_ms,
            min_tokens: decode_tokens(&args.min_tokens)?,
            deadline: deadline(args.deadline_ms, self.budgets.deadline),
            allow_partial: args.allow_partial,
        };

        let outcome = self
            .service
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
        envelope.data = Self::value_data(outcome.value);
        Ok(self.stats_envelope(envelope, &outcome.stats, outcome.partial, Vec::new()))
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
            deadline: deadline(args.deadline_ms, self.budgets.deadline),
        };

        let outcome = self
            .service
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
    ) -> Result<Envelope, Failure> {
        let outcome = self
            .service
            .sql_execute(tenant_hash, request)
            .await
            .map_err(failure)?;

        let mut envelope = self.sql_base(request);
        let output = outcome
            .output
            .to_json()
            .map_err(|error| d4::internal(format!("sql result encoding: {error}")))?;
        envelope.data = d4::sql_data(&output);
        if let Some(predicate) = &outcome.stats.window_predicate {
            envelope.scope.predicates_applied.push(predicate.clone());
        }
        // The executor's own row cap stopped the stream, which is a cap this
        // envelope must report even when the D6 row cap did not fire.
        envelope.presentation.row_cap_hit = outcome.stats.row_cap_hit;
        d4::spend(
            &mut envelope,
            &self.budgets,
            &Spend {
                accounting: &outcome.accounting,
                estimate: &outcome.estimate,
                phases: None,
                estimate_is_upper_envelope: true,
            },
        );
        Ok(d4::finish(envelope, &self.budgets))
    }

    async fn sql_explain(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
    ) -> Result<Envelope, Failure> {
        let report = self
            .service
            .sql_explain(tenant_hash, request)
            .await
            .map_err(failure)?;

        let mut envelope = self.sql_base(request);
        envelope.scope.signal = target_signal(report.target).to_string();
        envelope.plan = Some(report.plan_text.clone());
        if let Some(predicate) = &report.window_predicate {
            envelope.scope.predicates_applied.push(predicate.clone());
        }
        envelope.data = d4::property_data(vec![
            ("segments_resolved", json!(report.segments_resolved)),
            ("segments_admitted", json!(report.segments_admitted)),
            (
                "segments_recent_exempt",
                json!(report.segments_recent_exempt),
            ),
            (
                "result_columns",
                json!(
                    report
                        .schema
                        .fields()
                        .iter()
                        .map(|field| json!({
                            "name": field.name(),
                            "type": field.data_type().to_string(),
                        }))
                        .collect::<Vec<_>>()
                ),
            ),
        ]);
        // An estimate with unbounded components is not an upper envelope of
        // the whole query: the components the estimator cannot bound are
        // named so a structural zero is not read as an estimate of zero.
        for component in &report.unbounded_components {
            envelope
                .warnings
                .push(format!("cost estimate does not bound {component}"));
        }
        d4::estimate_only(
            &mut envelope,
            &self.budgets,
            &report.estimate,
            report.unbounded_components.is_empty(),
        );
        Ok(d4::finish(envelope, &self.budgets))
    }
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
        d4::min_tokens(&mut envelope, &request.min_tokens);
        envelope
    }
}

/// The signal name of a SQL statement's target table, in the spelling
/// `ravel_capabilities` reports its enabled signals with.
fn target_signal(target: ravel_sql::TargetSignal) -> &'static str {
    match target {
        ravel_sql::TargetSignal::Metrics => "metrics",
        ravel_sql::TargetSignal::Logs => "logs",
        ravel_sql::TargetSignal::Spans => "traces",
        ravel_sql::TargetSignal::Alerts => "alerts",
        ravel_sql::TargetSignal::Audit => "audit",
    }
}

fn failure(error: ServiceError) -> Failure {
    d4::failure(&error)
}
