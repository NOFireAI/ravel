//! `POST /api/v1/analytics`: the post-evaluation analytics endpoint decided by
//! ADR-0028.
//!
//! The endpoint runs the exact same range evaluation `/api/v1/query_range`
//! uses (same planner, budgets, staleness filtering, and wall deadline), then
//! applies one analytic operation to each series of the resulting matrix. The
//! analytic operations themselves live in `ravel-analytics` as pure per-series
//! functions over `(timestamp_ns, f64)` slices (ADR-0028 decision 1); this
//! module is only the transport, the request/response shape, and the
//! error-to-HTTP boundary, exactly the split `/api/v1/sql` keeps between this
//! crate and `ravel-sql`.
//!
//! # Request
//!
//! JSON body, mirroring `/api/v1/sql`'s choice of JSON over the form encoding
//! the Prometheus-shaped endpoints use:
//!
//! ```json
//! {
//!   "query": "http_requests_total",
//!   "start": 1735689600.0,
//!   "end":   1735693200.0,
//!   "step":  "30s",
//!   "op": {"type": "change_point", "downsample": false}
//! }
//! ```
//!
//! `start`, `end`, and `step` accept the identical syntax
//! `/api/v1/query_range` accepts: `start`/`end` are Unix float seconds or an
//! RFC3339 instant, `step` is a Prometheus duration (`30s`, `5m`) or bare
//! float seconds. A JSON number is accepted wherever a bare-seconds string
//! would be, so `30` and `"30"` and `"30s"` all mean thirty seconds. The
//! optional `timeout` (seconds) can only *lower* the server deadline, never
//! raise it, and the optional `min_commit_token` array carries read-your-write
//! tokens, both exactly as the range endpoint treats them.
//!
//! The optional `allow_partial` (bool, default `false`) is the consent gate for
//! partial federated coverage (ADR-0071, "partial results are consent-gated and
//! envelope-visible" amendment, section 2). Absent or `false`, a request whose
//! evaluation skipped a federated remote is refused with a 503 rather than
//! answered from an incomplete read; `true` accepts the partial answer, which
//! the response envelope then states. It is a body field, not a query-string
//! parameter, because this endpoint's whole request contract is the JSON body.
//!
//! `op` is a tagged object selecting one analytic:
//!
//! - `{"type": "change_point", "downsample": <bool, default false>}`
//! - `{"type": "summary", "percentiles": [<f64 in [0,1]>, ...], default []}`
//!
//! An unknown `op` type, a missing required field, or any other malformed body
//! is a 400.
//!
//! # Response
//!
//! A JSON envelope in the Prometheus response style with one entry per series:
//!
//! ```json
//! {
//!   "status": "success",
//!   "data": {
//!     "resultType": "analytics",
//!     "result": [
//!       {"metric": {"__name__": "http_requests_total"},
//!        "result": {"kind": "step_change", "ts_ns": 1735691400000000000,
//!                   "score": 42.1, "downsampled": false,
//!                   "original_points": 120, "nan_excluded": 0}}
//!     ]
//!   },
//!   "partial": false,
//!   "warnings": []
//! }
//! ```
//!
//! `partial` and `warnings` are the ADR-0071 coverage surface: `partial` is
//! always present and is `true` exactly when the evaluation skipped a federated
//! remote (which requires `allow_partial: true`, see above), and `warnings`
//! carries the fan-out's already-redacted per-cluster warnings, empty on a
//! complete answer.
//!
//! The op result is serialized through the server-local serde structs at the
//! bottom of this module; `ravel-analytics` carries no serde derive (ADR-0028
//! keeps that crate a pure, dependency-light compute layer).
//!
//! # Errors
//!
//! Evaluator errors keep the exact status mapping `/api/v1/query_range` uses,
//! including the redaction of storage-layer faults that would otherwise
//! leak an object key or tenant hash. `ravel-analytics` errors and the
//! per-call series cap map to 422. Partial coverage without `allow_partial`
//! maps to 503 `unavailable`, the same typed shape the HTTP query endpoints
//! use. The full table is in docs/analytics.md.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ravel_analytics::{
    AnalyticsError, ChangeKind, ChangePointParams, ChangePointResult, SummaryParams, SummaryResult,
};
use ravel_ingest::Clock;
use ravel_maintain::{QueryAuditSink, QueryStatus, query_audit_event};
use ravel_promql::Value;
use ravel_query::http::{QueryErrorResponse, TenantResolver};
use ravel_query::{Coverage, QueryEngine, QueryError};
use ravel_types::{CommitToken, LabelSet, Sample, TenantHash};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::Instrument;

use crate::metrics::WorkloadClass;

/// Cap on the request body. The analytics request is a query plus a few scalar
/// parameters, never large in legitimate use; this mirrors the defensive bound
/// the sibling `/api/v1/sql` handler applies.
const MAX_BODY_BYTES: usize = 1 << 20;

const MS_PER_SEC: f64 = 1_000.0;

/// ADR-0028 decision 4: an analytics call processes at most this many series;
/// more is a typed error before any op runs, never a silent truncation.
const MAX_SERIES: usize = 1000;

/// Shared state for the analytics route: the query engine (the same instance
/// the Prometheus-shaped routes use, so the evaluation is byte-for-byte the
/// one `/api/v1/query_range` would run), the tenant resolver, and an injected
/// clock read once per request.
#[derive(Clone)]
pub struct AnalyticsState {
    pub engine: Arc<QueryEngine>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub clock: Arc<dyn Clock>,
    /// The process-global per-query cost aggregator (ADR-0044 section 4).
    /// The analytics endpoint runs the same range evaluation
    /// `/api/v1/query_range` does, so its accounting folds into `/metrics`
    /// exactly like a range query's would.
    pub query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    /// The evidential audit sink one event per executed analytics query is
    /// submitted through, its durability awaited before the response is
    /// released (ADR-0062 §2a). Defaults to the no-op in
    /// deployments with no pipeline; a deployment attaches the one shared
    /// pipeline.
    pub audit_sink: Arc<dyn QueryAuditSink>,
}

/// The `/api/v1/analytics` router.
pub fn router(state: AnalyticsState) -> Router {
    Router::new()
        .route("/api/v1/analytics", post(handle))
        .with_state(state)
}

/// A JSON scalar that is either a number or a string. `start`/`end`/`step`
/// accept both so the endpoint matches `/api/v1/query_range`, whose values
/// arrive form-encoded (always strings) and are first tried as a float: a JSON
/// number and its string spelling parse identically.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Scalar {
    Num(f64),
    Str(String),
}

#[derive(Debug, Deserialize)]
struct AnalyticsBody {
    query: String,
    start: Scalar,
    end: Scalar,
    step: Scalar,
    op: OpSpec,
    /// Per-request wall deadline in seconds. Clamped to the server maximum;
    /// can only lower it, never raise it.
    #[serde(default)]
    timeout: Option<f64>,
    /// Read-your-write commit tokens, exactly as the range endpoint treats
    /// them.
    #[serde(default)]
    min_commit_token: Vec<String>,
    /// Consent to a partial federated answer (ADR-0071 amendment, section 2).
    /// `serde(default)`, so an absent field means `false`: a client that never
    /// asked for partial coverage gets a 503 refusal instead of an incomplete
    /// answer that looks complete. Mirrors the `allow_partial` query-string
    /// parameter the HTTP query endpoints take, carried in the body because
    /// this endpoint's request contract is the body.
    #[serde(default)]
    allow_partial: bool,
}

/// The tagged analytic selector. `rename_all = "snake_case"` makes the tag
/// values `"change_point"` and `"summary"`; an unknown tag is a serde error,
/// which the handler turns into a 400.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpSpec {
    ChangePoint {
        #[serde(default)]
        downsample: bool,
    },
    Summary {
        #[serde(default)]
        percentiles: Vec<f64>,
    },
}

async fn handle(State(state): State<AnalyticsState>, req: Request<Body>) -> Response {
    match run(&state, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn run(state: &AnalyticsState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let body: AnalyticsBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON request body: {e}")))?;

    // start/end/step parse exactly as `/api/v1/query_range`: a parse failure is
    // a 400 with the offending field and value, before any query runs.
    let start_ms = parse_timestamp_ms("start", &body.start)?;
    let end_ms = parse_timestamp_ms("end", &body.end)?;
    let step_ms = parse_duration_ms("step", &body.step)?;

    let min_tokens = body
        .min_commit_token
        .iter()
        .map(|raw| {
            CommitToken::decode(raw)
                .map_err(|_| ApiError::bad_request(format!("invalid min_commit_token: {raw:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let max_deadline = state.engine.config().deadline;
    let deadline = parse_deadline(body.timeout, max_deadline)?;

    // Percentiles are a request-level parameter: validate them once up front so
    // an invalid quantile is rejected before an expensive range evaluation and
    // regardless of how many series match (ADR-0028 decision 3, the crate's
    // per-call `InvalidPercentile` contract lifted to the request).
    if let OpSpec::Summary { percentiles } = &body.op {
        for &q in percentiles {
            if !(0.0..=1.0).contains(&q) {
                return Err(ApiError::from_analytics(
                    AnalyticsError::InvalidPercentile { got: q },
                ));
            }
        }
    }

    let now_ns = state.clock.now_ns();

    // A request-level span carrying only bounded values (ADR-0044 section 5):
    // the tenant hash, the workload class, and -- once the range evaluation
    // finishes -- the final store request and byte counts. No query text, label
    // values, or object keys ever become span fields. An analytics call is an
    // interactive, client-driven query.
    let span = tracing::info_span!(
        "analytics_query",
        tenant_hash = %tenant_hash.to_hex(),
        workload_class = WorkloadClass::Interactive.name(),
        s3_requests = tracing::field::Empty,
        s3_bytes = tracing::field::Empty,
    );
    // The range evaluation now runs for a resolved tenant, so it is auditable
    // (ADR-0062 §2a): run it, submit one audit event for the outcome, and await
    // its durability before releasing the response. A request rejected earlier
    // (auth, body parse, invalid parameters) never reached here and is not
    // audited. The recorded window is the request's resolved `[start, end]`.
    // Collect per-slice fragment observability (ADR-0071 stats.fragments[]) for the duration of the range evaluation. The sink is
    // installed in task-local storage so every distributed slice the engine
    // dispatches records into it; a non-distributed engine (or a query the cost
    // gate declined to fan out) records nothing, and the field stays absent.
    let fragment_sink = crate::distrib::FragmentStatsSink::new();
    let eval = crate::distrib::with_fragment_stats(
        fragment_sink.clone(),
        state
            .engine
            .range_with_stats(
                tenant_hash,
                &body.query,
                start_ms,
                end_ms,
                step_ms,
                &min_tokens,
                now_ns,
                deadline,
            )
            .instrument(span.clone()),
    )
    .await;
    let status = if eval.is_ok() {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };
    submit_audit(
        state,
        tenant_hash,
        now_ns,
        &body.query,
        status,
        ms_to_ns(start_ms),
        ms_to_ns(end_ms),
    )
    .await?;
    let (value, stats) = eval.map_err(ApiError::from_query)?;

    // Consent gate (ADR-0071 "partial results are consent-gated and
    // envelope-visible" amendment, section 2): the evaluation ran and was
    // audited, but a partial answer the client did not opt into is refused with
    // the typed 503 the HTTP query endpoints use, before any op runs and before
    // any body is built. Coverage is knowable only now, and it is derived from
    // the stats the engine already produced rather than newly tracked
    // (amendment decision 4).
    let coverage = Coverage::from_stats(&stats);
    gate_partial(&coverage, body.allow_partial)?;

    // Fold this query's actual cost and its pre-execution estimate into the
    // process-global aggregator for `/metrics` (ADR-0044 section 4) and record
    // the final counts on the span.
    span.record("s3_requests", stats.accounting.total_s3_requests());
    span.record("s3_bytes", stats.accounting.total_s3_bytes());
    state.query_accounting.record(
        tenant_hash,
        WorkloadClass::Interactive,
        &stats.accounting,
        &stats.estimate,
    );
    let mut stats_json = crate::query::accounting_stats_json(&stats.accounting, &stats.estimate);
    // ADR-0071: a distributed run attaches a per-slice
    // `fragments[]` beside `accounting`/`estimate`; a non-distributed run
    // collected no entries, so the field is omitted rather than an empty array.
    let fragments = fragment_sink.take();
    if !fragments.is_empty()
        && let Some(object) = stats_json.as_object_mut()
    {
        object.insert(
            "fragments".to_string(),
            crate::query::fragments_json(&fragments),
        );
    }

    let matrix = match value {
        Value::Matrix(matrix) => matrix,
        // A range query that folds to a scalar, a string, or an instant vector
        // has no per-series matrix to analyze. Rejected as a request error,
        // mirroring how `/api/v1/query_range` treats an instant-vector result
        // from a range query.
        other => {
            return Err(ApiError::bad_request(format!(
                "query evaluates to {}, but the analytics endpoint requires a range vector",
                other.type_name()
            )));
        }
    };

    // ADR-0028 decision 4: enforce the series cap before running any op, as a
    // typed error rather than a truncation.
    if matrix.len() > MAX_SERIES {
        return Err(ApiError::series_cap(matrix.len()));
    }

    let result = apply_op(&body.op, matrix)?;
    Ok((
        StatusCode::OK,
        axum::Json(json!({
            "status": "success",
            "data": {
                "resultType": "analytics",
                "result": result,
            },
            // Beside `data`, the "stats object beside data" shape
            // `/api/v1/query_exemplars` uses (ADR-0044).
            "stats": stats_json,
            // The coverage of the answer, stated on the envelope where a naive
            // consumer cannot miss it (ADR-0071 amendment, section 2): `partial`
            // is always present, and `warnings` carries the federation
            // fan-out's already-redacted per-cluster warnings (empty on a
            // complete answer). Reaching here with `partial: true` means the
            // request opted in through `allow_partial`.
            "partial": coverage.is_partial(),
            "warnings": stats.warnings,
        })),
    )
        .into_response())
}

/// The consent gate, the analytics mirror of `gate_partial` in
/// `crates/ravel-query/src/http/handlers.rs` (ADR-0071 amendment, section 2).
/// Partial coverage the client did not opt into fails with the same typed shape
/// the HTTP query endpoints use, HTTP 503 with `errorType: "unavailable"`, so a
/// consumer that never asked for a partial answer fails safe instead of
/// silently accepting one. Complete coverage, or partial coverage with
/// `allow_partial: true`, passes and the response is a 200 whose top-level
/// `partial` field states the coverage.
fn gate_partial(coverage: &Coverage, allow_partial: bool) -> Result<(), ApiError> {
    if coverage.is_partial() && !allow_partial {
        return Err(ApiError::partial_refusal(coverage.skipped()));
    }
    Ok(())
}

/// Apply the selected op to every series, converting each crate result into
/// its serialized entry. The evaluator emits `Sample::ts_ns` already in
/// nanoseconds (crates/ravel-promql doc: sample timestamps are nanoseconds),
/// which is the unit `ravel-analytics` expects, so the boundary conversion is
/// a direct read of `ts_ns` with no rescale.
fn apply_op(
    op: &OpSpec,
    matrix: Vec<(LabelSet, Vec<Sample>)>,
) -> Result<Vec<SeriesEntry>, ApiError> {
    matrix
        .into_iter()
        .map(|(labels, samples)| {
            let points: Vec<(i64, f64)> = samples.into_iter().map(|s| (s.ts_ns, s.value)).collect();
            let result = match op {
                OpSpec::ChangePoint { downsample } => {
                    let params = ChangePointParams {
                        downsample: *downsample,
                    };
                    let out = ravel_analytics::change_point(&points, &params)
                        .map_err(ApiError::from_analytics)?;
                    OpResultDto::ChangePoint(ChangePointDto::from(out))
                }
                OpSpec::Summary { percentiles } => {
                    let params = SummaryParams {
                        percentiles: percentiles.clone(),
                    };
                    let out = ravel_analytics::summary(&points, &params)
                        .map_err(ApiError::from_analytics)?;
                    OpResultDto::Summary(SummaryDto::from(out))
                }
            };
            Ok(SeriesEntry {
                metric: labels_to_map(&labels),
                result,
            })
        })
        .collect()
}

/// Submit one query-audit event for an executed analytics query and await its
/// durability before the response is released (ADR-0062 §2a). `language` is
/// `analytics` so the record shape stays one schema across surfaces. On a
/// submission failure the request fails closed with a retryable 503
/// (`audit_mode=required`); in best-effort mode the pipeline resolves it to
/// `Ok` and the response is released.
#[allow(clippy::too_many_arguments)]
async fn submit_audit(
    state: &AnalyticsState,
    tenant_hash: TenantHash,
    now_ns: i64,
    query_text: &str,
    status: QueryStatus,
    window_start_ns: i64,
    window_end_ns: i64,
) -> Result<(), ApiError> {
    let event = query_audit_event(
        &tenant_hash,
        now_ns,
        query_text,
        "analytics",
        status,
        window_start_ns,
        window_end_ns,
    );
    state.audit_sink.submit(event).await.map_err(|_| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        error_type: "unavailable",
        message: "query audit is temporarily unavailable; retry".to_string(),
    })
}

/// Nanosecond form of a millisecond bound for an audit window, saturating
/// rather than overflowing.
fn ms_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

fn authenticate(state: &AnalyticsState, headers: &HeaderMap) -> Result<TenantHash, ApiError> {
    state
        .tenant_resolver
        .resolve(headers)
        .map(|tenant| tenant.hash())
        .map_err(|_| ApiError {
            status: StatusCode::UNAUTHORIZED,
            error_type: "unauthorized",
            message: "authentication required".to_string(),
        })
}

fn labels_to_map(labels: &LabelSet) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Parameter parsing: identical semantics to crates/ravel-query/src/http/params
// (that module is private to ravel-query, so the semantics are reproduced here
// rather than imported). A JSON number is treated as the bare-seconds spelling
// that module tries first.
// ---------------------------------------------------------------------------

/// Prometheus float seconds or an RFC3339 instant, yielding milliseconds. A
/// JSON number is the bare-seconds form.
fn parse_timestamp_ms(name: &'static str, value: &Scalar) -> Result<i64, ApiError> {
    match value {
        Scalar::Num(secs) => seconds_to_ms(name, *secs),
        Scalar::Str(s) => {
            if let Ok(secs) = s.parse::<f64>() {
                return seconds_to_ms(name, secs);
            }
            let system_time =
                humantime::parse_rfc3339(s).map_err(|_| ApiError::invalid_param(name, s))?;
            let dur = system_time
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| ApiError::invalid_param(name, s))?;
            i64::try_from(dur.as_millis()).map_err(|_| ApiError::invalid_param(name, s))
        }
    }
}

/// Prometheus duration syntax (`30s`, `5m`) or bare float seconds, yielding
/// milliseconds. A JSON number is the bare-seconds form.
fn parse_duration_ms(name: &'static str, value: &Scalar) -> Result<i64, ApiError> {
    match value {
        Scalar::Num(secs) => seconds_to_ms(name, *secs),
        Scalar::Str(s) => {
            if let Ok(secs) = s.parse::<f64>() {
                return seconds_to_ms(name, secs);
            }
            let dur = humantime::parse_duration(s).map_err(|_| ApiError::invalid_param(name, s))?;
            i64::try_from(dur.as_millis()).map_err(|_| ApiError::invalid_param(name, s))
        }
    }
}

fn seconds_to_ms(name: &'static str, secs: f64) -> Result<i64, ApiError> {
    if !secs.is_finite() {
        return Err(ApiError::invalid_param(name, &secs.to_string()));
    }
    let ms = secs * MS_PER_SEC;
    if ms < i64::MIN as f64 || ms > i64::MAX as f64 {
        return Err(ApiError::invalid_param(name, &secs.to_string()));
    }
    Ok(ms as i64)
}

/// Resolve the wall deadline: the client `timeout` seconds clamped to the
/// server maximum, or the server maximum when absent. A non-positive or
/// non-finite `timeout` is a 400 (matching sql.rs).
fn parse_deadline(timeout: Option<f64>, max: Duration) -> Result<Duration, ApiError> {
    match timeout {
        Some(secs) if secs > 0.0 && secs.is_finite() => Ok(Duration::from_secs_f64(secs).min(max)),
        Some(_) => Err(ApiError::bad_request(
            "\"timeout\" must be a positive, finite number of seconds".to_string(),
        )),
        None => Ok(max),
    }
}

// ---------------------------------------------------------------------------
// Error boundary
// ---------------------------------------------------------------------------

/// A client-visible error: a status, a stable type tag, and a message that has
/// already passed the redaction boundary. `Debug` is safe to derive because
/// every field is already redacted.
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message,
        }
    }

    fn invalid_param(name: &str, value: &str) -> Self {
        ApiError::bad_request(format!("invalid value for parameter {name:?}: {value:?}"))
    }

    /// ADR-0028 decision 4: the per-call series cap is a budget breach, mapped
    /// to 422 like the query engine's own budget rejections.
    fn series_cap(count: usize) -> Self {
        ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error_type: "execution",
            message: format!(
                "analytics call matched {count} series, exceeding the limit of {MAX_SERIES}"
            ),
        }
    }

    /// Partial coverage without `allow_partial` (ADR-0071 amendment, section
    /// 2). The status, the `errorType` tag, and the message shape are the ones
    /// `ApiError::Unavailable` and `partial_refusal_message` produce on the HTTP
    /// query endpoints (`crates/ravel-query/src/http/error.rs`,
    /// `handlers.rs`), reproduced here because both are private to that crate;
    /// the wording is kept identical so one refusal contract covers every read
    /// surface. `skipped` names the degraded clusters, already redacted at the
    /// federation seam to operator-facing cluster names, and the remedy
    /// (`allow_partial`) is named in the failure itself.
    fn partial_refusal(skipped: &[String]) -> Self {
        let clusters = if skipped.is_empty() {
            "one or more federated clusters were skipped".to_string()
        } else {
            skipped.join("; ")
        };
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "unavailable",
            message: format!(
                "partial results: coverage is incomplete ({clusters}); \
                 set allow_partial=true to receive partial results"
            ),
        }
    }

    /// `ravel-analytics` errors map to 422. Their `Display` carries only the
    /// caller's own parameters (a percentile, a point count), no server state,
    /// so it is safe to echo.
    fn from_analytics(err: AnalyticsError) -> Self {
        ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error_type: "execution",
            message: err.to_string(),
        }
    }

    /// Map a `QueryError` through `ravel-query`'s public HTTP mapping, so this
    /// endpoint keeps the exact status contract of `/api/v1/query_range`,
    /// including the redaction of storage-layer faults, from one shared
    /// source rather than a copy that could drift.
    fn from_query(err: QueryError) -> Self {
        let QueryErrorResponse {
            status,
            error_type,
            message,
        } = QueryErrorResponse::from_query_error(err);
        ApiError {
            status,
            error_type,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(json!({
                "status": "error",
                "errorType": self.error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Server-local serialization of the ravel-analytics results (ADR-0028
// decision 3: snake_case fields, no serde in ravel-analytics).
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct SeriesEntry {
    metric: BTreeMap<String, String>,
    result: OpResultDto,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum OpResultDto {
    ChangePoint(ChangePointDto),
    Summary(SummaryDto),
}

/// The change-point kind, serialized snake_case (`StepChange` -> `step_change`).
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChangeKindDto {
    Spike,
    Dip,
    StepChange,
    TrendChange,
    DistributionChange,
    Stationary,
    Indeterminable,
}

impl From<ChangeKind> for ChangeKindDto {
    fn from(kind: ChangeKind) -> Self {
        match kind {
            ChangeKind::Spike => ChangeKindDto::Spike,
            ChangeKind::Dip => ChangeKindDto::Dip,
            ChangeKind::StepChange => ChangeKindDto::StepChange,
            ChangeKind::TrendChange => ChangeKindDto::TrendChange,
            ChangeKind::DistributionChange => ChangeKindDto::DistributionChange,
            ChangeKind::Stationary => ChangeKindDto::Stationary,
            ChangeKind::Indeterminable => ChangeKindDto::Indeterminable,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChangePointDto {
    kind: ChangeKindDto,
    /// The nanosecond timestamp of the change, or null for `stationary` and
    /// `indeterminable`.
    ts_ns: Option<i64>,
    score: f64,
    downsampled: bool,
    original_points: usize,
    nan_excluded: usize,
}

impl From<ChangePointResult> for ChangePointDto {
    fn from(r: ChangePointResult) -> Self {
        ChangePointDto {
            kind: r.kind.into(),
            ts_ns: r.ts_ns,
            score: r.score,
            downsampled: r.downsampled,
            original_points: r.original_points,
            nan_excluded: r.nan_excluded,
        }
    }
}

#[derive(Debug, Serialize)]
struct PercentileDto {
    quantile: f64,
    value: f64,
}

#[derive(Debug, Serialize)]
struct SummaryDto {
    median: f64,
    mad: f64,
    percentiles: Vec<PercentileDto>,
    stddev: f64,
    variance: f64,
    nan_excluded: usize,
}

impl From<SummaryResult> for SummaryDto {
    fn from(r: SummaryResult) -> Self {
        SummaryDto {
            median: r.median,
            mad: r.mad,
            percentiles: r
                .percentiles
                .into_iter()
                .map(|(quantile, value)| PercentileDto { quantile, value })
                .collect(),
            stddev: r.stddev,
            variance: r.variance,
            nan_excluded: r.nan_excluded,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn body(json: &str) -> AnalyticsBody {
        serde_json::from_str(json).expect("valid body")
    }

    const MAX: Duration = Duration::from_secs(30);

    #[test]
    fn change_point_op_parses_with_default_downsample() {
        let b = body(r#"{"query":"m","start":0,"end":1,"step":"1s","op":{"type":"change_point"}}"#);
        match b.op {
            OpSpec::ChangePoint { downsample } => assert!(!downsample),
            _ => panic!("expected change_point"),
        }
    }

    #[test]
    fn summary_op_parses_with_percentiles() {
        let b = body(
            r#"{"query":"m","start":0,"end":1,"step":"1s","op":{"type":"summary","percentiles":[0.5,0.9]}}"#,
        );
        match b.op {
            OpSpec::Summary { percentiles } => assert_eq!(percentiles, vec![0.5, 0.9]),
            _ => panic!("expected summary"),
        }
    }

    #[test]
    fn number_and_string_seconds_parse_identically() {
        let num = parse_timestamp_ms("start", &Scalar::Num(1.5)).expect("num");
        let string = parse_timestamp_ms("start", &Scalar::Str("1.5".to_string())).expect("str");
        assert_eq!(num, 1_500);
        assert_eq!(num, string);
    }

    #[test]
    fn step_accepts_duration_syntax() {
        assert_eq!(
            parse_duration_ms("step", &Scalar::Str("30s".to_string())).expect("dur"),
            30_000
        );
        assert_eq!(
            parse_duration_ms("step", &Scalar::Num(30.0)).expect("num"),
            30_000
        );
    }

    #[test]
    fn rfc3339_start_parses() {
        let ms = parse_timestamp_ms("start", &Scalar::Str("1970-01-01T00:00:01Z".to_string()))
            .expect("rfc3339");
        assert_eq!(ms, 1_000);
    }

    #[test]
    fn a_non_finite_or_unparseable_field_is_bad_request() {
        assert_eq!(
            parse_timestamp_ms("start", &Scalar::Num(f64::NAN))
                .expect_err("nan")
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            parse_duration_ms("step", &Scalar::Str("nonsense".to_string()))
                .expect_err("bad")
                .status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn a_timeout_above_the_server_maximum_is_clamped() {
        assert_eq!(parse_deadline(Some(3600.0), MAX).expect("clamped"), MAX);
    }

    #[test]
    fn a_timeout_below_the_server_maximum_is_honored() {
        assert_eq!(
            parse_deadline(Some(5.0), MAX).expect("honored"),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn a_non_positive_timeout_is_rejected() {
        assert_eq!(
            parse_deadline(Some(0.0), MAX).expect_err("rejected").status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn an_unknown_op_type_fails_to_parse() {
        let err = serde_json::from_str::<AnalyticsBody>(
            r#"{"query":"m","start":0,"end":1,"step":"1s","op":{"type":"forecast"}}"#,
        );
        assert!(err.is_err(), "unknown op type must not deserialize");
    }

    /// The series cap and every analytics error map to 422; storage faults
    /// keep the redacted status contract of `/api/v1/query_range`.
    #[test]
    fn status_mapping_is_pinned() {
        assert_eq!(
            ApiError::series_cap(1001).status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ApiError::from_analytics(AnalyticsError::SeriesTooLong {
                max: 2000,
                got: 3000
            })
            .status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ApiError::from_analytics(AnalyticsError::InvalidPercentile { got: 2.0 }).status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ApiError::from_analytics(AnalyticsError::EmptySeries).status,
            StatusCode::UNPROCESSABLE_ENTITY
        );

        assert_eq!(
            ApiError::from_query(QueryError::Parse("boom".to_string())).status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::from_query(QueryError::TooManySeries { count: 9, max: 3 }).status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ApiError::from_query(QueryError::DeadlineExceeded { deadline: MAX }).status,
            StatusCode::GATEWAY_TIMEOUT
        );

        // Storage faults are redacted, never echoed. The redacted messages are
        // single-sourced from ravel-query, so assert against those
        // public constants rather than a local copy.
        let corrupt = ApiError::from_query(QueryError::NonMonotonicSamples { prev: 2, next: 1 });
        assert_eq!(corrupt.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(corrupt.message, ravel_query::http::MSG_CORRUPT);

        let unavailable = ApiError::from_query(QueryError::SnapshotInvalidated);
        assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.message, ravel_query::http::MSG_UNAVAILABLE);
    }

    /// The consent gate's decision table (ADR-0071 amendment, section 2). The
    /// end-to-end behavior is pinned through the real handler in
    /// `tests/analytics_endpoint.rs`; this pins the branch table itself,
    /// including the empty-`skipped` fallback a partial flag with no per-cluster
    /// warning would take.
    #[test]
    fn the_consent_gate_refuses_only_unconsented_partial_coverage() {
        let partial = Coverage::Partial {
            skipped: vec!["remote cluster west unavailable".to_string()],
        };
        gate_partial(&Coverage::Complete, false).expect("complete coverage passes");
        gate_partial(&Coverage::Complete, true).expect("complete coverage passes opted in");
        gate_partial(&partial, true).expect("opted-in partial coverage passes");

        let err = gate_partial(&partial, false).expect_err("unconsented partial is refused");
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.error_type, "unavailable");
        assert!(err.message.contains("west"), "{}", err.message);
        assert!(err.message.contains("allow_partial"), "{}", err.message);

        let bare = ApiError::partial_refusal(&[]);
        assert!(
            bare.message.contains("one or more federated clusters"),
            "{}",
            bare.message
        );
    }
}
