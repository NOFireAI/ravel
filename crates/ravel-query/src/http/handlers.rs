//! Prometheus-compatible HTTP handlers (docs/query-engine.md "HTTP API").

use std::collections::{BTreeSet, HashMap};

use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, body::Body};

use ravel_maintain::{QueryStatus, query_audit_event};
use ravel_promql::LabelMatcher;
use ravel_types::accounting::{CostEstimate, QueryAccountingSnapshot, QueryWorkloadClass};
use ravel_types::{LabelSet, SeriesId, TenantHash, TimeRange};

use crate::engine::parse_match_selector;
use crate::http::error::{ApiError, MSG_UNAVAILABLE};
use crate::http::json::{
    ApiResponse, QueryResponseData, instant_value_to_json, range_value_to_json, series_to_json,
    with_stats,
};
use crate::http::params::{Params, decode_commit_tokens, parse_deadline, parse_timestamp_ms};
use crate::http::{AppState, ONE_HOUR_NS};

/// Caps the size of a request body read into memory. There is no
/// Prometheus-mandated limit; this is a defensive bound for a JSON-free,
/// form-encoded body of matcher/timestamp parameters, which are never
/// large in legitimate use.
const MAX_BODY_BYTES: usize = 1 << 20;

async fn read_params(req: Request<Body>) -> Result<Params, ApiError> {
    let (parts, body) = req.into_parts();
    let query_string = parts.uri.query().map(str::to_string);
    let body_bytes = if parts.method == Method::POST {
        Some(
            axum::body::to_bytes(body, MAX_BODY_BYTES)
                .await
                .map_err(|e| ApiError::BadData(e.to_string()))?,
        )
    } else {
        None
    };
    Ok(Params::parse(
        query_string.as_deref(),
        body_bytes.as_deref(),
    ))
}

/// The response for a query refused by the fleet-global concurrency ceiling
/// (ADR-0061 decision 2): a retryable HTTP 503 in the same Prometheus error
/// envelope every other rejection here uses. The refusal happens before any
/// snapshot resolve or object GET, so no work was done on the query's behalf.
fn concurrency_rejected_response() -> Response {
    let body: ApiResponse<()> = ApiResponse::Error {
        error_type: "unavailable",
        error: "fleet query concurrency ceiling reached; retry".to_string(),
    };
    (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
}

fn now_ns() -> i64 {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX)
}

fn success<T: serde::Serialize>(data: T) -> Response {
    (StatusCode::OK, Json(ApiResponse::success(data))).into_response()
}

/// Like [`success`], but carries the query's evaluation annotations into the
/// envelope's separate `warnings` and `infos` arrays (issue #178). Both are
/// omitted from the wire when empty (Prometheus' `omitempty`).
fn success_annotated<T: serde::Serialize>(
    data: T,
    warnings: Vec<String>,
    infos: Vec<String>,
) -> Response {
    (
        StatusCode::OK,
        Json(ApiResponse::success_with_annotations(data, warnings, infos)),
    )
        .into_response()
}

async fn authenticate(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<ravel_types::TenantHash, ApiError> {
    Ok(state.tenant_resolver.resolve(headers)?.hash())
}

/// Submit one evidential audit event for a query that reached execution for a
/// resolved tenant, await its durability, and only then release `result` to
/// the caller (ADR-0062 §2a, epic EL / issue #762).
///
/// `result` is the outcome of the surface's actual read: `Ok` audits
/// `QueryStatus::Ok`, any `Err` audits `QueryStatus::Error`, so the audit trail
/// records the attempt regardless of outcome, exactly as the SQL HTTP path
/// does. The event is submitted through [`AppState::audit_sink`] and its
/// durability is awaited before this returns, so a completed handler never
/// releases its response ahead of its own audit record. When submission fails
/// (`audit_mode=required` surfacing a flush error, or a stopped pipeline) the
/// request fails closed with a retryable 503 rather than running unaudited --
/// the deliberate inversion of "queries outlive the trail". A request rejected
/// before it reached here (auth, param parse) never calls this and is not
/// audited: there is no executed read to attribute.
async fn audit_and_release<T>(
    state: &AppState,
    tenant_hash: TenantHash,
    query_text: &str,
    language: &str,
    window: (i64, i64),
    now_ns: i64,
    result: Result<T, ApiError>,
) -> Result<T, ApiError> {
    let status = if result.is_ok() {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };
    let event = query_audit_event(
        &tenant_hash,
        now_ns,
        query_text,
        language,
        status,
        window.0,
        window.1,
    );
    state
        .audit_sink
        .submit(event)
        .await
        .map_err(|_| ApiError::Unavailable(MSG_UNAVAILABLE.to_string()))?;
    result
}

/// Nanosecond form of a millisecond timestamp for an audit window bound,
/// saturating rather than overflowing (the audit record is best-effort
/// informational for the window; the query itself already validated its
/// range).
fn ms_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

pub async fn query(State(state): State<AppState>, req: Request<Body>) -> Response {
    // Fleet-global concurrency admission (ADR-0061 decision 2): decide before any
    // resolve or GET. The permit is held for the whole handler and released on
    // return (success or error) or on a dropped request future, by its `Drop`.
    let _permit = match state.query_admission.try_admit() {
        Ok(permit) => permit,
        Err(_) => return concurrency_rejected_response(),
    };
    match handle_query(&state, req).await {
        Ok((data, warnings, infos)) => success_annotated(data, warnings, infos),
        Err(e) => e.into_response(),
    }
}

async fn handle_query(
    state: &AppState,
    req: Request<Body>,
) -> Result<(QueryResponseData, Vec<String>, Vec<String>), ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;

    let query = params.require("query")?;
    let now = now_ns();
    let time_ms = match params.first("time") {
        Some(s) => parse_timestamp_ms("time", s)?,
        None => now / 1_000_000,
    };
    let min_tokens = decode_commit_tokens(params.all("min_commit_token"))?;
    let deadline = parse_deadline(&params, state.engine.config().deadline)?;

    // The query now reaches execution for a resolved tenant, so it is
    // auditable: run it, then submit one audit event for the outcome and await
    // its durability before releasing the response (ADR-0062 §2a). The instant
    // query's window is the single evaluation instant, recorded as a zero-width
    // range.
    let exec = state
        .engine
        .instant_with_stats_annotated(tenant_hash, query, time_ms, &min_tokens, now, deadline)
        .await
        .map_err(ApiError::from);
    let time_ns = ms_to_ns(time_ms);
    let (value, annotations, stats) = audit_and_release(
        state,
        tenant_hash,
        query,
        "promql",
        (time_ns, time_ns),
        now,
        exec,
    )
    .await?;
    let (mut warnings, infos) = annotations.into_parts();
    // Merge the federation fan-out's partial-coverage warnings (ADR-0071, issue
    // #868) into the top-level `warnings` array alongside the evaluator's own
    // annotations, so a `skip_unavailable=true` federated query returns a
    // response a client can tell is partial (the `partial` stats marker) and
    // that names the skipped cluster. These are already redacted at the
    // federation seam: they name the operator-facing cluster only, never its
    // endpoint or transport error text.
    for w in &stats.warnings {
        if !warnings.contains(w) {
            warnings.push(w.clone());
        }
    }
    // Copy the counters out before `stats` is moved into the response body, so
    // the numbers folded into /metrics are exactly the numbers the response
    // reports; recording after the JSON render succeeds means a completed,
    // fully-rendered query is what gets counted (ADR-0044 section 4, issue
    // #425). Both are `Copy`.
    let accounting = stats.accounting;
    let estimate = stats.estimate;
    let data = with_stats(instant_value_to_json(value, time_ms)?, stats);
    state.cost_recorder.record(
        &accounting,
        &estimate,
        tenant_hash,
        QueryWorkloadClass::Interactive,
    );
    Ok((data, warnings, infos))
}

pub async fn query_range(State(state): State<AppState>, req: Request<Body>) -> Response {
    let _permit = match state.query_admission.try_admit() {
        Ok(permit) => permit,
        Err(_) => return concurrency_rejected_response(),
    };
    match handle_query_range(&state, req).await {
        Ok((data, warnings, infos)) => success_annotated(data, warnings, infos),
        Err(e) => e.into_response(),
    }
}

async fn handle_query_range(
    state: &AppState,
    req: Request<Body>,
) -> Result<(QueryResponseData, Vec<String>, Vec<String>), ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;

    let query = params.require("query")?;
    let start_ms = parse_timestamp_ms("start", params.require("start")?)?;
    let end_ms = parse_timestamp_ms("end", params.require("end")?)?;
    let step_ms = parse_duration_ms_field(&params)?;
    let min_tokens = decode_commit_tokens(params.all("min_commit_token"))?;
    let deadline = parse_deadline(&params, state.engine.config().deadline)?;
    let now = now_ns();

    // Auditable once the range evaluation runs for a resolved tenant: run it,
    // then submit one audit event for the outcome and await durability before
    // releasing the response (ADR-0062 §2a). The recorded window is the
    // request's resolved `[start, end]` range.
    let exec = state
        .engine
        .range_with_stats_annotated(
            tenant_hash,
            query,
            start_ms,
            end_ms,
            step_ms,
            &min_tokens,
            now,
            deadline,
        )
        .await
        .map_err(ApiError::from);
    let (value, annotations, stats) = audit_and_release(
        state,
        tenant_hash,
        query,
        "promql",
        (ms_to_ns(start_ms), ms_to_ns(end_ms)),
        now,
        exec,
    )
    .await?;
    let (mut warnings, infos) = annotations.into_parts();
    // Merge the federation fan-out's partial-coverage warnings (ADR-0071, issue
    // #868) into the top-level `warnings` array, same as handle_query. Already
    // redacted at the federation seam (cluster name only, no endpoint/errno).
    for w in &stats.warnings {
        if !warnings.contains(w) {
            warnings.push(w.clone());
        }
    }
    // See handle_query: record the same counters the response reports, once
    // the range payload has rendered.
    let accounting = stats.accounting;
    let estimate = stats.estimate;
    let data = with_stats(
        range_value_to_json(value, start_ms, end_ms, step_ms)?,
        stats,
    );
    state.cost_recorder.record(
        &accounting,
        &estimate,
        tenant_hash,
        QueryWorkloadClass::Interactive,
    );
    Ok((data, warnings, infos))
}

fn parse_duration_ms_field(params: &Params) -> Result<i64, ApiError> {
    let raw = params.require("step")?;
    Ok(crate::http::params::parse_duration_ms("step", raw)?)
}

pub async fn labels(State(state): State<AppState>, req: Request<Body>) -> Response {
    let _permit = match state.query_admission.try_admit() {
        Ok(permit) => permit,
        Err(_) => return concurrency_rejected_response(),
    };
    match handle_labels(&state, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_labels(state: &AppState, req: Request<Body>) -> Result<Vec<String>, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;
    let series = resolve_and_audit_series(state, tenant_hash, &params, "labels").await?;

    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, labels) in &series {
        for label in labels.iter() {
            names.insert(label.name.clone());
        }
    }
    Ok(names.into_iter().collect())
}

pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    req: Request<Body>,
) -> Response {
    let _permit = match state.query_admission.try_admit() {
        Ok(permit) => permit,
        Err(_) => return concurrency_rejected_response(),
    };
    match handle_label_values(&state, name, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_label_values(
    state: &AppState,
    name: String,
    req: Request<Body>,
) -> Result<Vec<String>, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;
    let series = resolve_and_audit_series(state, tenant_hash, &params, "labels").await?;

    let mut values: BTreeSet<String> = BTreeSet::new();
    for (_, labels) in &series {
        if let Some(v) = labels.get(&name) {
            values.insert(v.to_string());
        }
    }
    Ok(values.into_iter().collect())
}

pub async fn series(State(state): State<AppState>, req: Request<Body>) -> Response {
    let _permit = match state.query_admission.try_admit() {
        Ok(permit) => permit,
        Err(_) => return concurrency_rejected_response(),
    };
    match handle_series(&state, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_series(
    state: &AppState,
    req: Request<Body>,
) -> Result<Vec<HashMap<String, String>>, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;
    if params.all("match[]").is_empty() {
        return Err(ApiError::BadData(
            "missing required parameter \"match[]\"".to_string(),
        ));
    }
    let series = resolve_and_audit_series(state, tenant_hash, &params, "series").await?;
    Ok(series_to_json(series))
}

fn resolve_window(params: &Params, now: i64) -> Result<TimeRange, ApiError> {
    let start_ns = match params.first("start") {
        Some(s) => parse_timestamp_ms("start", s)?
            .checked_mul(1_000_000)
            .ok_or_else(|| ApiError::BadData("start out of range".to_string()))?,
        None => now.saturating_sub(ONE_HOUR_NS),
    };
    let end_ns = match params.first("end") {
        Some(s) => parse_timestamp_ms("end", s)?
            .checked_mul(1_000_000)
            .ok_or_else(|| ApiError::BadData("end out of range".to_string()))?,
        None => now,
    };
    Ok(TimeRange { start_ns, end_ns })
}

/// Resolve the matched series for a metadata request (labels / label_values /
/// series) and audit the outcome before releasing it (ADR-0062 §2a). The
/// metadata surfaces carry no single query string; the audited text is the
/// request's `match[]` selectors joined, and the audited window is the resolved
/// `[start, end]` these surfaces read over. `language` distinguishes the
/// surface (`labels` or `series`) so the record shape stays one schema.
async fn resolve_and_audit_series(
    state: &AppState,
    tenant_hash: TenantHash,
    params: &Params,
    language: &str,
) -> Result<Vec<(SeriesId, LabelSet)>, ApiError> {
    // Window bounds parse before any read; a bad `start`/`end` is a request
    // error, not an executed read, so it is not audited (it returns here).
    let now = now_ns();
    let window = resolve_window(params, now)?;
    let selectors_text = params.all("match[]").join("; ");
    let result = resolve_matched_series(state, tenant_hash, params).await;
    audit_and_release(
        state,
        tenant_hash,
        &selectors_text,
        language,
        (window.start_ns, window.end_ns),
        now,
        result,
    )
    .await
}

async fn resolve_matched_series(
    state: &AppState,
    tenant_hash: ravel_types::TenantHash,
    params: &Params,
) -> Result<Vec<(SeriesId, LabelSet)>, ApiError> {
    let now = now_ns();
    let window = resolve_window(params, now)?;
    let min_tokens = decode_commit_tokens(params.all("min_commit_token"))?;
    let deadline = parse_deadline(params, state.engine.config().deadline)?;
    let selectors = params.all("match[]");

    // The wall deadline is a per-query budget (docs/query-engine.md
    // "Budgets"), and one metadata request is one query. Convert the
    // duration into a single absolute instant computed once here, then hand
    // each resolve_series call only the time still remaining. Without this,
    // each match[] selector would be granted the full `deadline` afresh, so
    // N selectors would get N times the documented budget with no aggregate
    // cap (finding a7-F03). The engine still enforces the timeout per call;
    // sharing the remaining budget makes the whole request share one wall
    // bound.
    let request_deadline = tokio::time::Instant::now() + deadline;

    // Accumulates every sub-query's cost so the whole metadata request folds
    // once into the /metrics aggregator (ADR-0044 section 4, issue #425). Each
    // match[] selector resolves under its own accounting handle, so the
    // request total is the field-wise sum of the per-selector snapshots.
    // Recording once per request, not once per selector, keeps
    // ravel_query_queries_total counting requests.
    let mut combined: Option<(QueryAccountingSnapshot, CostEstimate)> = None;

    if selectors.is_empty() {
        let remaining = remaining_budget(request_deadline, deadline)?;
        let (series, stats) = state
            .engine
            .resolve_series_with_stats(tenant_hash, &[], window, &min_tokens, now, remaining)
            .await?;
        accumulate_cost(&mut combined, &stats);
        record_combined(state, tenant_hash, combined);
        return Ok(series);
    }

    // Deliberate deviation: each match[] selector resolves its own
    // snapshot independently, rather than one shared snapshot for the
    // whole request (docs in the task's pre-approved deviations list). The
    // shared wall budget above is orthogonal to that: snapshots stay
    // per-selector, but all selectors draw down one deadline.
    //
    // Per-selector segment stats (docs/metric-index-plan.md P5b) are not
    // surfaced on this path: the labels/label_values/series endpoints have
    // no established response envelope for it (only the value-bearing
    // query/query_range endpoints do; see this ticket's final report), and
    // aggregating per-selector counts here would double count segments any
    // two selectors both matched.
    let mut by_id: HashMap<SeriesId, LabelSet> = HashMap::new();
    for selector in selectors {
        let matchers: Vec<LabelMatcher> = parse_match_selector(selector)?;
        let remaining = remaining_budget(request_deadline, deadline)?;
        let (series, stats) = state
            .engine
            .resolve_series_with_stats(tenant_hash, &matchers, window, &min_tokens, now, remaining)
            .await?;
        accumulate_cost(&mut combined, &stats);
        for (id, labels) in series {
            by_id.entry(id).or_insert(labels);
        }
    }
    record_combined(state, tenant_hash, combined);
    Ok(by_id.into_iter().collect())
}

/// Fold one sub-query's [`QueryStats`](crate::QueryStats) into the request's
/// running cost total. The counters sum and the estimate sums
/// ([`QueryAccountingSnapshot::saturating_add`]); the first call seeds the
/// total because [`CostEstimate`] has no zero value (an estimate is only ever
/// a real resolved snapshot's, ADR-0044).
fn accumulate_cost(
    combined: &mut Option<(QueryAccountingSnapshot, CostEstimate)>,
    stats: &crate::QueryStats,
) {
    *combined = Some(match combined.take() {
        None => (stats.accounting, stats.estimate),
        Some((accounting, estimate)) => (
            accounting.saturating_add(&stats.accounting),
            estimate.saturating_add(&stats.estimate),
        ),
    });
}

/// Record the whole metadata request's summed cost into the aggregator, once.
/// `combined` is `Some` whenever any resolve ran (every path above runs at
/// least one), so a request that reached here without an error records exactly
/// one query; a request that failed a resolve returned earlier through `?` and
/// records nothing, the same rule the value-bearing handlers follow.
fn record_combined(
    state: &AppState,
    tenant_hash: ravel_types::TenantHash,
    combined: Option<(QueryAccountingSnapshot, CostEstimate)>,
) {
    if let Some((accounting, estimate)) = combined {
        state.cost_recorder.record(
            &accounting,
            &estimate,
            tenant_hash,
            QueryWorkloadClass::Interactive,
        );
    }
}

/// Time left in the shared request wall budget, or a `DeadlineExceeded`
/// error once it is spent. `configured` is the whole-request budget and is
/// reported in the error so the client sees the query's deadline, not the
/// residual slice handed to the last selector.
fn remaining_budget(
    request_deadline: tokio::time::Instant,
    configured: std::time::Duration,
) -> Result<std::time::Duration, ApiError> {
    let remaining = request_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(crate::QueryError::DeadlineExceeded {
            deadline: configured,
        }
        .into());
    }
    Ok(remaining)
}
