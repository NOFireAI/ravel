//! Prometheus-compatible HTTP handlers (docs/query-engine.md "HTTP API").

use std::collections::{BTreeSet, HashMap};

use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, body::Body};

use ravel_promql::LabelMatcher;
use ravel_types::{LabelSet, SeriesId, TimeRange};

use crate::engine::parse_match_selector;
use crate::http::error::ApiError;
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

pub async fn query(State(state): State<AppState>, req: Request<Body>) -> Response {
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

    let (value, annotations, stats) = state
        .engine
        .instant_with_stats_annotated(tenant_hash, query, time_ms, &min_tokens, now, deadline)
        .await?;
    let (warnings, infos) = annotations.into_parts();
    Ok((
        with_stats(instant_value_to_json(value, time_ms)?, stats),
        warnings,
        infos,
    ))
}

pub async fn query_range(State(state): State<AppState>, req: Request<Body>) -> Response {
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

    let (value, annotations, stats) = state
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
        .await?;
    let (warnings, infos) = annotations.into_parts();
    Ok((
        with_stats(
            range_value_to_json(value, start_ms, end_ms, step_ms)?,
            stats,
        ),
        warnings,
        infos,
    ))
}

fn parse_duration_ms_field(params: &Params) -> Result<i64, ApiError> {
    let raw = params.require("step")?;
    Ok(crate::http::params::parse_duration_ms("step", raw)?)
}

pub async fn labels(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_labels(&state, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_labels(state: &AppState, req: Request<Body>) -> Result<Vec<String>, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers).await?;
    let params = read_params(req).await?;
    let series = resolve_matched_series(state, tenant_hash, &params).await?;

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
    let series = resolve_matched_series(state, tenant_hash, &params).await?;

    let mut values: BTreeSet<String> = BTreeSet::new();
    for (_, labels) in &series {
        if let Some(v) = labels.get(&name) {
            values.insert(v.to_string());
        }
    }
    Ok(values.into_iter().collect())
}

pub async fn series(State(state): State<AppState>, req: Request<Body>) -> Response {
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
    let series = resolve_matched_series(state, tenant_hash, &params).await?;
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

    if selectors.is_empty() {
        let remaining = remaining_budget(request_deadline, deadline)?;
        let series = state
            .engine
            .resolve_series(tenant_hash, &[], window, &min_tokens, now, remaining)
            .await?;
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
        let series = state
            .engine
            .resolve_series(tenant_hash, &matchers, window, &min_tokens, now, remaining)
            .await?;
        for (id, labels) in series {
            by_id.entry(id).or_insert(labels);
        }
    }
    Ok(by_id.into_iter().collect())
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
