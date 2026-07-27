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
    ApiResponse, instant_vector_to_json, range_matrix_to_json, series_to_json,
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
    (StatusCode::OK, Json(ApiResponse::Success { data })).into_response()
}

async fn authenticate(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<ravel_types::TenantHash, ApiError> {
    Ok(state.tenant_resolver.resolve(headers)?.hash())
}

pub async fn query(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_query(&state, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_query(
    state: &AppState,
    req: Request<Body>,
) -> Result<crate::http::json::QueryData, ApiError> {
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

    let vector = state
        .engine
        .instant(tenant_hash, query, time_ms, &min_tokens, now, deadline)
        .await?;
    Ok(instant_vector_to_json(vector))
}

pub async fn query_range(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_query_range(&state, req).await {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

async fn handle_query_range(
    state: &AppState,
    req: Request<Body>,
) -> Result<crate::http::json::QueryData, ApiError> {
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

    let matrix = state
        .engine
        .range(
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
    Ok(range_matrix_to_json(matrix))
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

    if selectors.is_empty() {
        let series = state
            .engine
            .resolve_series(tenant_hash, &[], window, &min_tokens, now, deadline)
            .await?;
        return Ok(series);
    }

    // Deliberate deviation: each match[] selector resolves its own
    // snapshot independently, rather than one shared snapshot for the
    // whole request (docs in the task's pre-approved deviations list).
    let mut by_id: HashMap<SeriesId, LabelSet> = HashMap::new();
    for selector in selectors {
        let matchers: Vec<LabelMatcher> = parse_match_selector(selector)?;
        let series = state
            .engine
            .resolve_series(tenant_hash, &matchers, window, &min_tokens, now, deadline)
            .await?;
        for (id, labels) in series {
            by_id.entry(id).or_insert(labels);
        }
    }
    Ok(by_id.into_iter().collect())
}
