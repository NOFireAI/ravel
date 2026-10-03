//! Prometheus-compatible HTTP handlers (docs/query-engine.md "HTTP API").
//!
//! Every handler here is the same four steps: parse the request, authenticate
//! it, hand it to the query service layer
//! ([`crate::http::service`]), and encode the outcome. Admission,
//! deadline and budget clamping, usage accounting, the audit event, the
//! partial-coverage consent gate, and error redaction all live in the service
//! layer, so this transport and the SQL, analytics, exemplars and (issue
//! #1381) MCP transports run one implementation of them rather than five.
//!
//! Authentication precedes admission: an anonymous caller is rejected before
//! it can consume a permit from the fleet-global concurrency ceiling.

use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, body::Body};

use ravel_promql::LabelMatcher;
use ravel_types::{METRIC_NAME_LABEL, TenantHash, TimeRange};

use crate::engine::parse_match_selector;
use crate::http::error::ApiError;
use crate::http::json::{
    ApiResponse, instant_value_to_json, range_value_to_json, series_to_json, with_stats,
};
use crate::http::params::{
    Params, decode_commit_tokens, parse_allow_partial, parse_deadline, parse_timestamp_ms,
};
use crate::http::service::{self, InstantRequest, MetadataRequest, RangeRequest};
use crate::http::{AppState, ONE_HOUR_NS};
use crate::log_series;

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

/// Carries a successful response's data into the Prometheus JSON envelope, with
/// the query's evaluation and federation annotations in the separate `warnings`
/// and `infos` arrays (ADR-0071). Both are omitted from the wire
/// when empty (Prometheus' `omitempty`), so an unannotated response is
/// byte-identical to a bare `{"status":"success","data":...}`.
fn success_annotated<T: serde::Serialize>(
    data: T,
    partial: bool,
    warnings: Vec<String>,
    infos: Vec<String>,
) -> Response {
    (
        StatusCode::OK,
        Json(ApiResponse::success_with_annotations(
            data, partial, warnings, infos,
        )),
    )
        .into_response()
}

fn authenticate(state: &AppState, headers: &axum::http::HeaderMap) -> Result<TenantHash, ApiError> {
    service::authenticate(state.tenant_resolver.as_ref(), headers)
}

pub async fn query(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_query(&state, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn handle_query(state: &AppState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;
    let params = read_params(req).await?;
    let now = now_ns();
    let request = InstantRequest {
        query: params.require("query")?.to_string(),
        time_ms: match params.first("time") {
            Some(s) => parse_timestamp_ms("time", s)?,
            None => now / 1_000_000,
        },
        min_tokens: decode_commit_tokens(params.all("min_commit_token"))?,
        deadline: parse_deadline(&params, state.engine.config().deadline)?,
        allow_partial: parse_allow_partial(&params),
        now_ns: now,
        budgets: None,
    };
    let outcome =
        service::promql_instant(&state.controls(), &state.engine, tenant_hash, &request).await?;
    let data = with_stats(
        instant_value_to_json(outcome.value, request.time_ms)?,
        outcome.stats,
    );
    Ok(success_annotated(
        data,
        outcome.partial,
        outcome.warnings,
        outcome.infos,
    ))
}

pub async fn query_range(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_query_range(&state, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn handle_query_range(state: &AppState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;
    let params = read_params(req).await?;
    let request = RangeRequest {
        query: params.require("query")?.to_string(),
        start_ms: parse_timestamp_ms("start", params.require("start")?)?,
        end_ms: parse_timestamp_ms("end", params.require("end")?)?,
        step_ms: parse_duration_ms_field(&params)?,
        min_tokens: decode_commit_tokens(params.all("min_commit_token"))?,
        deadline: parse_deadline(&params, state.engine.config().deadline)?,
        allow_partial: parse_allow_partial(&params),
        now_ns: now_ns(),
        budgets: None,
    };
    let outcome =
        service::promql_range(&state.controls(), &state.engine, tenant_hash, &request).await?;
    let data = with_stats(
        range_value_to_json(
            outcome.value,
            request.start_ms,
            request.end_ms,
            request.step_ms,
        )?,
        outcome.stats,
    );
    Ok(success_annotated(
        data,
        outcome.partial,
        outcome.warnings,
        outcome.infos,
    ))
}

fn parse_duration_ms_field(params: &Params) -> Result<i64, ApiError> {
    let raw = params.require("step")?;
    Ok(crate::http::params::parse_duration_ms("step", raw)?)
}

pub async fn labels(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_labels(&state, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn handle_labels(state: &AppState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;
    let params = read_params(req).await?;
    let request = metadata_request(state, &params)?;
    let outcome = service::labels(&state.controls(), &state.engine, tenant_hash, &request).await?;
    Ok(success_annotated(
        outcome.names,
        outcome.partial,
        outcome.warnings,
        Vec::new(),
    ))
}

pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    req: Request<Body>,
) -> Response {
    match handle_label_values(&state, name, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn handle_label_values(
    state: &AppState,
    name: String,
    req: Request<Body>,
) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;
    let params = read_params(req).await?;
    let request = metadata_request(state, &params)?;
    let include_log_metrics = include_log_metric_names(&name, &request.selectors)?;
    let outcome = service::label_values(
        &state.controls(),
        &state.engine,
        tenant_hash,
        &request,
        &name,
        include_log_metrics,
    )
    .await?;
    Ok(success_annotated(
        outcome.values,
        outcome.partial,
        outcome.warnings,
        Vec::new(),
    ))
}

/// Whether a `label/{label_name}/values` answer should include the two
/// reserved log metric names (ADR-1103 decision 4). The two names never
/// appear in any stored postings, so `label/__name__/values` must add them
/// explicitly rather than discover them by scanning series; every other
/// label name returns `false` without inspecting `selectors` at all, since
/// the reserved names are metric names, not values of some other label.
///
/// For `label_name == __name__`, the rule is: include both names when
/// `selectors` is empty, or when at least one selector is a log selector
/// (a `__name__` matcher naming one of the two reserved metrics); a
/// selector set that names only metrics gets metrics names only. An entry
/// in `selectors` that fails to parse as a `match[]` selector returns the
/// same [`ApiError`] the label-values handler surfaces today.
///
/// This is the one implementation of that rule, extracted (issue #1571) so
/// the MCP `ravel_find_labels` tool can call it before reaching the
/// metadata path instead of re-deriving the condition on its own: a second,
/// independently written copy of "empty or log selector" would silently
/// drift from the HTTP answer for the same selector the moment either side
/// changed, with nothing to catch it.
pub fn include_log_metric_names(label_name: &str, selectors: &[String]) -> Result<bool, ApiError> {
    if label_name != METRIC_NAME_LABEL {
        return Ok(false);
    }
    if selectors.is_empty() {
        return Ok(true);
    }
    for selector in selectors {
        let matchers: Vec<LabelMatcher> = parse_match_selector(selector)?;
        if log_series::log_metric_of(&matchers).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn series(State(state): State<AppState>, req: Request<Body>) -> Response {
    match handle_series(&state, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn handle_series(state: &AppState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;
    let params = read_params(req).await?;
    if params.all("match[]").is_empty() {
        return Err(ApiError::BadData(
            "missing required parameter \"match[]\"".to_string(),
        ));
    }
    let request = metadata_request(state, &params)?;
    let outcome = service::series(&state.controls(), &state.engine, tenant_hash, &request).await?;
    Ok(success_annotated(
        series_to_json(outcome.series),
        outcome.partial,
        outcome.warnings,
        Vec::new(),
    ))
}

/// The parsed form of a metadata request (`labels`, `label_values`,
/// `series`). Every field a bad `start`/`end`/`timeout`/`min_commit_token`
/// could reject is parsed here, before the service layer takes a permit, so a
/// malformed request is a plain 400 that consumed nothing. That includes each
/// `match[]` selector: the service layer re-parses selectors on its own path
/// (it needs the matchers, not just a yes/no), but validating them here first
/// means a malformed selector is rejected before `admit()` and before any
/// audit event, the same as every other field.
fn metadata_request(state: &AppState, params: &Params) -> Result<MetadataRequest, ApiError> {
    let now = now_ns();
    let selectors = params.all("match[]").to_vec();
    for selector in &selectors {
        parse_match_selector(selector)?;
    }
    Ok(MetadataRequest {
        selectors,
        window: resolve_window(params, now)?,
        min_tokens: decode_commit_tokens(params.all("min_commit_token"))?,
        deadline: parse_deadline(params, state.engine.config().deadline)?,
        allow_partial: parse_allow_partial(params),
        now_ns: now,
        budgets: None,
    })
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod include_log_metric_names_tests {
    //! Issue #1571 T2: this is a MOVE, not a copy, of the derivation the
    //! label-values handler used to keep inline. These tests pin the exact
    //! documented result for every selector shape ADR-1103 decision 4 names,
    //! plus the one it does not apply to, and the parse-error passthrough, so
    //! a later change to the rule (here or in a re-derivation) is caught
    //! rather than silently drifting between the HTTP and MCP callers.
    use super::include_log_metric_names;
    use crate::engine::parse_match_selector;
    use crate::http::error::ApiError;
    use ravel_types::METRIC_NAME_LABEL;

    const LOG_SELECTOR: &str = r#"{__name__="ravel_log_lines"}"#;
    const METRICS_SELECTOR: &str = r#"{__name__="cpu_usage"}"#;
    const UNPARSEABLE_SELECTOR: &str = "{";

    #[test]
    fn no_selectors_includes_log_metric_names() {
        let got = include_log_metric_names(METRIC_NAME_LABEL, &[]).expect("no parse to fail");
        assert!(
            got,
            "no match[] selectors must include both reserved log metric names"
        );
    }

    #[test]
    fn log_selector_includes_log_metric_names() {
        let selectors = vec![LOG_SELECTOR.to_string()];
        let got = include_log_metric_names(METRIC_NAME_LABEL, &selectors).expect("selector parses");
        assert!(
            got,
            "a log selector among match[] must include both reserved log metric names"
        );
    }

    #[test]
    fn metrics_only_selector_excludes_log_metric_names() {
        let selectors = vec![METRICS_SELECTOR.to_string()];
        let got = include_log_metric_names(METRIC_NAME_LABEL, &selectors).expect("selector parses");
        assert!(
            !got,
            "match[] selectors that name only metrics must exclude both reserved log metric names"
        );
    }

    #[test]
    fn other_label_name_never_includes_log_metric_names() {
        let shapes: Vec<Vec<String>> = vec![
            Vec::new(),
            vec![LOG_SELECTOR.to_string()],
            vec![METRICS_SELECTOR.to_string()],
        ];
        for selectors in shapes {
            let got = include_log_metric_names("job", &selectors)
                .expect("the job label short-circuits before parsing any selector");
            assert!(
                !got,
                "a label other than __name__ must never include the reserved log metric \
                 names, selectors: {selectors:?}"
            );
        }
    }

    #[test]
    fn unparseable_selector_is_rejected_with_the_handler_error() {
        let selectors = vec![UNPARSEABLE_SELECTOR.to_string()];
        let got = include_log_metric_names(METRIC_NAME_LABEL, &selectors)
            .expect_err("an unparseable match[] selector must be rejected");
        let want = parse_match_selector(UNPARSEABLE_SELECTOR)
            .expect_err("fixture selector must itself be unparseable");
        match got {
            ApiError::BadData(msg) => assert_eq!(
                msg,
                want.to_string(),
                "must surface the same message the handler's own parse produces"
            ),
            other => panic!("expected ApiError::BadData, got {other:?}"),
        }
    }
}
