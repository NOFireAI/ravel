//! `POST /api/v1/esql`: one ES|QL statement over the `logs` table (ADR-2887).
//!
//! This module is the transport only. Parsing, binding, the time window and
//! execution live in `ravel_esql` and `ravel_sql::esql`; admission, the usage
//! record, the audit event and cost recording run in
//! [`QueryService::esql_execute`], shared with every other query surface.
//!
//! # Request and response
//!
//! ```json
//! { "query": "FROM logs | LIMIT 10", "start": 1735689600.0, "end": 1735693200.0, "timeout": 15.0 }
//! ```
//!
//! `query` is required. `start` and `end` are optional Unix float seconds and
//! narrow the statement's own window; `timeout` can only lower the server
//! deadline. Any other field is refused with 400, and `params` is refused by
//! name: this endpoint runs no parameterised statement.
//!
//! A success is JSON:
//!
//! ```json
//! {
//!   "status": "success",
//!   "data": { "columns": [{ "name": "n", "type": "long" }], "rows": [[3]] },
//!   "stats": { ... }
//! }
//! ```
//!
//! Each column's `type` is its ES|QL type name (`keyword`, `long`, `double`,
//! `boolean`, `date_nanos`, `null`); a `date_nanos` value is an integer count
//! of nanoseconds since the Unix epoch. `warnings`, an array of strings, is
//! present only when there is something to say: today, that the statement's
//! own `LIMIT` was lowered to [`MAX_LIMIT`]. `stats` has the shape
//! `/api/v1/sql` reports for a statement that reads the catalog, and only
//! `accounting` and `estimate`, all zero, for a `ROW` statement.
//!
//! Errors use the shared body, `{"status":"error","errorType":...,"error":...}`;
//! [`ServiceError::from_esql`](crate::service::ServiceError::from_esql) is the
//! mapping.

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ravel_ingest::Clock;
use ravel_sql::esql::{EsqlRequest, EsqlRun, MAX_LIMIT};
use serde::Deserialize;
use serde_json::json;

use crate::service::{ApiError, EsqlExecution, EsqlQuery, ServiceError};
use crate::sql::SqlState;

/// The request body cap `/api/v1/sql` applies, for the same reason.
const MAX_BODY_BYTES: usize = 64 << 10;

/// The `/api/v1/esql` router, over the SQL surface's state: the statement runs
/// through the same executor, audit sink and admission controller.
pub fn router(state: SqlState) -> Router {
    Router::new()
        .route("/api/v1/esql", post(handle))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EsqlBody {
    query: String,
    /// Window start, Unix float seconds.
    #[serde(default)]
    start: Option<f64>,
    /// Window end, Unix float seconds.
    #[serde(default)]
    end: Option<f64>,
    /// Per-request wall deadline in seconds. Clamped to the server maximum.
    #[serde(default)]
    timeout: Option<f64>,
    /// Whether the body has a `params` key, whatever its value (`null`
    /// included), so it can be refused by name.
    #[serde(default, deserialize_with = "key_present")]
    params: bool,
}

/// `true` for any value, so a field read with it records that its key was
/// present. An absent key takes the field's `Default`, `false`.
fn key_present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    serde::de::IgnoredAny::deserialize(deserializer).map(|_| true)
}

async fn handle(State(state): State<SqlState>, req: Request<Body>) -> Response {
    match run(&state, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

/// Authenticate before the body is read, so an anonymous caller reads no body
/// and takes no permit; then decode the body and hand the statement to the
/// query service.
async fn run(state: &SqlState, req: Request<Body>) -> Result<Response, ServiceError> {
    let principal =
        crate::service::authenticate_principal(state.tenant_resolver.as_ref(), req.headers())?;
    let tenant_hash = principal.tenant.hash();

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let body: EsqlBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON request body: {e}")))?;
    let query = build_query(body, state.clock.now_ns(), state.max_deadline)?;

    let execution = state.service().esql_execute(tenant_hash, &query).await?;
    respond(execution, tenant_hash)
}

/// Turn the request body into an [`EsqlQuery`].
fn build_query(
    body: EsqlBody,
    now_ns: i64,
    max_deadline: std::time::Duration,
) -> Result<EsqlQuery, ApiError> {
    if body.params {
        return Err(ApiError::bad_request(
            "\"params\" is not supported: write each value into the statement".to_string(),
        ));
    }
    let start = body
        .start
        .map(|secs| crate::sql::seconds_to_ns("start", secs))
        .transpose()?;
    let end = body
        .end
        .map(|secs| crate::sql::seconds_to_ns("end", secs))
        .transpose()?;
    if let (Some(start), Some(end)) = (start, end)
        && start > end
    {
        return Err(ApiError::bad_request(
            "\"start\" must not be after \"end\"".to_string(),
        ));
    }
    let deadline = crate::sql::clamp_timeout(body.timeout, max_deadline)?;
    Ok(EsqlQuery {
        query: body.query,
        now_ns,
        start,
        end,
        request: EsqlRequest {
            min_tokens: Vec::new(),
            deadline,
            budgets: None,
            max_rows: None,
        },
    })
}

/// The JSON success body: the rows under their ES|QL column types, `stats`,
/// and `warnings` when there are any.
fn respond(
    execution: EsqlExecution,
    tenant_hash: ravel_types::TenantHash,
) -> Result<Response, ServiceError> {
    let EsqlExecution {
        outcome,
        accounting,
        estimate,
        audit_elapsed,
    } = execution;

    let mut warnings: Vec<String> = Vec::new();
    if let Some(limit) = outcome.clamped_limit {
        warnings.push(format!(
            "the statement's LIMIT {limit} was lowered to {MAX_LIMIT}, the most rows an ES|QL query returns"
        ));
    }
    let stats = match &outcome.run {
        EsqlRun::Catalog(sql) => {
            crate::sql::record_postings(sql);
            warnings.extend(sql.warnings());
            crate::sql::esql_stats_json(sql, audit_elapsed)
        }
        EsqlRun::Row(_) => crate::query::accounting_stats_json(&accounting, &estimate),
    };

    let mut data = outcome
        .output()
        .to_json()
        .map_err(|err| ServiceError::from_sql(err, tenant_hash))?;
    if let serde_json::Value::Object(ref mut map) = data {
        let columns: Vec<serde_json::Value> = outcome
            .columns
            .iter()
            .map(|column| json!({ "name": column.name, "type": column.ty.name() }))
            .collect();
        map.insert("columns".to_string(), serde_json::Value::Array(columns));
    }

    let mut body = json!({ "status": "success", "data": data, "stats": stats });
    if !warnings.is_empty()
        && let serde_json::Value::Object(ref mut map) = body
    {
        map.insert("warnings".to_string(), json!(warnings));
    }
    Ok((StatusCode::OK, axum::Json(body)).into_response())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const MAX: std::time::Duration = std::time::Duration::from_secs(30);

    fn body(json: &str) -> Result<EsqlBody, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn an_absent_window_leaves_both_bounds_to_the_statement() {
        let query =
            build_query(body(r#"{"query":"ROW a = 1"}"#).expect("body"), 7, MAX).expect("query");
        assert_eq!((query.start, query.end, query.now_ns), (None, None, 7));
        assert_eq!(query.request.deadline, MAX);
        assert_eq!(query.request.max_rows, None);
    }

    #[test]
    fn seconds_convert_to_nanoseconds_and_timeout_is_clamped() {
        let query = build_query(
            body(r#"{"query":"q","start":1.5,"end":2.5,"timeout":3600}"#).expect("body"),
            0,
            MAX,
        )
        .expect("query");
        assert_eq!(query.start, Some(1_500_000_000));
        assert_eq!(query.end, Some(2_500_000_000));
        assert_eq!(query.request.deadline, MAX);
    }

    #[test]
    fn an_inverted_window_is_rejected() {
        let err = build_query(
            body(r#"{"query":"q","start":10,"end":1}"#).expect("body"),
            0,
            MAX,
        )
        .expect_err("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn params_are_refused_by_name() {
        for json in [
            r#"{"query":"ROW a = ?x","params":[{"x":1}]}"#,
            r#"{"query":"ROW a = 1","params":null}"#,
            r#"{"query":"ROW a = 1","params":[]}"#,
        ] {
            let err = build_query(body(json).expect("body"), 0, MAX).expect_err(json);
            assert_eq!(err.status, StatusCode::BAD_REQUEST, "{json}");
            assert!(
                err.message.contains("\"params\""),
                "{json}: {}",
                err.message
            );
        }
    }

    #[test]
    fn an_unknown_field_does_not_decode() {
        let err = body(r#"{"query":"q","filter":{}}"#).expect_err("unknown field");
        assert!(err.to_string().contains("filter"), "{err}");
    }
}
