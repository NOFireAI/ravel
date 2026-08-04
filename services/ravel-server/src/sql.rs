//! `POST /api/v1/sql`: the read-only SQL endpoint (ticket B3, issue #22,
//! docs/arrow-datafusion-plan.md section 2).
//!
//! This module is the transport and the error-to-HTTP boundary; every
//! semantic decision lives in ravel-sql. That split is what keeps ADR-0013's
//! structural isolation honest: nothing here links datafusion, and nothing in
//! ravel-sql links axum. Results cross the boundary as an opaque
//! `QueryOutput` that encodes itself to Arrow IPC or JSON.
//!
//! # Error redaction (a7-F02, applied to a second boundary)
//!
//! crates/ravel-query/src/http/error.rs establishes the discipline for the
//! PromQL path: storage-layer faults get a fixed, class-specific client
//! message, and the full `Display` -- which embeds the physical object key,
//! the tenant hash inside it, and raw backend text -- is logged server-side
//! only. `/api/v1/sql` is an independent boundary with the same obligation
//! plus one of its own: DataFusion planning and execution errors carry
//! schema, column, and plan-fragment detail, and can wrap a ravel error
//! carrying an object key.
//!
//! The redaction decision itself lives in `SqlError::client_message`
//! (ravel-sql), so exactly one place decides what a caller may see; this
//! module only maps [`ErrorClass`] to a status code and logs the full error.
//! Formatting an error here with `{err}` would bypass the boundary, so it is
//! never done.
//!
//! # Request and response
//!
//! Request body is JSON rather than the form encoding the Prometheus-shaped
//! endpoints use: SQL text is awkward to percent-encode and a JSON array is
//! the natural shape for repeated `min_commit_token` values.
//!
//! ```json
//! {
//!   "query": "SELECT ts, value FROM samples ORDER BY ts LIMIT 10",
//!   "start": 1735689600.0,
//!   "end":   1735693200.0,
//!   "timeout": 15.0,
//!   "min_commit_token": ["..."]
//! }
//! ```
//!
//! `start`/`end` are Unix float seconds (the Prometheus convention used by
//! the other endpoints) and bound the commit listing only; every predicate is
//! still re-applied above the scan. `timeout` can only *lower* the server
//! deadline, never raise it (finding a7-F01).
//!
//! The response encoding follows `Accept`:
//! `application/vnd.apache.arrow.stream` yields an Arrow IPC stream, which is
//! bit-exact for every float; anything else yields JSON.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ravel_ingest::Clock;
use ravel_maintain::{QueryStatus, write_query_audit};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::TenantResolver;
use ravel_sql::{ErrorClass, SqlError, SqlExecutor, SqlRequest};
use ravel_types::{CommitToken, TenantHash, TimeRange};
use serde::Deserialize;
use serde_json::json;
use tracing::Instrument;

use crate::metrics::WorkloadClass;

/// The Arrow IPC stream media type, as registered by the Arrow project.
pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Cap on the request body. SQL text is not large in legitimate use; this is
/// a defensive bound, mirroring the one the Prometheus-shaped handlers apply.
const MAX_BODY_BYTES: usize = 1 << 20;

const NS_PER_SEC: f64 = 1_000_000_000.0;
const ONE_HOUR_NS: i64 = 60 * 60 * 1_000_000_000;

/// Shared state for the SQL route.
#[derive(Clone)]
pub struct SqlState {
    pub executor: Arc<SqlExecutor>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// Object store handle used to write the query-audit record (ADR-0042
    /// decision 4). The audit record is written by the server itself, never
    /// derived from a client body, so a tenant cannot forge or suppress it.
    pub store: Arc<dyn ObjectStoreBackend>,
    /// Injected clock. Library logic never calls `SystemTime::now()`; the
    /// endpoint reads the clock once per request and threads the same
    /// `now_ns` through resolution and the snapshot retry.
    pub clock: Arc<dyn Clock>,
    /// Server wall-deadline ceiling. A request `timeout` is clamped to it.
    pub max_deadline: Duration,
    /// The process-global per-query cost aggregator (ADR-0044 section 4, issue
    /// #425). Each completed statement folds its accounting snapshot and cost
    /// estimate into it, tagged with the tenant hash and workload class, for
    /// `/metrics`. One instance per process, shared with `/api/v1/analytics`.
    ///
    /// The Flight SQL and PromQL paths do not record into it yet: both build
    /// their own `QueryAccounting` per request and report it in the response
    /// only, so `/metrics` covers SQL and analytics traffic, not all query
    /// traffic. Wiring them is the rest of #425.
    pub query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
}

/// The `/api/v1/sql` router.
pub fn router(state: SqlState) -> Router {
    Router::new()
        .route("/api/v1/sql", post(handle))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct SqlBody {
    query: String,
    /// Window start, Unix float seconds. Defaults to one hour before `end`.
    #[serde(default)]
    start: Option<f64>,
    /// Window end, Unix float seconds. Defaults to now.
    #[serde(default)]
    end: Option<f64>,
    /// Per-request wall deadline in seconds. Clamped to the server maximum.
    #[serde(default)]
    timeout: Option<f64>,
    /// Read-your-write commit tokens.
    #[serde(default)]
    min_commit_token: Vec<String>,
}

async fn handle(State(state): State<SqlState>, req: Request<Body>) -> Response {
    match run(&state, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn run(state: &SqlState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant_hash = authenticate(state, &headers)?;

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let body: SqlBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON request body: {e}")))?;

    let now_ns = state.clock.now_ns();
    let request = build_request(&body, now_ns, state.max_deadline)?;

    // A request-level span carrying only bounded values (ADR-0044 section 5):
    // the tenant hash, the workload class, and -- recorded once the query
    // finishes -- the final store request and byte counts. No query text, label
    // values, or object keys ever become span fields. Every SQL statement over
    // this transport is an interactive, client-driven query.
    let span = tracing::info_span!(
        "sql_query",
        tenant_hash = %tenant_hash.to_hex(),
        workload_class = WorkloadClass::Interactive.name(),
        s3_requests = tracing::field::Empty,
        s3_bytes = tracing::field::Empty,
    );

    // The query has now reached execution for a resolved tenant, so it is
    // auditable (ADR-0042 decision 4). Run it, then write exactly one
    // query-audit record for the outcome - success or the specific SqlError -
    // before mapping the result to a response. Requests rejected earlier (no
    // tenant, an unreadable body, or invalid request parameters) never reach
    // here and are not audited: there is no executed query to attribute.
    let result = state
        .executor
        .execute(tenant_hash, &request)
        .instrument(span.clone())
        .await;
    let status = match &result {
        Ok(_) => QueryStatus::Ok,
        Err(_) => QueryStatus::Error,
    };
    write_audit(
        state,
        tenant_hash,
        now_ns,
        &request.sql,
        status,
        request.window.start_ns,
        request.window.end_ns,
    )
    .await;

    let outcome = result.map_err(|err| ApiError::from_sql(err, tenant_hash))?;

    // Fold this query's actual cost and its pre-execution estimate into the
    // process-global aggregator for `/metrics` (ADR-0044 section 4), and record
    // the final counts on the span. The executor already built and dropped a
    // fresh `QueryAccounting` per attempt; `outcome.accounting` is the
    // successful attempt's snapshot, so a retried attempt's counters never
    // bleed in.
    span.record("s3_requests", outcome.accounting.total_s3_requests());
    span.record("s3_bytes", outcome.accounting.total_s3_bytes());
    state.query_accounting.record(
        tenant_hash,
        WorkloadClass::Interactive,
        &outcome.accounting,
        &outcome.estimate,
    );

    let stats = crate::query::accounting_stats_json(&outcome.accounting, &outcome.estimate);
    encode(&headers, &outcome, tenant_hash, stats)
}

/// Write the query-audit record for one request. The audit trail is a
/// server-side obligation independent of the query result, so a failure to
/// write it must never change the client's response: it is logged loudly
/// (`tracing::error!`) - a silently dropped audit record would defeat the whole
/// feature - and then swallowed, so a successful query stays a success.
#[allow(clippy::too_many_arguments)]
async fn write_audit(
    state: &SqlState,
    tenant_hash: TenantHash,
    now_ns: i64,
    query_text: &str,
    status: QueryStatus,
    window_start_ns: i64,
    window_end_ns: i64,
) {
    if let Err(err) = write_query_audit(
        state.store.as_ref(),
        &tenant_hash,
        now_ns,
        query_text,
        "sql",
        status,
        window_start_ns,
        window_end_ns,
    )
    .await
    {
        tracing::error!(
            tenant = %tenant_hash.to_hex(),
            error = %err,
            "failed to write query-audit record; client response unaffected but the audit \
             trail is now incomplete for this request",
        );
    }
}

fn authenticate(state: &SqlState, headers: &HeaderMap) -> Result<TenantHash, ApiError> {
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

/// Turn the request body into a [`SqlRequest`], resolving the window and
/// clamping the deadline.
fn build_request(
    body: &SqlBody,
    now_ns: i64,
    max_deadline: Duration,
) -> Result<SqlRequest, ApiError> {
    let end_ns = match body.end {
        Some(secs) => seconds_to_ns("end", secs)?,
        None => now_ns,
    };
    let start_ns = match body.start {
        Some(secs) => seconds_to_ns("start", secs)?,
        None => end_ns.saturating_sub(ONE_HOUR_NS),
    };
    if start_ns > end_ns {
        return Err(ApiError::bad_request(
            "\"start\" must not be after \"end\"".to_string(),
        ));
    }

    // A client may shorten its own deadline but never extend it past the
    // server budget (finding a7-F01).
    let deadline = match body.timeout {
        Some(secs) if secs > 0.0 && secs.is_finite() => {
            Duration::from_secs_f64(secs).min(max_deadline)
        }
        Some(_) => {
            return Err(ApiError::bad_request(
                "\"timeout\" must be a positive, finite number of seconds".to_string(),
            ));
        }
        None => max_deadline,
    };

    let min_tokens = body
        .min_commit_token
        .iter()
        .map(|raw| {
            CommitToken::decode(raw).map_err(|_| {
                // The token is the caller's own input, so quoting it back is
                // safe and is what makes the error actionable.
                ApiError::bad_request(format!("invalid min_commit_token: {raw:?}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SqlRequest {
        sql: body.query.clone(),
        window: TimeRange { start_ns, end_ns },
        min_tokens,
        now_ns,
        deadline,
    })
}

fn seconds_to_ns(name: &str, secs: f64) -> Result<i64, ApiError> {
    if !secs.is_finite() {
        return Err(ApiError::bad_request(format!(
            "{name:?} must be a finite number of Unix seconds"
        )));
    }
    let ns = secs * NS_PER_SEC;
    if ns < i64::MIN as f64 || ns > i64::MAX as f64 {
        return Err(ApiError::bad_request(format!(
            "{name:?} is out of the representable nanosecond range"
        )));
    }
    Ok(ns as i64)
}

/// Encode the result per the `Accept` header.
///
/// `stats` is this query's cost accounting and estimate (ADR-0044, issue
/// #425). It attaches only to the JSON encoding, as a sibling of `data`
/// mirroring `/api/v1/query_exemplars`' "stats beside data" shape: an Arrow IPC
/// stream is a bare columnar payload with no envelope to carry a JSON object,
/// so an arrow-negotiated response reports no in-body stats. The `/metrics`
/// aggregation still captures every query regardless of encoding, since the
/// fold happens before this function runs.
fn encode(
    headers: &HeaderMap,
    outcome: &ravel_sql::SqlOutcome,
    tenant_hash: TenantHash,
    stats: serde_json::Value,
) -> Result<Response, ApiError> {
    if wants_arrow(headers) {
        let bytes = outcome
            .output
            .to_arrow_ipc()
            .map_err(|err| ApiError::from_sql(err, tenant_hash))?;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, ARROW_STREAM_MEDIA_TYPE)],
            bytes,
        )
            .into_response());
    }

    let data = outcome
        .output
        .to_json()
        .map_err(|err| ApiError::from_sql(err, tenant_hash))?;
    Ok((
        StatusCode::OK,
        axum::Json(json!({ "status": "success", "data": data, "stats": stats })),
    )
        .into_response())
}

fn wants_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains(ARROW_STREAM_MEDIA_TYPE))
}

/// A client-visible error: a status, a stable type tag, and a message that
/// has already passed the redaction boundary.
///
/// `Debug` is safe to derive precisely because every field is already
/// redacted: there is no unredacted source error held here to leak through a
/// stray `{:?}`.
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl ApiError {
    /// Errors raised by this module before the query runs. The messages
    /// describe the caller's own request and carry no server state.
    fn bad_request(message: String) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message,
        }
    }

    /// The redaction boundary. The full error is logged here, once, with the
    /// tenant hash as a structured field (server-side logs may carry it; a
    /// client body may not). The body gets `client_message()` and nothing
    /// else -- in particular never `{err}`.
    fn from_sql(err: SqlError, tenant_hash: TenantHash) -> Self {
        let message = err.client_message();
        let (status, error_type) = match err.class() {
            ErrorClass::BadRequest => (StatusCode::BAD_REQUEST, "bad_data"),
            ErrorClass::Unsupported => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
            ErrorClass::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            ErrorClass::Timeout => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
        };

        // Client-caused rejections are not operational events; log them at
        // debug so a scripted client cannot flood warn-level logs. Everything
        // else keeps warn, because it is either a storage fault or a bug.
        if status == StatusCode::BAD_REQUEST {
            tracing::debug!(
                tenant = %tenant_hash.to_hex(),
                error = %err,
                client_message = %message,
                "sql request rejected",
            );
        } else {
            tracing::warn!(
                tenant = %tenant_hash.to_hex(),
                error = %err,
                client_message = %message,
                "sql query error redacted from client response",
            );
        }

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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn body(json: &str) -> SqlBody {
        serde_json::from_str(json).expect("valid body")
    }

    const MAX: Duration = Duration::from_secs(30);

    #[test]
    fn an_absent_window_defaults_to_the_last_hour_ending_now() {
        let request =
            build_request(&body(r#"{"query":"SELECT 1"}"#), 5_000_000_000, MAX).expect("request");
        assert_eq!(request.now_ns, 5_000_000_000);
        assert_eq!(request.window.end_ns, 5_000_000_000);
        assert_eq!(request.window.start_ns, 5_000_000_000 - ONE_HOUR_NS);
    }

    /// a7-F01: a client `timeout` above the server maximum is clamped, never
    /// honored verbatim.
    #[test]
    fn a_timeout_above_the_server_maximum_is_clamped() {
        let request = build_request(&body(r#"{"query":"SELECT 1","timeout":3600}"#), 0, MAX)
            .expect("request");
        assert_eq!(request.deadline, MAX);
    }

    #[test]
    fn a_timeout_below_the_server_maximum_is_honored() {
        let request =
            build_request(&body(r#"{"query":"SELECT 1","timeout":5}"#), 0, MAX).expect("request");
        assert_eq!(request.deadline, Duration::from_secs(5));
    }

    #[test]
    fn a_non_positive_or_non_finite_timeout_is_rejected() {
        for raw in [
            r#"{"query":"q","timeout":0}"#,
            r#"{"query":"q","timeout":-1}"#,
        ] {
            let err = build_request(&body(raw), 0, MAX).expect_err("rejected");
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn an_inverted_window_is_rejected() {
        let err = build_request(&body(r#"{"query":"q","start":10,"end":1}"#), 0, MAX)
            .expect_err("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn seconds_convert_to_nanoseconds() {
        let request = build_request(&body(r#"{"query":"q","start":1.5,"end":2.5}"#), 0, MAX)
            .expect("request");
        assert_eq!(request.window.start_ns, 1_500_000_000);
        assert_eq!(request.window.end_ns, 2_500_000_000);
    }

    #[test]
    fn an_undecodable_commit_token_is_rejected() {
        let err = build_request(
            &body(r#"{"query":"q","min_commit_token":["not-a-token"]}"#),
            0,
            MAX,
        )
        .expect_err("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn accept_selects_the_arrow_encoding_only_when_asked() {
        let mut headers = HeaderMap::new();
        assert!(!wants_arrow(&headers));
        headers.insert(header::ACCEPT, "application/json".parse().expect("header"));
        assert!(!wants_arrow(&headers));
        headers.insert(
            header::ACCEPT,
            ARROW_STREAM_MEDIA_TYPE.parse().expect("header"),
        );
        assert!(wants_arrow(&headers));
    }

    /// Status mapping is part of the client contract; pin every class.
    #[test]
    fn error_classes_map_to_stable_status_codes() {
        let tenant = TenantHash([0u8; 16]);
        let cases: Vec<(SqlError, StatusCode, &str)> = vec![
            (
                SqlError::Validation(ravel_sql::ValidationError::NotReadOnly { kind: "INSERT" }),
                StatusCode::BAD_REQUEST,
                "bad_data",
            ),
            (
                SqlError::SnapshotInvalidated,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            (
                SqlError::DeadlineExceeded { millis: 30_000 },
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
            ),
            (
                SqlError::Plan("No field named samples.nope".to_string()),
                StatusCode::UNPROCESSABLE_ENTITY,
                "execution",
            ),
        ];
        for (err, status, error_type) in cases {
            let api = ApiError::from_sql(err, tenant);
            assert_eq!(api.status, status);
            assert_eq!(api.error_type, error_type);
        }
    }
}
