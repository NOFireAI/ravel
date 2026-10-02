//! `POST /api/v1/sql`: the SQL endpoint. A query is read-only; the only
//! statements that write are the Parquet DDL forms below.
//!
//! This module is the transport and the error-to-HTTP boundary; every
//! semantic decision lives in ravel-sql. That split is what keeps ADR-0013's
//! structural isolation honest: nothing here links datafusion, and nothing in
//! ravel-sql links axum. Results cross the boundary as an opaque
//! `QueryOutput` that encodes itself to Arrow IPC or JSON.
//!
//! # Error redaction (applied to a second boundary)
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
//! deadline, never raise it.
//!
//! The response encoding follows `Accept`:
//! `application/vnd.apache.arrow.stream` yields an Arrow IPC stream, which is
//! bit-exact for every float; anything else yields JSON.
//!
//! # DDL
//!
//! `ravel_sql::statement_kind` routes a statement on its leading keyword
//! alone, without parsing: a first real token (skipping whitespace, `--`
//! comments, and nested plain `/* */` comments) that case-insensitively
//! matches `CREATE` or `DROP` takes the DDL path, whatever follows it,
//! including a syntax error or several statements; everything else,
//! including text that does not parse at all, takes the query path
//! unchanged. The DDL path accepts
//! `CREATE [OR REPLACE] EXTERNAL TABLE ... STORED AS PARQUET LOCATION ...` and
//! `DROP TABLE` (ADR-2040) and needs the `ddl` capability, which a principal
//! holds only through a `TOKEN=TENANT;ddl` token or the `--oidc-ddl-claim`
//! claim. Without it the response is 403 with `errorType` `forbidden`.
//!
//! A DDL success is 200 with
//! `{"status":"success","data":{"outcome":"created"|"dropped"|"noop","table":...}}`,
//! plus `version` for `created` and `dropped` and `files` for `created`. The
//! body is JSON even when `Accept` asks for Arrow IPC, since a DDL outcome has
//! no rows. A failure maps from `DdlExecuteError::class`: 400 `bad_data`, 409
//! `conflict`, 404 `not_found`, 422 `execution`, 503 `unavailable`, 504
//! `timeout`, 500 `internal`. The `timeout` request field clamps the DDL
//! deadline the same way it clamps a query's, and `start`, `end` and
//! `min_commit_token` do not apply to DDL.
//!
//! # Warnings
//!
//! A JSON success body carries a top-level `warnings` array of strings when
//! the statement's answer is non-fatally incomplete, omitted entirely when
//! there is nothing to say. It is the same field, with the same omit-when-empty
//! rule, that the PromQL surface renders (`ravel_query::http::json`), so a
//! client that already reads one reads the other. The strings come from
//! [`ravel_sql::SqlOutcome::warnings`]: the semantic decision is ravel-sql's,
//! this module only renders it.
//!
//! Today's one warning is the `samples` table's native-histogram exclusion
//! (issue #1738). An Arrow-negotiated response carries no warnings, for the
//! same reason it carries no `stats`: an IPC stream is a bare columnar payload
//! with no envelope to put them in.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ravel_ingest::Clock;
use ravel_maintain::{QueryAuditSink, QueryStatus};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::QueryAdmissionController;
use ravel_query::http::TenantResolver;
use ravel_sql::{DdlOutcome, SqlExecutor, SqlRequest};
use ravel_tenant_resolve::Principal;
use ravel_types::{CommitToken, TenantHash, TimeRange};
use serde::Deserialize;
use serde_json::json;

use crate::service::{ApiError, QueryService, ServiceError, ServiceErrorKind};

/// The Arrow IPC stream media type, as registered by the Arrow project.
pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Cap on the request body, sized against the statement gate rather than
/// picked as a round number: `ravel_sql::MAX_STATEMENT_COMPLEXITY` admits
/// 1,000 tokens, and 64 KiB leaves a statement that large all the
/// room it needs for whitespace, string literals (which the gate does not
/// count), and a list of `min_commit_token` values, while cutting the text a
/// deep-expression payload can reach to a sixteenth of the old
/// 1 MiB (issue #1680). The cap is the outer bound; the complexity guard in
/// ravel-sql is what actually bounds parse and walk depth, on every SQL
/// surface rather than only this one.
const MAX_BODY_BYTES: usize = 64 << 10;

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
    /// The evidential audit sink this endpoint submits every
    /// [`AuditEvent`](ravel_maintain::AuditEvent) through, awaiting each
    /// submission's durability before releasing the response or (for DDL)
    /// before running the statement (ADR-0062 §2a). Every query surface
    /// audits through this one seam rather than a direct `write_query_audit`
    /// call. A query submits one event per request. A statement refused for
    /// the `ddl` capability submits one event (`error`). A statement past
    /// that check submits two: `attempted` first, awaited so a failed
    /// submission refuses it before any store call, and `ok` or `error` once
    /// admission refuses it or [`SqlExecutor::execute_ddl`] finishes, from a
    /// task a client disconnect cannot cancel (see [`run_ddl`]). Defaults to
    /// [`NoopQueryAuditSink`](ravel_maintain::NoopQueryAuditSink); a deployment
    /// attaches the one shared pipeline.
    pub audit_sink: Arc<dyn QueryAuditSink>,
    /// Injected clock. Library logic never calls `SystemTime::now()`; the
    /// endpoint reads the clock once per request and threads the same
    /// `now_ns` through resolution and the snapshot retry.
    pub clock: Arc<dyn Clock>,
    /// Server wall-deadline ceiling. A request `timeout` is clamped to it.
    pub max_deadline: Duration,
    /// The process-global per-query cost aggregator (ADR-0044 section 4).
    /// Each completed statement folds its accounting snapshot and cost
    /// estimate into it, tagged with the tenant hash and workload class, for
    /// `/metrics`. One instance per process, shared with `/api/v1/analytics`,
    /// the Flight SQL path (`crate::flight::service`), and the PromQL path,
    /// all of which hold it as `Arc<dyn QueryCostRecorder>` -- so `/metrics`
    /// covers every query surface, not only `/api/v1/sql`.
    pub query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2), the one
    /// shared controller every query surface in the process gates against.
    /// `handle` acquires a permit before running the statement and is rejected,
    /// before any resolve or GET, if admitting one more query would exceed this
    /// process's reconciled fleet threshold.
    pub query_admission: Arc<QueryAdmissionController>,
}

impl SqlState {
    /// The query service layer for this surface: the shared controls plus this
    /// state. The handler below calls it, and so does the process-wide service
    /// built in `lib.rs`, through one implementation of the controls.
    pub fn service(&self) -> QueryService {
        QueryService::with_metrics(
            Arc::clone(&self.tenant_resolver),
            Arc::clone(&self.clock),
            Arc::clone(&self.query_admission),
            Arc::clone(&self.query_accounting),
            Arc::clone(&self.audit_sink),
        )
        .with_sql(self.clone())
    }
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

/// Parse, authenticate, route on the statement's kind, then either ask the
/// query service layer and encode (a query) or run the DDL path
/// ([`run_ddl`]). For a query, fleet-global admission, the usage record on
/// every exit path (the dropped-future exit included), the audit event and its
/// durability wait, and error redaction all happen inside
/// [`QueryService::sql_execute`], shared with every other query surface.
///
/// Authentication runs here, before the service takes a permit, so an anonymous
/// caller no longer consumes one from the fleet-global concurrency ceiling.
async fn run(state: &SqlState, req: Request<Body>) -> Result<Response, ServiceError> {
    let headers = req.headers().clone();
    let principal =
        crate::service::authenticate_principal(state.tenant_resolver.as_ref(), &headers)?;
    let tenant_hash = principal.tenant.hash();

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let body: SqlBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON request body: {e}")))?;

    match ravel_sql::statement_kind(&body.query) {
        ravel_sql::StatementKind::Query => {}
        ravel_sql::StatementKind::Ddl => return run_ddl(state, &principal, &body).await,
    }

    let now_ns = state.clock.now_ns();
    let request = build_request(&body, now_ns, state.max_deadline)?;

    let outcome = state.service().sql_execute(tenant_hash, &request).await?;

    // Fold this query's LogsScanExec block counters into the process-global
    // prune-selectivity totals. These are the scan's own
    // DataFusion counters, read off the plan in ravel-sql and surfaced on
    // `stats`; a metrics query passes zeros and moves nothing. Separate from
    // the cost aggregator in the service layer: that one answers "what did this
    // query spend", this one answers "how much did POSTINGS let it skip".
    crate::query_postings_metrics::record(
        outcome.stats.blocks_total,
        outcome.stats.blocks_scanned,
        outcome.stats.blocks_pruned_by_postings,
    );

    let mut stats = crate::query::accounting_stats_json(&outcome.accounting, &outcome.estimate);
    if let serde_json::Value::Object(ref mut map) = stats {
        map.insert(
            "phases".to_string(),
            serde_json::Value::Array(ravel_sql::stats_json::phase_costs_json(
                &outcome.phase_accounting,
            )),
        );
        map.insert(
            "io".to_string(),
            ravel_sql::stats_json::io_shape_json(&outcome.io_shape),
        );
    }
    encode(&headers, &outcome, tenant_hash, stats)
}

/// The request's wall deadline: a client may shorten its own deadline but
/// never extend it past the server budget. Shared by the query and DDL paths so
/// both clamp identically.
fn request_deadline(body: &SqlBody, max_deadline: Duration) -> Result<Duration, ApiError> {
    match body.timeout {
        Some(secs) if secs > 0.0 && secs.is_finite() => {
            // `Duration::from_secs_f64` panics when `secs` is finite but too
            // large to represent as a `Duration` (an `f64` as small as 1e300
            // triggers it). Capping to `max_deadline`'s own seconds first
            // keeps the value handed to it always representable, since
            // `max_deadline` itself already came from a `Duration`.
            let capped_secs = secs.min(max_deadline.as_secs_f64());
            Ok(Duration::from_secs_f64(capped_secs).min(max_deadline))
        }
        Some(_) => Err(ApiError::bad_request(
            "\"timeout\" must be a positive, finite number of seconds".to_string(),
        )),
        None => Ok(max_deadline),
    }
}

/// The DDL path: `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`
/// (ADR-2040 decision 4).
///
/// A caller without the `ddl` capability is refused with 403 before anything
/// else happens: no grants record is read, no store is called and
/// `validate_ddl` is not run. A caller with it takes a fleet-global admission
/// permit and runs [`SqlExecutor::execute_ddl`] as the principal's own tenant,
/// which is also what the manifest records as its creator; no header or body
/// value can name another.
///
/// A request whose `timeout` is malformed is refused with 400 before any
/// statement handling, for every caller, and writes no audit record, the
/// same as a body that is not valid JSON. A statement refused for the `ddl`
/// capability submits one audit event (`error`). A statement past that check
/// submits two: `attempted` first, awaited so a failed submission refuses it
/// before any store call, and `ok` or `error` once it is refused by admission
/// or finishes in [`SqlExecutor::execute_ddl`], from a task a client
/// disconnect cannot cancel. A failed outcome submission never changes the
/// response: the statement is already on record.
async fn run_ddl(
    state: &SqlState,
    principal: &Principal,
    body: &SqlBody,
) -> Result<Response, ServiceError> {
    let tenant_hash = principal.tenant.hash();
    let now_ns = state.clock.now_ns();
    let service = state.service();
    let controls = service.controls();
    let deadline = request_deadline(body, state.max_deadline)?;

    if !principal.ddl {
        controls
            .audit(
                tenant_hash,
                now_ns,
                &body.query,
                "sql",
                (now_ns, now_ns),
                QueryStatus::Error,
            )
            .await?;
        return Err(ServiceError::forbidden(
            "this credential does not hold the ddl capability".to_string(),
        ));
    }

    // The `attempted` record: submitted and awaited before anything is read
    // or written, so a failed submission (today's 503 `unavailable`) refuses
    // the statement before it touches the store.
    controls
        .audit(
            tenant_hash,
            now_ns,
            &body.query,
            "sql",
            (now_ns, now_ns),
            QueryStatus::Attempted,
        )
        .await?;

    // From here the statement runs, and its outcome is recorded, in a task a
    // client disconnect cannot cancel: dropping this function's future (the
    // client going away) never drops the spawned one.
    let executor = Arc::clone(&state.executor);
    let clock = Arc::clone(&state.clock);
    let task_controls = controls.clone();
    let sql = body.query.clone();
    let tenant_str = principal.tenant.as_str().to_string();

    let handle = tokio::spawn(async move {
        let ddl_result: Result<DdlOutcome, ServiceError> = async {
            let _permit = task_controls.admit()?;
            executor
                .execute_ddl(tenant_hash, &sql, &tenant_str, deadline)
                .await
                .map_err(|err| ServiceError::from_ddl(err, tenant_hash))
        }
        .await;

        let outcome_now_ns = clock.now_ns();
        let status = match &ddl_result {
            Ok(_) => QueryStatus::Ok,
            Err(_) => QueryStatus::Error,
        };
        let audit_result = task_controls
            .audit(
                tenant_hash,
                outcome_now_ns,
                &sql,
                "sql",
                (outcome_now_ns, outcome_now_ns),
                status,
            )
            .await;
        (ddl_result, audit_result)
    });

    let (ddl_result, audit_result) = handle.await.map_err(|_join_err| {
        ServiceError::new(
            ServiceErrorKind::Internal,
            ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_type: "internal",
                message: "ddl execution task failed unexpectedly".to_string(),
            },
        )
    })?;

    match ddl_result {
        Ok(outcome) => {
            let warnings: &[&str] = if audit_result.is_err() {
                &[
                    "the audit outcome record for this statement was not made durable; its attempted record is",
                ]
            } else {
                &[]
            };
            Ok((
                StatusCode::OK,
                axum::Json(ddl_outcome_json(&outcome, warnings)),
            )
                .into_response())
        }
        Err(err) => {
            if let Err(audit_err) = &audit_result {
                tracing::error!(
                    tenant = %tenant_hash.to_hex(),
                    error = ?audit_err,
                    "ddl outcome audit submission failed after a failed statement",
                );
            }
            Err(err)
        }
    }
}

/// The JSON success body of a DDL statement, whatever the `Accept` header
/// says: a DDL outcome has no rows to stream as Arrow IPC. `warnings` is
/// rendered only when non-empty, the same omit-when-empty rule the query
/// path's `warnings` array follows.
fn ddl_outcome_json(outcome: &DdlOutcome, warnings: &[&str]) -> serde_json::Value {
    let data = match outcome {
        DdlOutcome::Created {
            table,
            version,
            files,
            skipped_directory_markers,
            skipped_other_suffixes,
        } => json!({
            "outcome": "created",
            "table": table,
            "version": version,
            "files": files,
            "skipped_directory_markers": skipped_directory_markers,
            "skipped_other_suffixes": skipped_other_suffixes,
        }),
        DdlOutcome::Dropped { table, version } => json!({
            "outcome": "dropped",
            "table": table,
            "version": version,
        }),
        DdlOutcome::NoOp { table } => json!({
            "outcome": "noop",
            "table": table,
        }),
    };
    let mut body = json!({ "status": "success", "data": data });
    if !warnings.is_empty()
        && let serde_json::Value::Object(ref mut map) = body
    {
        map.insert("warnings".to_string(), json!(warnings));
    }
    body
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

    let deadline = request_deadline(body, max_deadline)?;

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
        // The HTTP surface never opts into the MCP-only request knobs
        // (#1376): no row window, no row cap, no per-request budgets, so
        // the server ceilings alone govern an HTTP query.
        row_window: false,
        max_rows: None,
        budgets: None,
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
/// `stats` is this query's cost accounting and estimate (ADR-0044).
/// It attaches only to the JSON encoding, as a sibling of `data`
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
) -> Result<Response, ServiceError> {
    if wants_arrow(headers) {
        let bytes = outcome
            .output
            .to_arrow_ipc()
            .map_err(|err| ServiceError::from_sql(err, tenant_hash))?;
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
        .map_err(|err| ServiceError::from_sql(err, tenant_hash))?;
    let mut body = json!({ "status": "success", "data": data, "stats": stats });
    // Omitted when empty, the way the PromQL envelope omits its own
    // `warnings` (crates/ravel-query/src/http/json.rs): a client that does not
    // read warnings sees the response it saw before, and a client that does
    // never has to distinguish an empty array from no array.
    let warnings = outcome.warnings();
    if !warnings.is_empty()
        && let serde_json::Value::Object(ref mut map) = body
    {
        map.insert("warnings".to_string(), json!(warnings));
    }
    Ok((StatusCode::OK, axum::Json(body)).into_response())
}

fn wants_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains(ARROW_STREAM_MEDIA_TYPE))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_sql::SqlError;

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

    /// A client `timeout` above the server maximum is clamped, never
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
            (
                // The structural-complexity rejection (issue #1680) is a bad
                // request like every other validation refusal, not a 413 and
                // not a 422: the statement is malformed for this endpoint, and
                // the caller can fix it by simplifying the expression.
                SqlError::Validation(
                    ravel_sql::StatementTooComplex {
                        count: ravel_sql::MAX_STATEMENT_COMPLEXITY + 1,
                        max: ravel_sql::MAX_STATEMENT_COMPLEXITY,
                    }
                    .into(),
                ),
                StatusCode::BAD_REQUEST,
                "bad_data",
            ),
        ];
        for (err, status, error_type) in cases {
            let api = ServiceError::from_sql(err, tenant);
            assert_eq!(api.status, status);
            assert_eq!(api.error_type, error_type);
        }
    }

    /// The DDL status mapping is part of the client contract too. The `match`
    /// below names every `DdlErrorClass` with no wildcard, so a new class
    /// fails to compile here until it is given a pinned status.
    #[test]
    fn ddl_error_classes_map_to_stable_status_codes() {
        use ravel_sql::DdlErrorClass;

        fn pinned(class: DdlErrorClass) -> (StatusCode, &'static str) {
            match class {
                DdlErrorClass::BadRequest => (StatusCode::BAD_REQUEST, "bad_data"),
                DdlErrorClass::Conflict => (StatusCode::CONFLICT, "conflict"),
                DdlErrorClass::NotFound => (StatusCode::NOT_FOUND, "not_found"),
                DdlErrorClass::Unsupported => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
                DdlErrorClass::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
                DdlErrorClass::Timeout => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
                DdlErrorClass::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            }
        }

        for class in [
            DdlErrorClass::BadRequest,
            DdlErrorClass::Conflict,
            DdlErrorClass::NotFound,
            DdlErrorClass::Unsupported,
            DdlErrorClass::Unavailable,
            DdlErrorClass::Timeout,
            DdlErrorClass::Internal,
        ] {
            let (_, status, error_type) = crate::service::error::ddl_class_to_http(class);
            assert_eq!((status, error_type), pinned(class), "{class:?}");
        }
    }

    /// A real `DdlExecuteError` takes its class's status and its body is the
    /// redacted `client_message()`, not the full `Display`.
    #[test]
    fn a_ddl_error_takes_its_classs_status_and_client_message() {
        let tenant = TenantHash([0u8; 16]);
        let err = ravel_sql::DdlExecuteError::NotConfigured;
        let message = err.client_message();
        let api = ServiceError::from_ddl(err, tenant);
        assert_eq!(api.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(api.error_type, "execution");
        assert_eq!(api.message, message);

        let api = ServiceError::from_ddl(
            ravel_sql::DdlExecuteError::Deadline {
                deadline: Duration::from_secs(1),
            },
            tenant,
        );
        assert_eq!(api.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(api.error_type, "timeout");
    }

    /// The DDL deadline is clamped exactly as a query's is: lowered by the
    /// request, never raised past the server maximum.
    #[test]
    fn the_ddl_deadline_is_clamped_like_a_querys() {
        assert_eq!(
            request_deadline(&body(r#"{"query":"DROP TABLE t","timeout":3600}"#), MAX)
                .expect("deadline"),
            MAX
        );
        assert_eq!(
            request_deadline(&body(r#"{"query":"DROP TABLE t","timeout":5}"#), MAX)
                .expect("deadline"),
            Duration::from_secs(5)
        );
        assert_eq!(
            request_deadline(&body(r#"{"query":"DROP TABLE t"}"#), MAX).expect("deadline"),
            MAX
        );
        // Finite but far beyond what a `Duration` holds: clamped to the
        // server maximum rather than converted, so it cannot panic.
        assert_eq!(
            request_deadline(&body(r#"{"query":"DROP TABLE t","timeout":1e300}"#), MAX)
                .expect("deadline"),
            MAX
        );
        let err = request_deadline(&body(r#"{"query":"DROP TABLE t","timeout":0}"#), MAX)
            .expect_err("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_ddl_outcome_renders_the_fields_its_variant_has() {
        let created = ddl_outcome_json(
            &DdlOutcome::Created {
                table: "t".to_string(),
                version: 1,
                files: 3,
                skipped_directory_markers: 0,
                skipped_other_suffixes: 2,
            },
            &[],
        );
        assert_eq!(created["status"], "success");
        assert!(created.get("warnings").is_none(), "{created}");
        assert_eq!(created["data"]["outcome"], "created");
        assert_eq!(created["data"]["table"], "t");
        assert_eq!(created["data"]["version"], 1);
        assert_eq!(created["data"]["files"], 3);
        assert_eq!(created["data"]["skipped_other_suffixes"], 2);

        let dropped = ddl_outcome_json(
            &DdlOutcome::Dropped {
                table: "t".to_string(),
                version: 2,
            },
            &[],
        );
        assert_eq!(dropped["data"]["outcome"], "dropped");
        assert_eq!(dropped["data"]["version"], 2);
        assert!(dropped["data"].get("files").is_none(), "{dropped}");

        let noop = ddl_outcome_json(
            &DdlOutcome::NoOp {
                table: "t".to_string(),
            },
            &["a warning"],
        );
        assert_eq!(noop["warnings"][0], "a warning", "{noop}");
        assert_eq!(noop["data"]["outcome"], "noop");
        assert!(noop["data"].get("version").is_none(), "{noop}");
    }

    /// The body cap and the statement gate are one decision, so the two
    /// numbers are pinned together: a body large enough to hold a statement at
    /// the complexity bound is accepted, and the cap stays far below the 1 MiB
    /// that let a 500,000-operator payload through (issue #1680).
    #[test]
    fn the_body_cap_admits_a_statement_at_the_complexity_bound() {
        assert_eq!(MAX_BODY_BYTES, 64 << 10);
        // Space-separated: a run of `a` is ONE token under the token rule, so
        // `"a".repeat(N)` builds a 1-unit statement, not an N-unit one. Each
        // `a ` is its own token, which is what makes this the widest statement
        // the gate admits.
        let statement = "a ".repeat(ravel_sql::MAX_STATEMENT_COMPLEXITY);
        let body = format!(r#"{{"query":"{statement}"}}"#);
        assert_eq!(
            ravel_sql::complexity_guard::structural_count(&statement),
            ravel_sql::MAX_STATEMENT_COMPLEXITY,
            "the probe must actually sit at the bound for this case to pin anything"
        );
        assert!(
            body.len() < MAX_BODY_BYTES,
            "a statement at the complexity bound must fit the body cap: {} vs {MAX_BODY_BYTES}",
            body.len()
        );
    }
}
