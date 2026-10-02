//! The two error types every query surface in this crate shares.
//!
//! [`ApiError`] is the HTTP form: a status, a stable `errorType` tag, and a
//! message that has already passed the redaction boundary. One type, so the
//! `/api/v1/sql`, `/api/v1/analytics`, and `/api/v1/query_exemplars` bodies
//! cannot drift from each other.
//!
//! [`ServiceError`] is the transport-independent form the query service layer
//! returns: the same redacted message plus the ADR-1374 decision 4 failure
//! class, so a non-HTTP transport (the MCP adapter, issue #1381) can map the
//! failure to its own envelope without re-deriving it from a status code.
//! `From<ServiceError> for ApiError` is the whole HTTP mapping, one function.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ravel_query::QueryError;
use ravel_query::http::{QueryErrorResponse, UsageStatus};
use serde_json::json;

/// A client-visible error: a status, a stable type tag, and a message that has
/// already passed the redaction boundary. `Debug` is safe to derive because
/// every field is already redacted.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub error_type: &'static str,
    pub message: String,
}

impl ApiError {
    /// Errors raised before the query runs. The messages describe the caller's
    /// own request and carry no server state.
    pub fn bad_request(message: String) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message,
        }
    }

    pub fn invalid_param(name: &str, value: &str) -> Self {
        ApiError::bad_request(format!("invalid value for parameter {name:?}: {value:?}"))
    }

    /// Map a `QueryError` through `ravel-query`'s public HTTP mapping, so every
    /// surface here keeps the exact status contract of `/api/v1/query_range`,
    /// including the redaction of storage-layer faults, from one shared source
    /// rather than a copy that could drift.
    pub fn from_query(err: QueryError) -> Self {
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

/// The failure classes ADR-1374 decision 4 defines. A transport maps these to
/// its own envelope; nothing here is HTTP-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceErrorKind {
    /// No resolvable tenant credential.
    Unauthorized,
    /// A resolved, authenticated caller that lacks a capability the request
    /// needs (the `ddl` capability for a `CREATE` or `DROP`, ADR-2040 decision
    /// 4). Distinct from [`ServiceErrorKind::Unauthorized`]: there the caller
    /// has no credential at all, here the credential is good but does not
    /// grant this request.
    Forbidden,
    /// A malformed or unparseable request parameter.
    InvalidArgument,
    /// A well-formed request whose values are out of contract (a percentile
    /// outside `[0, 1]`, a non-positive step).
    Validation,
    /// A construct the engine does not implement.
    Unsupported,
    /// A per-request or per-server budget the query exceeded.
    BudgetExceeded,
    /// The wall deadline elapsed.
    Deadline,
    /// A transient fault: a storage outage, an audit trail that could not be
    /// written, or partial coverage the caller did not consent to.
    Unavailable,
    /// The resolved snapshot was invalidated under the query.
    SnapshotInvalidated,
    /// A server-side fault, including a permanent data-integrity fault.
    Internal,
}

/// A failed query service operation: its ADR-1374 failure class and the
/// already-redacted client message, alongside the HTTP status and `errorType`
/// tag the HTTP transports render.
#[derive(Debug)]
pub struct ServiceError {
    pub kind: ServiceErrorKind,
    pub message: String,
    pub status: StatusCode,
    pub error_type: &'static str,
}

impl ServiceError {
    /// A failure whose HTTP shape is already decided (an audit-trail failure, a
    /// partial-coverage refusal, a rejected admission), classified explicitly.
    pub fn new(kind: ServiceErrorKind, api: ApiError) -> Self {
        ServiceError {
            kind,
            message: api.message,
            status: api.status,
            error_type: api.error_type,
        }
    }

    pub fn unauthorized() -> Self {
        ServiceError::new(
            ServiceErrorKind::Unauthorized,
            ApiError {
                status: StatusCode::UNAUTHORIZED,
                error_type: "unauthorized",
                message: "authentication required".to_string(),
            },
        )
    }

    /// An authenticated caller that lacks a capability the request needs (the
    /// `ddl` capability for a `CREATE` or `DROP`, ADR-2040 decision 4). The
    /// message names the capability and nothing about server state.
    pub fn forbidden(message: String) -> Self {
        ServiceError::new(
            ServiceErrorKind::Forbidden,
            ApiError {
                status: StatusCode::FORBIDDEN,
                error_type: "forbidden",
                message,
            },
        )
    }

    pub fn invalid_argument(message: String) -> Self {
        ServiceError::new(
            ServiceErrorKind::InvalidArgument,
            ApiError::bad_request(message),
        )
    }

    /// A query surface this process does not serve. An ingest-only server
    /// genuinely carries no engine, so a caller asking it for a query gets a
    /// typed refusal naming the surface rather than a panic.
    pub fn unsupported_surface(surface: &str) -> Self {
        ServiceError::new(
            ServiceErrorKind::Unsupported,
            ApiError {
                status: StatusCode::NOT_FOUND,
                error_type: "not_found",
                message: format!("{surface} queries are not served by this instance"),
            },
        )
    }

    /// How a query that ended in this error should be recorded in its usage
    /// record: a deadline is its own outcome, everything else is an error.
    pub fn usage_status(&self) -> UsageStatus {
        match self.kind {
            ServiceErrorKind::Deadline => UsageStatus::Timeout,
            _ => UsageStatus::Error,
        }
    }

    /// Classify and redact a `SqlError`, and log the unredacted form once at
    /// the level its class deserves.
    ///
    /// Client-caused rejections are not operational events, so they log at
    /// debug: a scripted client cannot flood the warn level. Everything else
    /// keeps warn, because it is either a storage fault or a bug.
    #[cfg(feature = "sql")]
    pub fn from_sql(err: ravel_sql::SqlError, tenant_hash: ravel_types::TenantHash) -> Self {
        use ravel_sql::ErrorClass;

        let message = err.client_message();
        let (kind, status, error_type) = match err.class() {
            ErrorClass::BadRequest => (
                ServiceErrorKind::InvalidArgument,
                StatusCode::BAD_REQUEST,
                "bad_data",
            ),
            ErrorClass::Unsupported => (
                ServiceErrorKind::Unsupported,
                StatusCode::UNPROCESSABLE_ENTITY,
                "execution",
            ),
            ErrorClass::Unavailable => (
                ServiceErrorKind::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            ErrorClass::Timeout => (
                ServiceErrorKind::Deadline,
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
            ),
            ErrorClass::Internal => (
                ServiceErrorKind::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        };

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

        ServiceError {
            kind,
            message,
            status,
            error_type,
        }
    }

    /// Classify and redact a [`ravel_sql::DdlExecuteError`], and log the
    /// unredacted form once: at debug for a client-caused status (4xx), so a
    /// scripted client cannot flood the warn level, and at warn otherwise.
    ///
    /// The body message is `client_message()` only; the full `Display` can
    /// carry an object key or backend text and goes to the log alone.
    #[cfg(feature = "sql")]
    pub fn from_ddl(err: ravel_sql::DdlExecuteError, tenant_hash: ravel_types::TenantHash) -> Self {
        let message = err.client_message();
        let (kind, status, error_type) = ddl_class_to_http(err.class());

        if status.is_client_error() {
            tracing::debug!(
                tenant = %tenant_hash.to_hex(),
                error = %err,
                client_message = %message,
                "ddl request rejected",
            );
        } else {
            tracing::warn!(
                tenant = %tenant_hash.to_hex(),
                error = %err,
                client_message = %message,
                "ddl error redacted from client response",
            );
        }

        ServiceError {
            kind,
            message,
            status,
            error_type,
        }
    }

    /// Classify a `QueryError` before it is redacted, then take the status,
    /// tag, and message from the same redaction every HTTP surface uses. The
    /// class comes from the typed error and the body from the redacted
    /// mapping, so a caller learns the class of a storage fault without
    /// learning the object key.
    pub fn from_query(err: QueryError) -> Self {
        let kind = match &err {
            QueryError::Parse(_)
            | QueryError::NonPositiveStep { .. }
            | QueryError::InvalidRange { .. }
            | QueryError::TimeOverflow => ServiceErrorKind::InvalidArgument,
            QueryError::Unsupported { .. } => ServiceErrorKind::Unsupported,
            QueryError::TooManySegments { .. }
            | QueryError::TooManySeries { .. }
            | QueryError::TooManySamples { .. }
            | QueryError::TooManyBytesScanned { .. }
            | QueryError::TooManySliceFrames { .. }
            | QueryError::TooManySliceBytes { .. }
            | QueryError::RequestBudgetExceeded { .. } => ServiceErrorKind::BudgetExceeded,
            QueryError::DeadlineExceeded { .. } => ServiceErrorKind::Deadline,
            QueryError::SnapshotInvalidated => ServiceErrorKind::SnapshotInvalidated,
            QueryError::Eval(_) => ServiceErrorKind::Validation,
            _ => ServiceErrorKind::Unavailable,
        };
        let api = ApiError::from_query(err);
        // A redacted storage fault can still resolve to a permanent 500; keep
        // the class honest rather than reporting a retryable one.
        let kind = if api.status == StatusCode::INTERNAL_SERVER_ERROR {
            ServiceErrorKind::Internal
        } else {
            kind
        };
        ServiceError::new(kind, api)
    }
}

/// The failure class, HTTP status and `errorType` tag a DDL failure class
/// takes. The match has no wildcard, so a new [`ravel_sql::DdlErrorClass`]
/// does not compile until it is given a status here.
///
/// A plain `CREATE` on an existing table (409) and a plain `DROP` of an absent
/// one (404) are well-formed requests refused on the table's current state, so
/// both classify as [`ServiceErrorKind::Validation`].
#[cfg(feature = "sql")]
pub(crate) fn ddl_class_to_http(
    class: ravel_sql::DdlErrorClass,
) -> (ServiceErrorKind, StatusCode, &'static str) {
    use ravel_sql::DdlErrorClass;

    match class {
        DdlErrorClass::BadRequest => (
            ServiceErrorKind::InvalidArgument,
            StatusCode::BAD_REQUEST,
            "bad_data",
        ),
        DdlErrorClass::Conflict => (
            ServiceErrorKind::Validation,
            StatusCode::CONFLICT,
            "conflict",
        ),
        DdlErrorClass::NotFound => (
            ServiceErrorKind::Validation,
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        DdlErrorClass::Unsupported => (
            ServiceErrorKind::Unsupported,
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
        ),
        DdlErrorClass::Unavailable => (
            ServiceErrorKind::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        DdlErrorClass::Timeout => (
            ServiceErrorKind::Deadline,
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
        ),
        DdlErrorClass::Internal => (
            ServiceErrorKind::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ),
    }
}

/// A surface that already built the HTTP form (a parameter parse, a segment
/// read that redacted a store fault) classifies it back from its status. The
/// mapping is total: every status these surfaces can produce is one of the
/// ADR-1374 classes, and a status outside the table is a server fault by
/// definition.
///
/// [`ServiceErrorKind::SnapshotInvalidated`] is not reachable through here: a
/// snapshot invalidation redacts to the same retryable 503 as a storage
/// outage, so it is only distinguishable where the typed `QueryError` is still
/// in hand ([`ServiceError::from_query`]).
impl From<ApiError> for ServiceError {
    fn from(api: ApiError) -> Self {
        let kind = match api.status {
            StatusCode::BAD_REQUEST => ServiceErrorKind::InvalidArgument,
            StatusCode::UNAUTHORIZED => ServiceErrorKind::Unauthorized,
            StatusCode::FORBIDDEN => ServiceErrorKind::Forbidden,
            StatusCode::UNPROCESSABLE_ENTITY => ServiceErrorKind::Unsupported,
            StatusCode::SERVICE_UNAVAILABLE => ServiceErrorKind::Unavailable,
            StatusCode::GATEWAY_TIMEOUT => ServiceErrorKind::Deadline,
            _ => ServiceErrorKind::Internal,
        };
        ServiceError::new(kind, api)
    }
}

impl From<ServiceError> for ApiError {
    fn from(err: ServiceError) -> Self {
        ApiError {
            status: err.status,
            error_type: err.error_type,
            message: err.message,
        }
    }
}

/// The service layer runs on `ravel-query`'s HTTP error type internally (it is
/// what the shared controls return); this classifies one back into a
/// [`ServiceError`] on the way out.
impl From<ravel_query::http::ApiError> for ServiceError {
    fn from(err: ravel_query::http::ApiError) -> Self {
        let kind = match &err {
            ravel_query::http::ApiError::BadData(_) => ServiceErrorKind::InvalidArgument,
            ravel_query::http::ApiError::Unsupported(_) => ServiceErrorKind::Unsupported,
            ravel_query::http::ApiError::Corrupt(_) => ServiceErrorKind::Internal,
            ravel_query::http::ApiError::Unavailable(_) => ServiceErrorKind::Unavailable,
            ravel_query::http::ApiError::Timeout(_) => ServiceErrorKind::Deadline,
            ravel_query::http::ApiError::Unauthenticated => ServiceErrorKind::Unauthorized,
        };
        let QueryErrorResponse {
            status,
            error_type,
            message,
        } = err.into_parts();
        ServiceError {
            kind,
            message,
            status,
            error_type,
        }
    }
}

impl From<ServiceError> for Response {
    fn from(err: ServiceError) -> Self {
        ApiError::from(err).into_response()
    }
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        ApiError::from(self).into_response()
    }
}
