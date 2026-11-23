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

    /// Classify an [`EsqlError`](ravel_sql::esql::EsqlError) for `source`, the
    /// statement text it was raised against (ADR-2887 D12).
    ///
    /// A statement that does not parse, does not bind, or uses something the
    /// lowering does not execute is the caller's statement: 400 `bad_data`,
    /// with the 1-based line and column in the message. A lowered statement
    /// DataFusion cannot plan is 422 `execution` under a fixed message, its
    /// detail logged only. An execution error keeps the mapping and redaction
    /// of [`ServiceError::from_sql`].
    #[cfg(feature = "esql")]
    pub fn from_esql(
        err: ravel_sql::esql::EsqlError,
        source: &str,
        tenant_hash: ravel_types::TenantHash,
    ) -> Self {
        use ravel_sql::esql::EsqlError;

        let (kind, status, error_type, message) = match err {
            EsqlError::Execution(inner) => return ServiceError::from_sql(inner, tenant_hash),
            EsqlError::Parse(parse) => (
                ServiceErrorKind::InvalidArgument,
                StatusCode::BAD_REQUEST,
                "bad_data",
                esql_parse_message(&parse, source),
            ),
            err @ EsqlError::Verification { .. } => (
                ServiceErrorKind::Validation,
                StatusCode::BAD_REQUEST,
                "bad_data",
                err.to_string(),
            ),
            err @ EsqlError::Unsupported { .. } => (
                ServiceErrorKind::Unsupported,
                StatusCode::BAD_REQUEST,
                "bad_data",
                err.to_string(),
            ),
            err @ EsqlError::Plan(_) => {
                if let EsqlError::Plan(detail) = &err {
                    tracing::warn!(
                        tenant = %tenant_hash.to_hex(),
                        error = %detail,
                        "esql plan error redacted from client response",
                    );
                }
                (
                    ServiceErrorKind::Unsupported,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "execution",
                    err.to_string(),
                )
            }
        };
        if status == StatusCode::BAD_REQUEST {
            tracing::debug!(
                tenant = %tenant_hash.to_hex(),
                client_message = %message,
                "esql request rejected",
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
            // A memory refusal is a budget refusal of a well-formed query, the
            // 422 HTTP answers (ADR-2633 decision 5), not a retryable outage.
            QueryError::Fetch(ravel_query::FetchError::FetchMemoryExhausted { .. })
            | QueryError::Catalog(ravel_catalog::CatalogError::MemoryExhausted(_))
            | QueryError::AllocationRefused { .. } => ServiceErrorKind::BudgetExceeded,
            QueryError::DeadlineExceeded { .. } => ServiceErrorKind::Deadline,
            QueryError::SnapshotInvalidated => ServiceErrorKind::SnapshotInvalidated,
            QueryError::Eval(_) => ServiceErrorKind::Validation,
            QueryError::Catalog(_)
            | QueryError::Fetch(_)
            | QueryError::CpuGate(_)
            | QueryError::Distrib { .. }
            | QueryError::Federation { .. }
            | QueryError::NonMonotonicSamples { .. }
            | QueryError::PrioritySampleCountMismatch { .. }
            | QueryError::DuplicatePushdownSeries { .. } => ServiceErrorKind::Unavailable,
            // `QueryError` is `#[non_exhaustive]`, so this crate cannot match
            // it exhaustively; `from_query_classes_every_variant` lists the
            // variants a new one has to be added beside.
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

/// The client message for an ES|QL statement that does not parse: `line L:C: `
/// and the parser's own message, without the byte offset the parser's message
/// also carries, so the body states the position once.
#[cfg(feature = "esql")]
pub(crate) fn esql_parse_message(parse: &ravel_esql::Error, source: &str) -> String {
    let at = parse.line_col(source);
    let start = parse.span().start;
    let mut detail = parse.to_string();
    for marker in [
        format!(" starting at byte {start}"),
        format!(" at byte {start}"),
        format!(" (byte {start})"),
    ] {
        // The last occurrence, so text the statement quotes earlier in the
        // message cannot be taken for the offset; and one not followed by a
        // digit, so offset 1 does not match the start of 12.
        let found = detail
            .rfind(&marker)
            .filter(|&pos| !detail[pos + marker.len()..].starts_with(|c: char| c.is_ascii_digit()));
        if let Some(pos) = found {
            detail.replace_range(pos..pos + marker.len(), "");
            break;
        }
    }
    format!("line {}:{}: {detail}", at.line, at.column)
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

#[cfg(test)]
pub(crate) mod tests {
    use ravel_catalog::CatalogError;
    use ravel_cpu_gate::CpuGateError;
    use ravel_memory::{ExhaustionCause, MemoryExhausted};
    use ravel_object_store::StoreError;
    use ravel_query::{FetchError, FoldLag};

    use super::*;

    /// The memory refusals, one per variant that carries one: a fetch the
    /// budget refused, a catalog decode it refused, and an allocation the
    /// allocator refused after the budget admitted it.
    pub(crate) fn memory_refusals() -> Vec<(&'static str, QueryError)> {
        let exhausted = MemoryExhausted {
            requested: 9,
            reserved: 4,
            limit: 8,
            cause: ExhaustionCause::Accounted,
        };
        vec![
            (
                "FetchMemoryExhausted",
                QueryError::Fetch(FetchError::FetchMemoryExhausted {
                    requested: exhausted.requested,
                    reserved: exhausted.reserved,
                    limit: exhausted.limit,
                    cause: ExhaustionCause::HostMemory {
                        available: 1,
                        floor: 2,
                    },
                }),
            ),
            (
                "Catalog(MemoryExhausted)",
                QueryError::Catalog(CatalogError::MemoryExhausted(exhausted)),
            ),
            (
                "AllocationRefused",
                QueryError::AllocationRefused { requested: 9 },
            ),
        ]
    }

    /// Every memory refusal is the 422 HTTP answers and the `BudgetExceeded`
    /// class, never the retryable `Unavailable` (ADR-2633 decision 5).
    ///
    /// FLIP: drop any one of the three patterns from the memory arm of
    /// `from_query` and that case falls to `Unavailable`.
    #[test]
    fn from_query_classes_every_memory_refusal_as_budget_exceeded() {
        for (name, err) in memory_refusals() {
            let error = ServiceError::from_query(err);

            assert_eq!(error.kind, ServiceErrorKind::BudgetExceeded, "{name}");
            assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY, "{name}");
        }
    }

    /// One case per `QueryError` variant, each pinned to its class. The
    /// enum is `#[non_exhaustive]`, so `from_query` keeps a wildcard; a new
    /// variant belongs both in an explicit arm there and in this list.
    /// `Fetch` and `Catalog` are pinned through a transient store fault and
    /// the memory refusals; a fault that redacts to a 500 is `Internal`.
    #[test]
    fn from_query_classes_every_variant() {
        let store = || StoreError::Transient("down".to_string());
        let mut cases = vec![
            (
                QueryError::Parse("x".to_string()),
                ServiceErrorKind::InvalidArgument,
            ),
            (
                QueryError::NonPositiveStep { step_ms: 0 },
                ServiceErrorKind::InvalidArgument,
            ),
            (
                QueryError::InvalidRange {
                    start_ms: 2,
                    end_ms: 1,
                },
                ServiceErrorKind::InvalidArgument,
            ),
            (QueryError::TimeOverflow, ServiceErrorKind::InvalidArgument),
            (
                QueryError::Unsupported {
                    construct: "x".to_string(),
                },
                ServiceErrorKind::Unsupported,
            ),
            (
                QueryError::Catalog(CatalogError::Store(store())),
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::Fetch(FetchError::Store {
                    key: "k".to_string(),
                    source: store(),
                }),
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::Eval(ravel_promql::Error::TimeOverflow),
                ServiceErrorKind::Validation,
            ),
            (
                QueryError::CpuGate(CpuGateError::Cancelled),
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::CpuGate(CpuGateError::Panicked),
                ServiceErrorKind::Internal,
            ),
            (
                QueryError::TooManySegments { count: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::TooManySeries { count: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::TooManySamples { count: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::TooManyBytesScanned { scanned: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::TooManySliceFrames { frames: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::TooManySliceBytes { bytes: 9, max: 8 },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::RequestBudgetExceeded {
                    requests: 9,
                    max: 8,
                    fold_lag: FoldLag::Healthy,
                },
                ServiceErrorKind::BudgetExceeded,
            ),
            (
                QueryError::DeadlineExceeded {
                    deadline: std::time::Duration::from_secs(1),
                },
                ServiceErrorKind::Deadline,
            ),
            (
                QueryError::SnapshotInvalidated,
                ServiceErrorKind::SnapshotInvalidated,
            ),
            (
                QueryError::Distrib {
                    reason: "x".to_string(),
                },
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::Federation {
                    cluster: "c".to_string(),
                    reason: "x".to_string(),
                },
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::NonMonotonicSamples { prev: 2, next: 1 },
                ServiceErrorKind::Internal,
            ),
            (
                QueryError::PrioritySampleCountMismatch {
                    priorities: 1,
                    samples: 2,
                },
                ServiceErrorKind::Unavailable,
            ),
            (
                QueryError::DuplicatePushdownSeries {
                    series_id: "s".to_string(),
                },
                ServiceErrorKind::Unavailable,
            ),
        ];
        cases.extend(
            memory_refusals()
                .into_iter()
                .map(|(_, err)| (err, ServiceErrorKind::BudgetExceeded)),
        );

        for (err, kind) in cases {
            let label = format!("{err:?}");
            let error = ServiceError::from_query(err);

            assert_eq!(error.kind, kind, "{label}");
            if error.status == StatusCode::INTERNAL_SERVER_ERROR {
                assert_eq!(error.kind, ServiceErrorKind::Internal, "{label}");
            }
        }
    }
}
