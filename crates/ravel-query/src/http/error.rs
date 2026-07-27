//! Maps internal error types to Prometheus-shaped HTTP error responses.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::QueryError;
use crate::http::json::ApiResponse;
use crate::http::params::ParamError;
use crate::http::tenant::AuthError;

pub enum ApiError {
    BadData(String),
    Unsupported(String),
    Unavailable(String),
    Timeout(String),
    Unauthenticated,
}

impl From<ParamError> for ApiError {
    fn from(e: ParamError) -> Self {
        ApiError::BadData(e.to_string())
    }
}

impl From<AuthError> for ApiError {
    fn from(_: AuthError) -> Self {
        ApiError::Unauthenticated
    }
}

impl From<QueryError> for ApiError {
    fn from(e: QueryError) -> Self {
        match &e {
            QueryError::Parse(_)
            | QueryError::NonPositiveStep { .. }
            | QueryError::InvalidRange { .. }
            | QueryError::TimeOverflow => ApiError::BadData(e.to_string()),
            QueryError::Unsupported { .. } => ApiError::Unsupported(e.to_string()),
            QueryError::Catalog(_)
            | QueryError::Fetch(_)
            | QueryError::SnapshotInvalidated
            | QueryError::NonMonotonicSamples { .. } => ApiError::Unavailable(e.to_string()),
            QueryError::TooManySegments { .. }
            | QueryError::TooManySeries { .. }
            | QueryError::TooManySamples { .. } => ApiError::Unsupported(e.to_string()),
            QueryError::DeadlineExceeded { .. } => ApiError::Timeout(e.to_string()),
            QueryError::Eval(inner) => from_eval_error(inner, e.to_string()),
        }
    }
}

fn from_eval_error(inner: &ravel_promql::Error, message: String) -> ApiError {
    match inner {
        ravel_promql::Error::Parse(_)
        | ravel_promql::Error::TimeOverflow
        | ravel_promql::Error::NonPositiveStep { .. }
        | ravel_promql::Error::InvalidRange { .. } => ApiError::BadData(message),
        ravel_promql::Error::Unsupported { .. } => ApiError::Unsupported(message),
        ravel_promql::Error::Source(_) => ApiError::Unavailable(message),
        _ => ApiError::Unavailable(message),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            ApiError::BadData(msg) => (StatusCode::BAD_REQUEST, "bad_data", msg),
            ApiError::Unsupported(msg) => (StatusCode::UNPROCESSABLE_ENTITY, "execution", msg),
            ApiError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", msg),
            ApiError::Timeout(msg) => (StatusCode::GATEWAY_TIMEOUT, "timeout", msg),
            ApiError::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "authentication required".to_string(),
            ),
        };
        let body: ApiResponse<()> = ApiResponse::Error {
            error_type,
            error: message,
        };
        (status, Json(body)).into_response()
    }
}
