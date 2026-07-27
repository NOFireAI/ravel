//! Maps internal error types to Prometheus-shaped HTTP error responses.
//!
//! Storage-layer faults (catalog resolution, segment fetch, and the
//! object-store errors they wrap) carry the physical object key, the tenant
//! hash embedded in that key, and raw backend error text in their `Display`
//! form. Those strings must never reach a client body: the tenant-hashed key
//! layout exists precisely to keep the physical layout opaque (ADR-0009,
//! "no tenant names leaked via object listings"; finding a7-F02). This module
//! is the typed-error to HTTP boundary where that redaction happens: the
//! caller sees a stable, class-specific message with no internal identifiers,
//! while the full error is logged server-side for diagnosis.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ravel_catalog::CatalogError;

use crate::QueryError;
use crate::fetcher::FetchError;
use crate::http::json::ApiResponse;
use crate::http::params::ParamError;
use crate::http::tenant::AuthError;

/// Stable client message for a data-integrity fault (corrupt segment,
/// unreconstructable or mismatched commit record, non-monotonic run). The
/// full detail, including the object key, is logged server-side only.
const MSG_CORRUPT: &str = "stored data failed integrity validation";

/// Stable client message for a transient storage-layer fault (object-store
/// error, changed etag between reads, invalidated snapshot). Distinct from
/// the corruption message so a client and operator can tell a retryable
/// outage apart from a permanent data fault without the leaked detail.
const MSG_UNAVAILABLE: &str = "upstream storage temporarily unavailable";

/// Stable client message for a `min_commit_token` that did not resolve after
/// the catalog's retry. The token fields come from the caller's own request,
/// but the message is fixed for a stable, typed contract.
const MSG_UNSATISFIABLE: &str = "requested commit token is not yet visible; retry";

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
        // Storage-layer faults are redacted to a fixed, class-specific
        // message; the full `Display` (which embeds the object key and tenant
        // hash) is logged here and never placed in the body (a7-F02).
        if let Some(redacted) = redacted_storage_message(&e) {
            tracing::warn!(
                error = %e,
                client_message = redacted,
                "storage-layer query error redacted from client response",
            );
            return ApiError::Unavailable(redacted.to_string());
        }
        match &e {
            QueryError::Parse(_)
            | QueryError::NonPositiveStep { .. }
            | QueryError::InvalidRange { .. }
            | QueryError::TimeOverflow => ApiError::BadData(e.to_string()),
            QueryError::Unsupported { .. } => ApiError::Unsupported(e.to_string()),
            QueryError::TooManySegments { .. }
            | QueryError::TooManySeries { .. }
            | QueryError::TooManySamples { .. } => ApiError::Unsupported(e.to_string()),
            QueryError::DeadlineExceeded { .. } => ApiError::Timeout(e.to_string()),
            QueryError::Eval(inner) => from_eval_error(inner, &e),
            // Handled above by `redacted_storage_message`.
            QueryError::Catalog(_)
            | QueryError::Fetch(_)
            | QueryError::SnapshotInvalidated
            | QueryError::NonMonotonicSamples { .. } => {
                ApiError::Unavailable(MSG_UNAVAILABLE.to_string())
            }
        }
    }
}

/// Returns the redacted, class-specific client message for a storage-layer
/// fault, or `None` for errors whose `Display` is already safe to show the
/// caller (parse errors carry only the client's own query, budget errors
/// carry only counts and limits, deadlines carry only a duration).
///
/// The four client-visible classes required by a7-F02 are kept distinct:
/// `corrupt`, `unavailable`, and unsatisfiable-token all share HTTP 503 here
/// (the status mapping is unchanged, tracked separately by #62) but carry
/// distinct stable messages so diagnosability survives redaction; the budget
/// class keeps its own 422 mapping and unredacted counts.
fn redacted_storage_message(err: &QueryError) -> Option<&'static str> {
    match err {
        QueryError::Fetch(fetch) => Some(match fetch {
            FetchError::Corrupt { .. } => MSG_CORRUPT,
            FetchError::Store { .. } | FetchError::EtagChanged { .. } => MSG_UNAVAILABLE,
        }),
        QueryError::Catalog(catalog) => Some(match catalog {
            CatalogError::UnsatisfiableToken { .. } => MSG_UNSATISFIABLE,
            CatalogError::Reconstruction { .. }
            | CatalogError::FieldMismatch { .. }
            | CatalogError::Record(_)
            | CatalogError::Key(_) => MSG_CORRUPT,
            // Store errors and any future catalog variant redact to the
            // transient-unavailable message rather than risk leaking text.
            _ => MSG_UNAVAILABLE,
        }),
        QueryError::NonMonotonicSamples { .. } => Some(MSG_CORRUPT),
        QueryError::SnapshotInvalidated => Some(MSG_UNAVAILABLE),
        _ => None,
    }
}

fn from_eval_error(inner: &ravel_promql::Error, outer: &QueryError) -> ApiError {
    match inner {
        ravel_promql::Error::Parse(_)
        | ravel_promql::Error::TimeOverflow
        | ravel_promql::Error::NonPositiveStep { .. }
        | ravel_promql::Error::InvalidRange { .. } => ApiError::BadData(outer.to_string()),
        ravel_promql::Error::Unsupported { .. } => ApiError::Unsupported(outer.to_string()),
        // The series-source error can wrap raw backend text; redact it and
        // log the full detail rather than echo it to the client (a7-F02).
        ravel_promql::Error::Source(_) => {
            tracing::warn!(
                error = %outer,
                client_message = MSG_UNAVAILABLE,
                "series-source query error redacted from client response",
            );
            ApiError::Unavailable(MSG_UNAVAILABLE.to_string())
        }
        _ => {
            tracing::warn!(
                error = %outer,
                client_message = MSG_UNAVAILABLE,
                "query error redacted from client response",
            );
            ApiError::Unavailable(MSG_UNAVAILABLE.to_string())
        }
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::StoreError;

    use super::*;

    /// A representative internal object key: tenant-hashed prefix, signal,
    /// level, shard, writer id, and the `.rseg` suffix (ADR-0010 key layout).
    const LEAKY_KEY: &str = "t/deadbeefcafef00d/metrics/l0/0/writer-7.1.2.0123456789abcdef.rseg";
    const TENANT_HASH: &str = "deadbeefcafef00d";
    const RAW_STORE_TEXT: &str = "bucket=prod-telemetry endpoint=s3.internal request-id=abc";

    /// The redacted client message must not carry the object key, the tenant
    /// hash embedded in it, the `.rseg` suffix, the `t/` prefix, or raw
    /// backend error text (a7-F02).
    fn assert_redacted(message: &str) {
        assert!(!message.contains(LEAKY_KEY), "leaked full key: {message}");
        assert!(
            !message.contains(TENANT_HASH),
            "leaked tenant hash: {message}"
        );
        assert!(
            !message.contains(".rseg"),
            "leaked segment suffix: {message}"
        );
        assert!(!message.contains("t/"), "leaked key prefix: {message}");
        assert!(
            !message.contains(RAW_STORE_TEXT),
            "leaked raw store text: {message}"
        );
    }

    fn client_message(err: QueryError) -> String {
        match ApiError::from(err) {
            ApiError::Unavailable(msg) => msg,
            ApiError::BadData(msg) | ApiError::Unsupported(msg) | ApiError::Timeout(msg) => msg,
            ApiError::Unauthenticated => "authentication required".to_string(),
        }
    }

    #[test]
    fn fetch_store_error_is_redacted_but_detail_survives_for_the_log() {
        let err = QueryError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Transient(RAW_STORE_TEXT.to_string()),
        });
        // The Display form (what the server logs) keeps the full detail.
        let detail = err.to_string();
        assert!(detail.contains(LEAKY_KEY), "log detail lost the key");
        assert!(
            detail.contains(RAW_STORE_TEXT),
            "log detail lost store text"
        );

        let message = client_message(err);
        assert_eq!(message, MSG_UNAVAILABLE);
        assert_redacted(&message);
    }

    #[test]
    fn fetch_corrupt_and_etag_classes_stay_distinct_and_redacted() {
        let corrupt = client_message(QueryError::Fetch(FetchError::EtagChanged {
            key: LEAKY_KEY.to_string(),
        }));
        assert_eq!(corrupt, MSG_UNAVAILABLE);
        assert_redacted(&corrupt);

        let non_monotonic = client_message(QueryError::NonMonotonicSamples { prev: 2, next: 1 });
        assert_eq!(non_monotonic, MSG_CORRUPT);

        // corrupt and unavailable are distinct client-visible classes.
        assert_ne!(MSG_CORRUPT, MSG_UNAVAILABLE);
    }

    #[test]
    fn catalog_field_mismatch_redacts_the_key_as_corrupt() {
        let err = QueryError::Catalog(CatalogError::FieldMismatch {
            key: LEAKY_KEY.to_string(),
            field: "tenant_hash",
            expected: "aaaa".to_string(),
            actual: TENANT_HASH.to_string(),
        });
        assert!(err.to_string().contains(LEAKY_KEY));
        let message = client_message(err);
        assert_eq!(message, MSG_CORRUPT);
        assert_redacted(&message);
    }

    #[test]
    fn catalog_store_error_redacts_to_unavailable() {
        let err = QueryError::Catalog(CatalogError::Store(StoreError::Permanent(
            RAW_STORE_TEXT.to_string(),
        )));
        assert!(err.to_string().contains(RAW_STORE_TEXT));
        let message = client_message(err);
        assert_eq!(message, MSG_UNAVAILABLE);
        assert_redacted(&message);
    }

    #[test]
    fn unsatisfiable_token_is_a_distinct_stable_class() {
        let message = client_message(QueryError::Catalog(CatalogError::UnsatisfiableToken {
            shard: 0,
            writer_id: "writer-7".to_string(),
            epoch: 1,
            seq: 2,
            ingest_hour_bucket: 3,
        }));
        assert_eq!(message, MSG_UNSATISFIABLE);
        assert_ne!(MSG_UNSATISFIABLE, MSG_UNAVAILABLE);
        assert_ne!(MSG_UNSATISFIABLE, MSG_CORRUPT);
    }

    #[test]
    fn safe_errors_are_not_redacted() {
        // Budget errors carry only counts and limits: passed through so an
        // operator keeps the useful numbers.
        assert!(
            redacted_storage_message(&QueryError::TooManySeries { count: 9, max: 3 }).is_none()
        );
        // Parse errors carry only the caller's own query text.
        assert!(redacted_storage_message(&QueryError::Parse("bad".to_string())).is_none());
    }
}
