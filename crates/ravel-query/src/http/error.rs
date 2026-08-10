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
///
/// This and its siblings are the single source of the redacted messages: any
/// endpoint that runs the query engine reaches them through
/// [`QueryErrorResponse`] rather than keeping its own copy.
pub const MSG_CORRUPT: &str = "stored data failed integrity validation";

/// Stable client message for a transient storage-layer fault (object-store
/// error, changed etag between reads, invalidated snapshot). Distinct from
/// the corruption message so a client and operator can tell a retryable
/// outage apart from a permanent data fault without the leaked detail.
pub const MSG_UNAVAILABLE: &str = "upstream storage temporarily unavailable";

/// Stable client message for a `min_commit_token` that did not resolve after
/// the catalog's retry. The token fields come from the caller's own request,
/// but the message is fixed for a stable, typed contract.
pub const MSG_UNSATISFIABLE: &str = "requested commit token is not yet visible; retry";

#[derive(Debug)]
pub enum ApiError {
    BadData(String),
    Unsupported(String),
    /// A permanent server-side data-integrity fault: stored data decoded but
    /// failed validation (corrupt segment, unreconstructable or mismatched
    /// commit record, non-monotonic run). Maps to HTTP 500 `internal`, a
    /// non-retryable 5xx: the corruption is in already-stored objects, so a
    /// retry re-reads the same bytes and never clears (a7-F05, #62). Kept
    /// distinct from `Unavailable` so a client does not retry forever against
    /// permanently corrupt data.
    Corrupt(String),
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
            // A corruption fault is a permanent server-side data problem, not a
            // transient outage: map it to the non-retryable 500 `internal` so a
            // client stops retrying against permanently corrupt data (a7-F05,
            // #62). Transient storage faults keep the retryable 503.
            return if redacted == MSG_CORRUPT {
                ApiError::Corrupt(redacted.to_string())
            } else {
                ApiError::Unavailable(redacted.to_string())
            };
        }
        match &e {
            QueryError::Parse(_)
            | QueryError::NonPositiveStep { .. }
            | QueryError::InvalidRange { .. }
            | QueryError::TimeOverflow => ApiError::BadData(e.to_string()),
            QueryError::Unsupported { .. } => ApiError::Unsupported(e.to_string()),
            QueryError::TooManySegments { .. }
            | QueryError::TooManySeries { .. }
            | QueryError::TooManySamples { .. }
            | QueryError::TooManyBytesScanned { .. } => ApiError::Unsupported(e.to_string()),
            // An over-wide window refused before any LIST (issue #635) is a
            // resource-budget rejection, grouped with the budget classes above
            // under the same 422 "execution" mapping. Its text carries only the
            // estimate and the limit (counts, no object key or tenant
            // identity), so it is echoed rather than redacted, exactly as the
            // budget errors are. `redacted_storage_message` returns `None` for
            // it, so it reaches this arm rather than the storage-fault path.
            QueryError::Catalog(CatalogError::WindowTooWide { .. }) => {
                ApiError::Unsupported(e.to_string())
            }
            QueryError::DeadlineExceeded { .. } => ApiError::Timeout(e.to_string()),
            QueryError::Eval(inner) => from_eval_error(inner, &e),
            // Handled above by `redacted_storage_message`.
            QueryError::Catalog(_)
            | QueryError::Fetch(_)
            | QueryError::SnapshotInvalidated
            // A distributed slice failure is a server-side outage from the
            // client's view (a worker was unreachable, corrupt, or framed a bad
            // response); its `reason` may carry internal detail, so it is
            // redacted to the same retryable 503 as the other storage faults.
            | QueryError::Distrib { .. }
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
/// `corrupt` maps to HTTP 500 `internal` (a permanent, non-retryable
/// server-side data fault; a7-F05, #62), while `unavailable` and
/// unsatisfiable-token map to the retryable HTTP 503; each carries its own
/// stable message so diagnosability survives redaction; the budget class
/// keeps its own 422 mapping and unredacted counts.
fn redacted_storage_message(err: &QueryError) -> Option<&'static str> {
    match err {
        QueryError::Fetch(fetch) => Some(match fetch {
            FetchError::Corrupt { .. } => MSG_CORRUPT,
            FetchError::Store { .. } | FetchError::EtagChanged { .. } => MSG_UNAVAILABLE,
        }),
        // An over-wide-window refusal (issue #635) carries only counts and is
        // safe to show; like the budget errors it is not a storage fault, so
        // it is not redacted (the `From` impl maps it to a 422).
        QueryError::Catalog(CatalogError::WindowTooWide { .. }) => None,
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
        | ravel_promql::Error::InvalidRange { .. }
        | ravel_promql::Error::WrongType { .. } => ApiError::BadData(outer.to_string()),
        ravel_promql::Error::Unsupported { .. }
        | ravel_promql::Error::TooManyPoints { .. }
        | ravel_promql::Error::AmbiguousMatch { .. }
        | ravel_promql::Error::InvalidRegex { .. }
        | ravel_promql::Error::InvalidLabelName { .. } => ApiError::Unsupported(outer.to_string()),
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

/// The HTTP rendering of a query error: the status code, the stable
/// Prometheus-shaped `errorType` tag, and a client-safe message that has
/// already passed the a7-F02 redaction boundary (storage-layer faults carry
/// only a fixed class message here; the full detail is logged, never echoed).
///
/// This is the reusable, public form of the mapping the [`IntoResponse`] impl
/// applies. An endpoint that runs the query engine outside this module (the
/// analytics endpoint in `ravel-server`) builds its response from this so its
/// status contract cannot drift from `/api/v1/query_range`'s; both paths share
/// the one table in [`ApiError::into_parts`].
#[derive(Debug, Clone)]
pub struct QueryErrorResponse {
    /// The HTTP status code.
    pub status: StatusCode,
    /// The stable `errorType` tag: `bad_data`, `execution`, `internal`,
    /// `unavailable`, `timeout`, or `unauthorized`.
    pub error_type: &'static str,
    /// The client-visible message, already redacted for storage-layer faults.
    pub message: String,
}

impl QueryErrorResponse {
    /// Map a [`QueryError`] to its HTTP rendering. Storage-layer faults are
    /// logged in full and redacted to a fixed class message exactly as the
    /// [`IntoResponse`] path does: this routes through the same
    /// [`ApiError`] conversion, so the two can never disagree.
    pub fn from_query_error(err: QueryError) -> Self {
        ApiError::from(err).into_parts()
    }
}

impl ApiError {
    /// The status, stable `errorType` tag, and message this error renders to.
    /// Extracted so both [`IntoResponse`] and the public
    /// [`QueryErrorResponse`] mapping share one table and cannot drift.
    fn into_parts(self) -> QueryErrorResponse {
        let (status, error_type, message) = match self {
            ApiError::BadData(msg) => (StatusCode::BAD_REQUEST, "bad_data", msg),
            ApiError::Unsupported(msg) => (StatusCode::UNPROCESSABLE_ENTITY, "execution", msg),
            ApiError::Corrupt(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", msg),
            ApiError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", msg),
            ApiError::Timeout(msg) => (StatusCode::GATEWAY_TIMEOUT, "timeout", msg),
            ApiError::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "authentication required".to_string(),
            ),
        };
        QueryErrorResponse {
            status,
            error_type,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let QueryErrorResponse {
            status,
            error_type,
            message,
        } = self.into_parts();
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
            ApiError::Unavailable(msg) | ApiError::Corrupt(msg) => msg,
            ApiError::BadData(msg) | ApiError::Unsupported(msg) | ApiError::Timeout(msg) => msg,
            ApiError::Unauthenticated => "authentication required".to_string(),
        }
    }

    /// The HTTP status a `QueryError` maps to, as a `u16`, exercising the full
    /// `From` + `IntoResponse` path.
    fn status_code(err: QueryError) -> u16 {
        ApiError::from(err).into_response().status().as_u16()
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
    fn eval_wrong_type_maps_to_bad_data_not_unsupported() {
        // WrongType is a client mistake (asked for a shape the query does
        // not produce), not an unimplemented construct: it must map like a
        // parse error (400), not like Unsupported (422).
        let err = QueryError::Eval(ravel_promql::Error::WrongType {
            expected: "instant vector",
            got: "range vector",
        });
        match ApiError::from(err) {
            ApiError::BadData(_) => {}
            other => panic!("expected BadData, got a different ApiError variant: {other:?}"),
        }
    }

    #[test]
    fn eval_too_many_points_maps_to_unsupported_not_bad_data() {
        // TooManyPoints is a resolution-budget rejection, grouped with the
        // other budget classes (TooManySegments/Series/Samples) under the
        // same 422 "execution" mapping, not the 400 "bad_data" mapping.
        let err = QueryError::Eval(ravel_promql::Error::TooManyPoints {
            points: 20_000,
            max: 11_000,
        });
        match ApiError::from(err) {
            ApiError::Unsupported(_) => {}
            other => panic!("expected Unsupported, got a different ApiError variant: {other:?}"),
        }
    }

    #[test]
    fn eval_ambiguous_match_maps_to_unsupported_not_unavailable() {
        // A many-to-many or unmarked many-to-one binary-operator match is a
        // client-side query mistake, not a storage fault: it must not fall
        // into the catch-all's blanket 503 redaction, which would hide a
        // real, query-derived (never backend-derived) message behind the
        // generic unavailable text.
        let err = QueryError::Eval(ravel_promql::Error::AmbiguousMatch {
            detail: "many-to-many matching not allowed".to_string(),
        });
        match ApiError::from(err) {
            ApiError::Unsupported(_) => {}
            other => panic!("expected Unsupported, got a different ApiError variant: {other:?}"),
        }
    }

    #[test]
    fn eval_invalid_regex_maps_to_unsupported_not_unavailable() {
        let err = QueryError::Eval(ravel_promql::Error::InvalidRegex {
            pattern: "(unterminated".to_string(),
            reason: "unclosed group".to_string(),
        });
        match ApiError::from(err) {
            ApiError::Unsupported(_) => {}
            other => panic!("expected Unsupported, got a different ApiError variant: {other:?}"),
        }
    }

    #[test]
    fn eval_invalid_label_name_maps_to_unsupported_not_unavailable() {
        let err = QueryError::Eval(ravel_promql::Error::InvalidLabelName {
            label: "1bad".to_string(),
        });
        match ApiError::from(err) {
            ApiError::Unsupported(_) => {}
            other => panic!("expected Unsupported, got a different ApiError variant: {other:?}"),
        }
    }

    #[test]
    fn non_monotonic_samples_is_not_a_retryable_503() {
        // a7-F05 (#62): NonMonotonicSamples is a permanent decode-corruption
        // condition. It must not map to 503 `unavailable` (which a Prometheus
        // client retries forever against the same corrupt stored data); it maps
        // to the non-retryable 500 `internal`.
        let err = QueryError::NonMonotonicSamples { prev: 2, next: 1 };
        assert_eq!(status_code(err), 500, "corruption must not be a 503");

        // Explicit: the mapped variant is Corrupt, and it is 500, not 503.
        match ApiError::from(QueryError::NonMonotonicSamples { prev: 2, next: 1 }) {
            ApiError::Corrupt(_) => {}
            other => panic!("expected Corrupt, got a different ApiError variant: {other:?}"),
        }
        assert_ne!(
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn sibling_corruption_decode_errors_are_500_not_503() {
        // The other permanent-corruption faults that previously shared the
        // MSG_CORRUPT/503 mapping now also carry the non-retryable 500.
        let fetch_corrupt = QueryError::Fetch(FetchError::Corrupt {
            key: LEAKY_KEY.to_string(),
            source: ravel_segment::SegmentError::BadMagic,
        });
        assert_eq!(status_code(fetch_corrupt), 500);

        let catalog_mismatch = QueryError::Catalog(CatalogError::FieldMismatch {
            key: LEAKY_KEY.to_string(),
            field: "tenant_hash",
            expected: "aaaa".to_string(),
            actual: TENANT_HASH.to_string(),
        });
        assert_eq!(status_code(catalog_mismatch), 500);
    }

    #[test]
    fn transient_storage_faults_stay_retryable_503() {
        // The remapping touches only the corruption class: genuinely transient
        // faults keep the retryable 503 `unavailable` so clients still retry.
        let store = QueryError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Transient(RAW_STORE_TEXT.to_string()),
        });
        assert_eq!(status_code(store), 503);

        let etag = QueryError::Fetch(FetchError::EtagChanged {
            key: LEAKY_KEY.to_string(),
        });
        assert_eq!(status_code(etag), 503);

        assert_eq!(status_code(QueryError::SnapshotInvalidated), 503);
    }

    #[test]
    fn window_too_wide_is_a_422_that_keeps_its_counts() {
        // Issue #635: an over-wide window refused before any LIST is a
        // resource-budget rejection grouped with the budget classes at 422
        // "execution", not a storage fault at 503. It is not redacted (counts
        // only), and its text reaches the client verbatim.
        let err = QueryError::Catalog(CatalogError::WindowTooWide {
            estimate: 496_089,
            limit: 100_000,
        });
        assert!(
            redacted_storage_message(&err).is_none(),
            "a counts-only refusal must not be redacted"
        );
        assert_eq!(status_code(err), 422);

        let msg = client_message(QueryError::Catalog(CatalogError::WindowTooWide {
            estimate: 496_089,
            limit: 100_000,
        }));
        assert!(
            msg.contains("496089"),
            "estimate must survive to the client"
        );
        assert!(msg.contains("100000"), "limit must survive to the client");
        assert!(
            msg.contains("narrow"),
            "message must tell the caller what to do"
        );
        assert_redacted(&msg);

        match ApiError::from(QueryError::Catalog(CatalogError::WindowTooWide {
            estimate: 1,
            limit: 0,
        })) {
            ApiError::Unsupported(_) => {}
            other => panic!("expected Unsupported, got a different ApiError variant: {other:?}"),
        }
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

    /// The public [`QueryErrorResponse`] mapping and the `IntoResponse` path
    /// must render every `QueryError` variant identically: same status, same
    /// `errorType` tag, and same (redacted) message. This is what lets the
    /// analytics endpoint in `ravel-server` consume the public mapping instead
    /// of carrying its own copy without the two drifting. A fresh value is
    /// built for each path so neither observes the other's consumption.
    #[test]
    fn public_mapping_agrees_with_into_response_for_every_variant() {
        use std::time::Duration;

        use ravel_promql::Error as PromErr;

        let cases: Vec<fn() -> QueryError> = vec![
            || QueryError::Parse("bad".to_string()),
            || QueryError::Unsupported {
                construct: "x".to_string(),
            },
            || QueryError::NonPositiveStep { step_ms: 0 },
            || QueryError::InvalidRange {
                start_ms: 5,
                end_ms: 1,
            },
            || QueryError::TimeOverflow,
            || QueryError::TooManySegments { count: 9, max: 3 },
            || QueryError::TooManySeries { count: 9, max: 3 },
            || QueryError::TooManySamples { count: 9, max: 3 },
            || QueryError::TooManyBytesScanned {
                scanned: 9_000,
                max: 3_000,
            },
            || QueryError::DeadlineExceeded {
                deadline: Duration::from_secs(1),
            },
            || QueryError::SnapshotInvalidated,
            || QueryError::NonMonotonicSamples { prev: 2, next: 1 },
            // Fetch sub-variants: corrupt (500) vs transient (503).
            || {
                QueryError::Fetch(FetchError::Store {
                    key: LEAKY_KEY.to_string(),
                    source: StoreError::Transient(RAW_STORE_TEXT.to_string()),
                })
            },
            || {
                QueryError::Fetch(FetchError::EtagChanged {
                    key: LEAKY_KEY.to_string(),
                })
            },
            || {
                QueryError::Fetch(FetchError::Corrupt {
                    key: LEAKY_KEY.to_string(),
                    source: ravel_segment::SegmentError::BadMagic,
                })
            },
            // Catalog sub-variants: unsatisfiable token, corruption, transient.
            || {
                QueryError::Catalog(CatalogError::UnsatisfiableToken {
                    shard: 0,
                    writer_id: "writer-7".to_string(),
                    epoch: 1,
                    seq: 2,
                    ingest_hour_bucket: 3,
                })
            },
            || {
                QueryError::Catalog(CatalogError::FieldMismatch {
                    key: LEAKY_KEY.to_string(),
                    field: "tenant_hash",
                    expected: "aaaa".to_string(),
                    actual: TENANT_HASH.to_string(),
                })
            },
            || {
                QueryError::Catalog(CatalogError::Store(StoreError::Permanent(
                    RAW_STORE_TEXT.to_string(),
                )))
            },
            || {
                QueryError::Catalog(CatalogError::WindowTooWide {
                    estimate: 496_089,
                    limit: 100_000,
                })
            },
            // Eval sub-variants: bad_data vs execution vs redacted source.
            || {
                QueryError::Eval(PromErr::WrongType {
                    expected: "instant vector",
                    got: "range vector",
                })
            },
            || QueryError::Eval(PromErr::TooManyPoints { points: 9, max: 3 }),
            || {
                QueryError::Eval(PromErr::AmbiguousMatch {
                    detail: "many-to-many".to_string(),
                })
            },
            || {
                QueryError::Eval(PromErr::InvalidRegex {
                    pattern: "(".to_string(),
                    reason: "unclosed group".to_string(),
                })
            },
            || {
                QueryError::Eval(PromErr::InvalidLabelName {
                    label: "1bad".to_string(),
                })
            },
        ];

        for make in cases {
            let public = QueryErrorResponse::from_query_error(make());

            // Drive the real `IntoResponse` path and read status + body back.
            let response = ApiError::from(make()).into_response();
            let status = response.status();
            let bytes =
                futures::executor::block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
                    .expect("read response body");
            let json: serde_json::Value =
                serde_json::from_slice(&bytes).expect("response body is JSON");

            assert_eq!(public.status, status, "status disagreed for {:?}", make());
            assert_eq!(json["status"], "error", "envelope status for {:?}", make());
            assert_eq!(
                json["errorType"].as_str().expect("errorType present"),
                public.error_type,
                "errorType disagreed for {:?}",
                make()
            );
            assert_eq!(
                json["error"].as_str().expect("error message present"),
                public.message,
                "message disagreed for {:?}",
                make()
            );
        }
    }
}
