//! Maps internal error types to Prometheus-shaped HTTP error responses.
//!
//! Storage-layer faults (catalog resolution, segment fetch, and the
//! object-store errors they wrap) carry the physical object key, the tenant
//! hash embedded in that key, and raw backend error text in their `Display`
//! form. Those strings must never reach a client body: the tenant-hashed key
//! layout exists precisely to keep the physical layout opaque (ADR-0009,
//! "no tenant names leaked via object listings"). This module
//! is the typed-error to HTTP boundary where that redaction happens: the
//! caller sees a stable, class-specific message with no internal identifiers,
//! while the full error is logged server-side for diagnosis.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ravel_catalog::{CatalogError, HEAD_FORMAT_VERSION, SnapshotFormatError};
use ravel_commit::erasure::ErasureError;
use ravel_commit::record::RecordError;
use ravel_cpu_gate::CpuGateError;
use ravel_object_store::StoreError;

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

/// Stable client message for a segment fetch the process memory budget
/// refused (`FetchMemoryExhausted`). Same class as [`MSG_UNAVAILABLE`] (503,
/// retryable), worded in the memory family so a caller can tell memory
/// pressure from a storage fault. The SQL surface renders the same string.
pub const MSG_FETCH_MEMORY_EXHAUSTED: &str = "query memory budget exhausted: the process could not reserve memory to fetch segment data; retry";

/// Stable client message for a query whose evaluation failed on the server:
/// it panicked on the read CPU gate. Non-retryable, like a corruption fault.
pub const MSG_INTERNAL: &str = "query evaluation failed on the server";

/// Stable client message for a query whose evidential audit event could not be
/// made durable (ADR-0062 section 2a). Distinct from [`MSG_UNAVAILABLE`]: the
/// read itself may have succeeded, and what failed is the audit trail, so an
/// operator reading a client report can tell the two apart. Every query
/// surface in the process, HTTP and Flight SQL alike, renders this exact
/// string, so the wire contract cannot drift between transports.
pub const MSG_AUDIT_UNAVAILABLE: &str = "query audit is temporarily unavailable; retry";

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
    /// retry re-reads the same bytes and never clears. Kept
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
        // hash) is logged here and never placed in the body.
        if let Some(redacted) = redacted_storage_message(&e) {
            tracing::warn!(
                error = %e,
                client_message = redacted,
                "storage-layer query error redacted from client response",
            );
            // A corruption fault is a permanent server-side data problem, not a
            // transient outage: map it to the non-retryable 500 `internal` so a
            // client stops retrying against permanently corrupt data.
            // Transient storage faults keep the retryable 503.
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
            | QueryError::TooManyBytesScanned { .. }
            // A coordinator refusing a remote's oversized slice stream (issue
            // #1687 part B) is the same class: it declined to hold what the
            // remote offered, and the message carries only two counts of the
            // coordinator's own, so it is echoed like the other budget trips
            // rather than redacted to the retryable 503 a `Distrib` outage takes.
            | QueryError::TooManySliceFrames { .. }
            | QueryError::TooManySliceBytes { .. }
            | QueryError::RequestBudgetExceeded { .. } => ApiError::Unsupported(e.to_string()),
            // An over-wide window refused before any LIST is a
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
            // A federated fetch failure (a remote cluster was unavailable with
            // skip_unavailable off, or framed a bad response) is a server-side
            // outage from the client's view; its `reason` may name an internal
            // cluster, so it is redacted to the same retryable 503.
            | QueryError::Federation { .. }
            | QueryError::NonMonotonicSamples { .. }
            // A run whose per-sample dedup priority column is not parallel to
            // its samples is the same class as a non-ascending run: the decoded
            // object contradicts the format's invariants, so it is reported as
            // a server-side fault with a fixed message rather than echoing
            // internal column lengths.
            | QueryError::PrioritySampleCountMismatch { .. }
            // ADR-0103: the same series arrived from two slices under the
            // pushdown eligibility gate, which guarantees each series lives on
            // exactly one worker. A repeat is a server-side gate violation the
            // query fails closed on rather than keeping one of two values; its
            // message carries only a series id, redacted to the fixed message.
            | QueryError::DuplicatePushdownSeries { .. }
            // An evaluation the runtime dropped at shutdown, or a closed gate,
            // never ran: retrying elsewhere can succeed.
            | QueryError::CpuGate(CpuGateError::Cancelled | CpuGateError::Closed) => {
                ApiError::Unavailable(MSG_UNAVAILABLE.to_string())
            }
            // An evaluation that panicked on the read CPU gate panics again on
            // the same data (ADR-1702 decision 2), so it takes the
            // non-retryable 500 with a fixed message.
            QueryError::CpuGate(CpuGateError::Panicked) => {
                ApiError::Corrupt(MSG_INTERNAL.to_string())
            }
        }
    }
}

/// Returns the redacted, class-specific client message for a storage-layer
/// fault, or `None` for errors whose `Display` is already safe to show the
/// caller (parse errors carry only the client's own query, budget errors
/// carry only counts and limits, deadlines carry only a duration).
///
/// The four client-visible classes required for redaction are kept distinct:
/// `corrupt` maps to HTTP 500 `internal` (a permanent, non-retryable
/// server-side data fault), while `unavailable` and
/// unsatisfiable-token map to the retryable HTTP 503; each carries its own
/// stable message so diagnosability survives redaction; the budget class
/// keeps its own 422 mapping and unredacted counts.
///
/// The catalog arm follows the same rule the SQL boundary's `redact_catalog`
/// does, so both surfaces answer the same fault the same way: a fault in stored
/// bytes whose format version this build covers, a version or enum value below
/// the supported minimum included, is corrupt (500, non-retryable), while a
/// catalog object carrying a format version or enum value above the highest
/// this build reads stays unavailable (503, retryable), because a peer on a
/// newer build can read it during a rolling upgrade. An erasure request's or
/// rewrite record's unknown signal is corrupt at every value, since it is read
/// only under its own signal's key prefix, and a provisioning fault takes the
/// class [`ravel_catalog::ProvisioningError::is_retryable`] gives it. A catalog
/// store fault is retryable except a checksum mismatch (`StoreError::Corrupted`),
/// which is corrupt. Every
/// catalog variant is named (no wildcard) so a new one fails to compile until
/// it is classified.
fn redacted_storage_message(err: &QueryError) -> Option<&'static str> {
    match err {
        QueryError::Fetch(fetch) => Some(match fetch {
            FetchError::Corrupt { .. } => MSG_CORRUPT,
            // An RLOG fault reaches this enum as `Store` carrying a
            // `Corrupted` source, because `FetchError::Corrupt` can only hold
            // an RSEG `SegmentError`: `engine`'s log-series mapper folds
            // `LogFetchError::Corrupt` and `CarryMismatch` in that way. Both
            // are permanent data faults, so they take the corruption class
            // here rather than the retryable one, which a client would retry
            // forever against data that cannot change.
            FetchError::Store {
                source: StoreError::Corrupted(_),
                ..
            } => MSG_CORRUPT,
            FetchError::Store { .. } | FetchError::EtagChanged { .. } => MSG_UNAVAILABLE,
            // A memory-budget refusal is transient backpressure, not a storage
            // fault: same retryable class, memory-family wording.
            FetchError::FetchMemoryExhausted { .. } => MSG_FETCH_MEMORY_EXHAUSTED,
        }),
        // An over-wide-window refusal carries only counts and is
        // safe to show; like the budget errors it is not a storage fault, so
        // it is not redacted (the `From` impl maps it to a 422).
        QueryError::Catalog(CatalogError::WindowTooWide { .. }) => None,
        QueryError::Catalog(catalog) => Some(match catalog {
            CatalogError::UnsatisfiableToken { .. } => MSG_UNSATISFIABLE,

            // Corrupt stored data whose format version this build covers: a
            // retry re-reads the same bytes and fails the same way, so it is
            // the non-retryable 500, not the retryable 503. The supersession
            // faults are properties of the stored records (the chain depth
            // bound is a fixed constant, the same on every build), and the
            // column-stats part ceiling is a fixed format constant, so every
            // node refuses them alike.
            CatalogError::Reconstruction { .. }
            | CatalogError::FieldMismatch { .. }
            | CatalogError::Key(_)
            | CatalogError::RewriteSupersessionChainTooDeep { .. }
            | CatalogError::RewriteSupersessionCycle { .. }
            | CatalogError::CompactionSupersessionInputMismatch { .. }
            | CatalogError::ColumnStatsPartOverBound { .. } => MSG_CORRUPT,
            CatalogError::Record(source) | CatalogError::CompactionRecordDecode { source, .. } => {
                redacted_record_message(source)
            }
            CatalogError::ErasureRequestDecode { source, .. }
            | CatalogError::RewriteRecordDecode { source, .. } => redacted_erasure_message(source),
            CatalogError::SnapshotFormat(source) => redacted_snapshot_format_message(source),

            // A newer on-object format version this build cannot read is
            // retryable: a peer on a newer build can read it during a rolling
            // upgrade. 503. A HEAD below the floor is corrupt. 500.
            CatalogError::UnsupportedHeadVersion { format_version } => {
                if *format_version > HEAD_FORMAT_VERSION {
                    MSG_UNAVAILABLE
                } else {
                    MSG_CORRUPT
                }
            }

            // Never reached: the outer arm returns None for WindowTooWide.
            // Matched only for exhaustiveness.
            CatalogError::WindowTooWide { .. } => MSG_UNAVAILABLE,

            // A provisioning record's version above the read ceiling, a lost
            // CAS race or a transient store fault is retryable (503); a corrupt
            // or below-floor record is not (500).
            CatalogError::Provisioning(source) => {
                if source.is_retryable() {
                    MSG_UNAVAILABLE
                } else {
                    MSG_CORRUPT
                }
            }

            // A checksum mismatch on a catalog object GET: the bytes read do
            // not match the checksum stored with them. 500.
            CatalogError::Store(StoreError::Corrupted(_)) => MSG_CORRUPT,

            // Transient storage faults, fold progress/liveness failures, and
            // resource backpressure stay retryable.
            CatalogError::InvalidConfig(_)
            | CatalogError::Store(_)
            | CatalogError::FoldCasRetriesExhausted { .. }
            | CatalogError::MemoryExhausted(_) => MSG_UNAVAILABLE,
        }),
        QueryError::NonMonotonicSamples { .. } => Some(MSG_CORRUPT),
        QueryError::SnapshotInvalidated => Some(MSG_UNAVAILABLE),
        _ => None,
    }
}

/// A commit or compaction record: retryable only for a format version above the
/// highest this build reads. A version below the floor (a writer that left
/// proto3's default 0) is corrupt, since no build reads it.
fn redacted_record_message(err: &RecordError) -> &'static str {
    if err.is_newer_format_version() {
        MSG_UNAVAILABLE
    } else {
        MSG_CORRUPT
    }
}

/// An erasure request or rewrite record: retryable only for a format version
/// above the highest this build reads.
fn redacted_erasure_message(err: &ErasureError) -> &'static str {
    if err.is_newer_format_version() {
        MSG_UNAVAILABLE
    } else {
        MSG_CORRUPT
    }
}

/// A snapshot part, HEAD, postings or column-stats fault. A decode job the read
/// CPU gate dropped at shutdown, or a closed gate, never ran, so a retry on a
/// healthy node can succeed; one that panicked panics again on the same bytes.
fn redacted_snapshot_format_message(err: &SnapshotFormatError) -> &'static str {
    if err.is_newer_format_version()
        || matches!(
            err,
            SnapshotFormatError::DecodeJob(CpuGateError::Cancelled | CpuGateError::Closed)
        )
    {
        MSG_UNAVAILABLE
    } else {
        MSG_CORRUPT
    }
}

fn from_eval_error(inner: &ravel_promql::Error, outer: &QueryError) -> ApiError {
    match inner {
        ravel_promql::Error::Parse(_)
        | ravel_promql::Error::TooComplex(_)
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
        // log the full detail rather than echo it to the client.
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
/// already passed the redaction boundary (storage-layer faults carry
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
    pub fn into_parts(self) -> QueryErrorResponse {
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
    use ravel_commit::record::RecordKind;
    use ravel_object_store::StoreError;

    use super::*;

    /// A representative internal object key: tenant-hashed prefix, signal,
    /// level, shard, writer id, and the `.rseg` suffix (ADR-0010 key layout).
    const LEAKY_KEY: &str = "t/deadbeefcafef00d/metrics/l0/0/writer-7.1.2.0123456789abcdef.rseg";
    const TENANT_HASH: &str = "deadbeefcafef00d";
    const RAW_STORE_TEXT: &str = "bucket=prod-telemetry endpoint=s3.internal request-id=abc";

    /// The redacted client message must not carry the object key, the tenant
    /// hash embedded in it, the `.rseg` suffix, the `t/` prefix, or raw
    /// backend error text.
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

    /// A fetch the process memory budget refused keeps the retryable 503
    /// `unavailable` class but says it is memory, not storage.
    ///
    /// FLIP: fold `FetchMemoryExhausted` back into the `MSG_UNAVAILABLE` arm
    /// of `redacted_storage_message` and the message assertion fails.
    #[test]
    fn fetch_memory_refusal_is_503_with_the_memory_message() {
        let p = ApiError::from(QueryError::Fetch(FetchError::FetchMemoryExhausted {
            requested: 2,
            reserved: 1,
            limit: 2,
        }))
        .into_parts();
        assert_eq!(p.status.as_u16(), 503);
        assert_eq!(p.error_type, "unavailable");
        assert_eq!(p.message, MSG_FETCH_MEMORY_EXHAUSTED);
        assert_ne!(MSG_FETCH_MEMORY_EXHAUSTED, MSG_UNAVAILABLE);
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

    /// A catalog store fault is the retryable 503 unless the store reports a
    /// checksum mismatch, which is the non-retryable 500. The SQL boundary pins
    /// the same split.
    ///
    /// FLIP: drop the `CatalogError::Store(StoreError::Corrupted(_))` arm of
    /// `redacted_storage_message` and the corrupt case fails with
    /// `left: "upstream storage temporarily unavailable"`,
    /// `right: "stored data failed integrity validation"`.
    #[test]
    fn catalog_store_error_is_503_unless_a_checksum_mismatch() {
        let err = QueryError::Catalog(CatalogError::Store(StoreError::Permanent(
            RAW_STORE_TEXT.to_string(),
        )));
        assert!(err.to_string().contains(RAW_STORE_TEXT));
        let p = ApiError::from(err).into_parts();
        assert_eq!(p.message, MSG_UNAVAILABLE);
        assert_redacted(&p.message);
        assert_eq!(p.status.as_u16(), 503);
        assert_eq!(p.error_type, "unavailable");

        let corrupt = QueryError::Catalog(CatalogError::Store(StoreError::Corrupted(
            RAW_STORE_TEXT.to_string(),
        )));
        let p = ApiError::from(corrupt).into_parts();
        assert_eq!(p.message, MSG_CORRUPT);
        assert_redacted(&p.message);
        assert_eq!(p.status.as_u16(), 500);
        assert_eq!(p.error_type, "internal");
    }

    /// A decode failure of stored catalog bytes whose format version this build
    /// covers is the non-retryable 500 `internal`: a retry re-reads the same
    /// bytes. A newer format version this build cannot read stays the retryable
    /// 503 `unavailable`, because a peer on a newer build can read it during a
    /// rolling upgrade. The SQL boundary pins the same split, so both surfaces
    /// answer alike.
    #[test]
    fn undecodable_catalog_objects_are_500_newer_versions_stay_503() {
        let parts = |err: QueryError| ApiError::from(err).into_parts();

        // Decode faults of a covered format version: non-retryable 500 internal.
        let corrupt: Vec<fn() -> QueryError> = vec![
            || {
                QueryError::Catalog(CatalogError::CompactionRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: RecordError::InvalidTenantHashLen(3),
                })
            },
            || {
                QueryError::Catalog(CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::InvalidTenantHashLen(3),
                })
            },
            || QueryError::Catalog(CatalogError::SnapshotFormat(SnapshotFormatError::BadMagic)),
        ];
        for make in &corrupt {
            let p = parts(make());
            assert_eq!(p.status.as_u16(), 500, "{:?}", make());
            assert_eq!(p.error_type, "internal", "{:?}", make());
            assert_eq!(p.message, MSG_CORRUPT, "{:?}", make());
            assert_redacted(&p.message);
        }

        // Newer-format-version cases, including the unsupported-version case each
        // decode fault carries in its source: retryable 503 unavailable.
        let unavailable: Vec<fn() -> QueryError> = vec![
            || QueryError::Catalog(CatalogError::UnsupportedHeadVersion { format_version: 2 }),
            || {
                QueryError::Catalog(CatalogError::SnapshotFormat(
                    SnapshotFormatError::UnsupportedVersion(2),
                ))
            },
            || {
                QueryError::Catalog(CatalogError::CompactionRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: RecordError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 2,
                    },
                })
            },
            || {
                QueryError::Catalog(CatalogError::CompactionRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: RecordError::UnsupportedRecordFormatVersion {
                        kind: RecordKind::Compaction,
                        min: 1,
                        max: 2,
                        actual: 3,
                    },
                })
            },
            || {
                QueryError::Catalog(CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 2,
                    },
                })
            },
        ];
        for make in &unavailable {
            let p = parts(make());
            assert_eq!(p.status.as_u16(), 503, "{:?}", make());
            assert_eq!(p.error_type, "unavailable", "{:?}", make());
            assert_eq!(p.message, MSG_UNAVAILABLE, "{:?}", make());
            assert_redacted(&p.message);
        }

        // Control: an existing corrupt catalog variant stays 500, a transient
        // store fault stays 503.
        let field = parts(QueryError::Catalog(CatalogError::FieldMismatch {
            key: LEAKY_KEY.to_string(),
            field: "tenant_hash",
            expected: "aaaa".to_string(),
            actual: TENANT_HASH.to_string(),
        }));
        assert_eq!(field.status.as_u16(), 500);
        assert_eq!(field.error_type, "internal");
        let store = parts(QueryError::Catalog(CatalogError::Store(
            StoreError::Permanent(RAW_STORE_TEXT.to_string()),
        )));
        assert_eq!(store.status.as_u16(), 503);
        assert_eq!(store.error_type, "unavailable");
    }

    /// Every snapshot-format object's newer-version case (part, HEAD, postings,
    /// column-stats, and an entry level above the highest this build reads) is
    /// the retryable 503, as is a decode job the read CPU gate cancelled or
    /// refused while closed. A panicked decode job, a retired column-stats
    /// version below the accepted one, and a declared body over the decode cap
    /// (a catalog cap no server flag sets, so every node refuses it) are the
    /// non-retryable 500. The SQL boundary pins the same split.
    #[test]
    fn snapshot_format_newer_versions_and_gate_aborts_are_503() {
        let parts = |err: SnapshotFormatError| {
            ApiError::from(QueryError::Catalog(CatalogError::SnapshotFormat(err))).into_parts()
        };

        let unavailable: Vec<fn() -> SnapshotFormatError> = vec![
            || SnapshotFormatError::UnsupportedVersion(2),
            || SnapshotFormatError::UnsupportedHeadVersion(2),
            || SnapshotFormatError::PostingsUnsupportedVersion(2),
            || SnapshotFormatError::ColumnStatsUnsupportedVersion(4),
            || SnapshotFormatError::UnsupportedLevel(2),
            || SnapshotFormatError::DecodeJob(CpuGateError::Cancelled),
            || SnapshotFormatError::DecodeJob(CpuGateError::Closed),
        ];
        for make in &unavailable {
            let p = parts(make());
            assert_eq!(p.status.as_u16(), 503, "{:?}", make());
            assert_eq!(p.error_type, "unavailable", "{:?}", make());
            assert_eq!(p.message, MSG_UNAVAILABLE, "{:?}", make());
        }

        let corrupt: Vec<fn() -> SnapshotFormatError> = vec![
            || SnapshotFormatError::DecodeJob(CpuGateError::Panicked),
            || SnapshotFormatError::ColumnStatsUnsupportedVersion(2),
            || SnapshotFormatError::DecompressedTooLarge {
                declared: 2,
                cap: 1,
            },
            || SnapshotFormatError::HeaderVersionMismatch {
                header: 2,
                envelope: 1,
            },
        ];
        for make in &corrupt {
            let p = parts(make());
            assert_eq!(p.status.as_u16(), 500, "{:?}", make());
            assert_eq!(p.error_type, "internal", "{:?}", make());
            assert_eq!(p.message, MSG_CORRUPT, "{:?}", make());
        }
    }

    /// Asserts every error `makes` builds renders as `status`, with the
    /// matching `errorType` and redacted message.
    fn assert_catalog_status(makes: &[fn() -> CatalogError], status: u16) {
        let (error_type, message) = match status {
            500 => ("internal", MSG_CORRUPT),
            503 => ("unavailable", MSG_UNAVAILABLE),
            other => panic!("no catalog class renders as {other}"),
        };
        for make in makes {
            let p = ApiError::from(QueryError::Catalog(make())).into_parts();
            assert_eq!(p.status.as_u16(), status, "{:?}", make());
            assert_eq!(p.error_type, error_type, "{:?}", make());
            assert_eq!(p.message, message, "{:?}", make());
            assert_redacted(&p.message);
        }
    }

    /// A version below the supported minimum (a writer that left proto3's
    /// default 0) is permanent corruption of an immutable object, not a newer
    /// version a peer can read: 500 for every version kind, record, erasure
    /// object and snapshot format alike. The SQL boundary pins the same cases.
    ///
    /// FLIP: answer every `UnsupportedHeadVersion` with `MSG_UNAVAILABLE` and
    /// the first case fails with `left: 503`, `right: 500`.
    #[test]
    fn below_floor_versions_are_500_not_503() {
        assert_catalog_status(
            &[
                || CatalogError::UnsupportedHeadVersion { format_version: 0 },
                || CatalogError::CompactionRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: RecordError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 0,
                    },
                },
                || {
                    CatalogError::Record(RecordError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 0,
                    })
                },
                || CatalogError::CompactionRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: RecordError::UnsupportedRecordFormatVersion {
                        kind: RecordKind::Compaction,
                        min: 1,
                        max: 2,
                        actual: 0,
                    },
                },
                || CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 0,
                    },
                },
                || CatalogError::RewriteRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 0,
                    },
                },
                || CatalogError::SnapshotFormat(SnapshotFormatError::UnsupportedVersion(0)),
                || CatalogError::SnapshotFormat(SnapshotFormatError::UnsupportedHeadVersion(0)),
                || CatalogError::SnapshotFormat(SnapshotFormatError::PostingsUnsupportedVersion(0)),
                || {
                    CatalogError::SnapshotFormat(
                        SnapshotFormatError::ColumnStatsUnsupportedVersion(0),
                    )
                },
            ],
            500,
        );
    }

    /// An entry level or column declared type above the highest this build
    /// reads is the retryable 503, because a new value can ship without a
    /// format version bump and a peer on a newer build can read it; a declared
    /// type of 0, proto3's default and an unstamped field, is the
    /// non-retryable 500. An erasure signal or deferral cause is 500 at every
    /// value, under both erasure wrappers: a newer build writes a new signal
    /// under a key prefix this build never lists, so an unknown one read here
    /// disagrees with its own key. The SQL boundary pins the same cases.
    ///
    /// FLIP: classify `ErasureError::UnknownSignal` above `Signal::Audit` as
    /// newer and the first erasure-request case fails with `left: 503`,
    /// `right: 500`.
    #[test]
    fn unknown_level_and_type_above_the_maximum_are_503_erasure_enums_500() {
        assert_catalog_status(
            &[
                || CatalogError::SnapshotFormat(SnapshotFormatError::UnsupportedLevel(2)),
                || {
                    CatalogError::SnapshotFormat(
                        SnapshotFormatError::ColumnStatsUnknownDeclaredType {
                            name: "c".to_string(),
                            declared_type: 5,
                        },
                    )
                },
            ],
            503,
        );
        assert_catalog_status(
            &[
                || CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownSignal(7),
                },
                || CatalogError::RewriteRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownSignal(7),
                },
                || CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownSignal(0),
                },
                || CatalogError::RewriteRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownSignal(0),
                },
                || CatalogError::ErasureRequestDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownDeferralCause(2),
                },
                || CatalogError::RewriteRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::UnknownDeferralCause(2),
                },
                || {
                    CatalogError::SnapshotFormat(
                        SnapshotFormatError::ColumnStatsUnknownDeclaredType {
                            name: "c".to_string(),
                            declared_type: 0,
                        },
                    )
                },
            ],
            500,
        );
    }

    /// A provisioning record above the read ceiling, a lost CAS race and a
    /// transient store fault are the retryable 503; an undecodable, misfiled
    /// or structurally corrupt record, a version below the floor, a checksum
    /// mismatch and a refused reshard argument are the non-retryable 500. The
    /// SQL boundary pins the same split.
    ///
    /// FLIP: put `CatalogError::Provisioning(_)` back in the `MSG_UNAVAILABLE`
    /// arm and the first 500 case fails with `left: 503`, `right: 500`.
    #[test]
    fn provisioning_faults_take_the_class_of_the_record_fault() {
        use ravel_catalog::{
            GenerationDefect, PROVISIONING_MAX_READ_VERSION, PROVISIONING_MIN_READ_VERSION,
            ProvisioningError,
        };

        assert_catalog_status(
            &[
                || {
                    CatalogError::Provisioning(ProvisioningError::UnsupportedVersion {
                        key: LEAKY_KEY.to_string(),
                        got: PROVISIONING_MAX_READ_VERSION + 1,
                        ceiling: PROVISIONING_MAX_READ_VERSION,
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::Store {
                        key: LEAKY_KEY.to_string(),
                        source: StoreError::Timeout,
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::ReshardCasConflict {
                        key: LEAKY_KEY.to_string(),
                    })
                },
            ],
            503,
        );
        assert_catalog_status(
            &[
                || {
                    CatalogError::Provisioning(ProvisioningError::Decode {
                        key: LEAKY_KEY.to_string(),
                        source: <() as prost::Message>::decode(&[0xff][..])
                            .expect_err("a lone 0xff is not a valid message"),
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::VersionBelowFloor {
                        key: LEAKY_KEY.to_string(),
                        got: 0,
                        floor: PROVISIONING_MIN_READ_VERSION,
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::CorruptRecord {
                        key: LEAKY_KEY.to_string(),
                        field: "signal",
                        expected: "Metrics".to_string(),
                        actual: "Logs".to_string(),
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::CorruptGenerations {
                        key: LEAKY_KEY.to_string(),
                        defect: GenerationDefect::NotDense,
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::Store {
                        key: LEAKY_KEY.to_string(),
                        source: StoreError::Corrupted(RAW_STORE_TEXT.to_string()),
                    })
                },
                || {
                    CatalogError::Provisioning(ProvisioningError::ReshardSameCount {
                        shard_count: 4,
                    })
                },
            ],
            500,
        );
    }

    /// A commit record with a newer format version reaches the surface as
    /// `CatalogError::Record` (`record::decode` then `?`), and answers as the
    /// same `RecordError` does under `CompactionRecordDecode`: the retryable
    /// 503. A non-version record fault stays 500.
    ///
    /// FLIP: put `CatalogError::Record(_)` back in the `MSG_CORRUPT` arm and the
    /// first case fails with `left: 500`, `right: 503`.
    #[test]
    fn newer_commit_record_through_record_is_503() {
        assert_catalog_status(
            &[
                || {
                    CatalogError::Record(RecordError::UnsupportedFormatVersion {
                        expected: 1,
                        actual: 2,
                    })
                },
                || {
                    CatalogError::Record(RecordError::UnsupportedRecordFormatVersion {
                        kind: RecordKind::Commit,
                        min: 1,
                        max: 1,
                        actual: 2,
                    })
                },
            ],
            503,
        );
        assert_catalog_status(
            &[|| CatalogError::Record(RecordError::InvalidTenantHashLen(3))],
            500,
        );
    }

    /// The five catalog variants that used to answer 503 unclassified. A
    /// rewrite record that fails to decode is 500 unless its version is above
    /// the highest this build reads; a supersession chain past the fixed depth
    /// bound, a cycle, and a version 2 record naming a different input set are
    /// properties of the stored records; and the per-part column-stats ceiling
    /// is a fixed format constant. None of those clears on a retry.
    ///
    /// FLIP: move `RewriteSupersessionCycle` back to the `MSG_UNAVAILABLE` arm
    /// and its case fails with `left: 503`, `right: 500`.
    #[test]
    fn rewrite_and_supersession_faults_are_500_newer_rewrites_503() {
        assert_catalog_status(
            &[
                || CatalogError::RewriteRecordDecode {
                    key: LEAKY_KEY.to_string(),
                    source: ErasureError::InvalidTenantHashLen(3),
                },
                || CatalogError::RewriteSupersessionChainTooDeep {
                    bucket: LEAKY_KEY.to_string(),
                    max: 64,
                },
                || CatalogError::RewriteSupersessionCycle {
                    key: LEAKY_KEY.to_string(),
                },
                || CatalogError::CompactionSupersessionInputMismatch {
                    key: LEAKY_KEY.to_string(),
                    superseded_key: LEAKY_KEY.to_string(),
                },
                || CatalogError::ColumnStatsPartOverBound {
                    part_key: LEAKY_KEY.to_string(),
                    declared: 2,
                    ceiling: 1,
                },
            ],
            500,
        );
        assert_catalog_status(
            &[|| CatalogError::RewriteRecordDecode {
                key: LEAKY_KEY.to_string(),
                source: ErasureError::UnsupportedFormatVersion {
                    expected: 1,
                    actual: 2,
                },
            }],
            503,
        );
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
    fn eval_too_complex_maps_to_bad_data_not_unavailable() {
        // A query rejected by the pre-parse complexity guard (issue #529)
        // is a malformed-input-shaped rejection, exactly like Parse: it
        // must not fall into the catch-all's blanket 503 redaction.
        let err = QueryError::Eval(ravel_promql::Error::TooComplex(
            ravel_promql::complexity_guard::QueryTooComplex { count: 9, max: 3 },
        ));
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
        // NonMonotonicSamples is a permanent decode-corruption
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
    fn log_path_corruption_is_500_not_503() {
        // The local (non-distributed) PromQL log path folds every RLOG fault
        // through `FetchError::Store`, since `FetchError::Corrupt` can only
        // carry an RSEG error. Without matching on the `Corrupted` source
        // these land in the retryable class, so a corrupt object and a carry
        // paired with the wrong segment would both answer 503 and a client
        // would retry forever against data that cannot change.
        let corrupt_rlog = QueryError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Corrupted("corrupt log segment: bad footer".to_string()),
        });
        assert_eq!(status_code(corrupt_rlog), 500);

        let carry_mismatch = QueryError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Corrupted(
                crate::log_fetcher::LogFetchError::CarryMismatch {
                    key: LEAKY_KEY.to_string(),
                    carried_key: format!("{LEAKY_KEY}.other"),
                    tenant: ravel_types::TenantHash([1u8; 16]),
                    carried_tenant: ravel_types::TenantHash([2u8; 16]),
                }
                .to_string(),
            ),
        });
        assert_eq!(status_code(carry_mismatch), 500);

        // Still redacted: neither key nor tenant reaches the body.
        match ApiError::from(QueryError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Corrupted(LEAKY_KEY.to_string()),
        })) {
            ApiError::Corrupt(msg) => {
                assert_eq!(msg, MSG_CORRUPT);
                assert!(!msg.contains(LEAKY_KEY), "leaked key: {msg}");
            }
            other => panic!("expected Corrupt, got {other:?}"),
        }
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
        // An over-wide window refused before any LIST is a
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

        // An internal identifier a distributed/federated error's `reason` may
        // carry (a remote endpoint, transport text, a cluster-internal cause).
        // The `Distrib`/`Federation` arms redact to the fixed `MSG_UNAVAILABLE`,
        // so this string must never survive into the client-facing message. The
        // per-case redaction assertion below pins that: interpolating `reason`
        // into either arm's public message makes it leak and fails the test.
        const REASON_LEAK: &str = "10.9.8.7:9443 cluster-internal cause";

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
                QueryError::Eval(PromErr::TooComplex(
                    ravel_promql::complexity_guard::QueryTooComplex { count: 9, max: 3 },
                ))
            },
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
            // Distributed/federated outages: their `reason` carries internal
            // detail (endpoint, transport text) that must be redacted to the
            // fixed MSG_UNAVAILABLE at this boundary, exactly like the storage
            // faults above. Both arms drop `reason` entirely; the redaction
            // assertion in the loop proves interpolating it back would leak.
            || QueryError::Distrib {
                reason: REASON_LEAK.to_string(),
            },
            || QueryError::Federation {
                cluster: "eu-west".to_string(),
                reason: REASON_LEAK.to_string(),
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

            // Redaction is pinned per variant: no case's client-facing message
            // may carry an internal `reason` identifier. Only the
            // Distrib/Federation cases embed REASON_LEAK, and both redact it
            // away, so this holds for every case; a mutation interpolating
            // `reason` into either public message reintroduces the leak here.
            assert!(
                !public.message.contains(REASON_LEAK),
                "redaction leaked an internal reason into the client message for {:?}: {}",
                make(),
                public.message
            );
        }
    }

    /// ADR-1702 decision 2: a gate job that panicked is the non-retryable 500
    /// whether it was an evaluation or a catalog decode, and a job that never
    /// ran is the retryable 503. The panic comes from a real gate job, and the
    /// fetch error from the mapping every RSEG catalog decode site uses.
    ///
    /// FLIP: mapping `CpuGateError::Panicked` back into the `Unavailable` arm
    /// of `From<QueryError>` reads `left: 503, right: 500` on the first status
    /// assertion; mapping it to a transient `FetchError::Store` in
    /// `fetcher::gate_failed` fails the `FetchError::Corrupt` match with that
    /// `Store` error.
    #[tokio::test]
    async fn a_panicked_gate_job_is_a_500_and_a_cancelled_one_a_503() {
        let gate = crate::read_gate_test_support::floor_zero_gate();
        let panicked = gate
            .run(
                ravel_cpu_gate::ReadSite::SegmentSection,
                ravel_cpu_gate::JobSize::Bytes(1),
                || -> u8 { panic!("decode panicked") },
            )
            .await
            .expect_err("a panicking job fails");
        assert!(matches!(panicked, CpuGateError::Panicked), "{panicked:?}");

        assert_eq!(
            status_code(QueryError::CpuGate(CpuGateError::Panicked)),
            500
        );
        let fetch = crate::fetcher::gate_failed(LEAKY_KEY, panicked);
        assert!(matches!(fetch, FetchError::Corrupt { .. }), "{fetch:?}");
        let message = client_message(QueryError::Fetch(crate::fetcher::gate_failed(
            LEAKY_KEY,
            CpuGateError::Panicked,
        )));
        assert_redacted(&message);
        assert_eq!(status_code(QueryError::Fetch(fetch)), 500);

        assert_eq!(
            status_code(QueryError::CpuGate(CpuGateError::Cancelled)),
            503
        );
        let cancelled = crate::fetcher::gate_failed(LEAKY_KEY, CpuGateError::Cancelled);
        assert!(
            matches!(
                cancelled,
                FetchError::Store {
                    source: StoreError::Transient(_),
                    ..
                }
            ),
            "{cancelled:?}"
        );
        assert_eq!(status_code(QueryError::Fetch(cancelled)), 503);
    }
}
