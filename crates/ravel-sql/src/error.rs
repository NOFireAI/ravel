//! Errors surfaced by the ravel-sql pipeline, and the redaction boundary
//! between their full server-side detail and what a client may see.
//!
//! Every fetch/decode failure is a hard, typed error carried across the
//! DataFusion boundary as `DataFusionError::External` so no operator ever
//! observes partial or silently-wrong data (docs/consistency-model.md
//! "never silent partial results").
//!
//! # Redaction
//!
//! `/api/v1/sql` is a second, independent error-to-HTTP boundary alongside
//! the PromQL one in crates/ravel-query/src/http/error.rs, and it carries
//! the same obligation. Two distinct leak sources meet here:
//!
//! - Storage-layer faults. Their `Display` embeds the physical object key,
//!   the tenant hash inside that key, and raw backend error text. The
//!   tenant-hashed key layout exists precisely to keep the physical layout
//!   opaque (ADR-0009), so none of it may reach a client body.
//! - DataFusion planning and execution errors. Their `Display` embeds
//!   schema fragments, column lists, resolved plan nodes, and (through
//!   `DataFusionError::External`) whatever a wrapped ravel error carried --
//!   including, transitively, an object key. Echoing them verbatim would
//!   reopen the same hole from the other side.
//!
//! [`SqlError::client_message`] is the single place that decides what a
//! caller sees: a fixed, class-specific string per [`ErrorClass`], with the
//! full `Display` left intact for the server to log. The endpoint logs
//! `%err` and returns `client_message()`; it never formats the error itself.
//!
//! Two classes are deliberately *not* redacted, matching the PromQL path:
//! validation errors (which quote only the caller's own SQL) and budget
//! errors (which carry only counts and limits an operator needs).

use datafusion::error::DataFusionError;
use ravel_catalog::CatalogError;
use ravel_query::{FetchError, LogFetchError};

use crate::spans_fetcher::SpanFetchError;
use crate::validate::ValidationError;

/// Stable client message for a data-integrity fault (corrupt segment,
/// unreconstructable or mismatched commit record). Full detail, including
/// the object key, is logged server-side only.
pub const MSG_CORRUPT: &str = "stored data failed integrity validation";

/// Stable client message for a transient storage-layer fault (object-store
/// error, changed etag between reads, invalidated snapshot).
pub const MSG_UNAVAILABLE: &str = "upstream storage temporarily unavailable";

/// Stable client message for a `min_commit_token` that did not resolve.
pub const MSG_UNSATISFIABLE: &str = "requested commit token is not yet visible; retry";

/// Stable client message for a DataFusion planning failure. Deliberately
/// says nothing about which column, type, or plan node was at fault: those
/// strings carry schema detail. The full error is logged server-side.
///
/// It names both v1 tables rather than one: a `Plan` error is built from a
/// bare `DataFusionError` in `crate::executor::plan_error`, which has no
/// handle on which table the failed query targeted, and a `logs` query can
/// fail to plan like any other (an unregistered function, an unknown
/// column). Naming only `samples` would point a `logs` client at the wrong
/// table.
///
/// This doc cited the `attrs['k']` subscript gap as the example until
/// `crate::map_field_planner` closed it. The reason for naming both tables
/// never depended on that particular gap, so only the example changed.
pub const MSG_PLAN: &str = "the SQL query could not be planned; check that it uses only the v1 subset \
     over the samples or logs table";

/// Stable client message for a DataFusion execution failure that is not one
/// of the classes above.
pub const MSG_EXECUTION: &str = "the SQL query failed during execution";

/// Stable client message for an internal invariant violation. These are
/// bugs; the caller gets nothing actionable and the server logs everything.
pub const MSG_INTERNAL: &str = "internal query engine error";

/// The client-visible class of a [`SqlError`]. The HTTP layer maps this to
/// a status code and an error-type tag; it never inspects the error itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// The request is malformed or outside the accepted SQL subset. 400.
    BadRequest,
    /// The request is well-formed but cannot be served: budget exceeded,
    /// memory pool exhausted, planning or execution failure. 422.
    Unsupported,
    /// A storage-layer fault. 503.
    Unavailable,
    /// The wall deadline expired. 504.
    Timeout,
}

/// A ravel-sql execution error.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// The request failed the read-only single-statement gate or the v1
    /// subset check, before any planning (crate::validate).
    #[error(transparent)]
    Validation(#[from] ValidationError),

    /// The query references both the `samples` and `logs` tables. ADR-0033
    /// decision C admits exactly one signal per query in v1 (no query needs to
    /// scan or join both metrics and logs), so this is rejected before any
    /// catalog resolve. Its text names only the two fixed table names -- no
    /// server state -- so it is safe to return verbatim, like a validation
    /// error, and maps to HTTP 400.
    #[error(
        "a SQL query may reference either the samples table or the logs table, \
         not both; metrics and logs cannot be scanned or joined together in v1"
    )]
    CrossSignalQuery,

    /// Snapshot resolution failed.
    #[error("snapshot resolution failed: {0}")]
    Catalog(#[from] CatalogError),

    /// A segment fetch or decode failed mid-scan.
    #[error("segment fetch failed: {0}")]
    Fetch(#[from] FetchError),

    /// An RLOG log-segment fetch or decode failed mid-scan (the `logs` table's
    /// sibling of [`SqlError::Fetch`]). Its `Display` embeds the
    /// object key, so it redacts the same way [`SqlError::Fetch`] does.
    #[error("log segment fetch failed: {0}")]
    LogFetch(#[from] LogFetchError),

    /// An RSPAN span-segment fetch or decode failed mid-scan (the `spans`
    /// table's sibling of [`SqlError::Fetch`]). Its `Display`
    /// embeds the object key, so it redacts the same way [`SqlError::Fetch`]
    /// does.
    #[error("span segment fetch failed: {0}")]
    SpanFetch(#[from] SpanFetchError),

    /// A record's canonical `stream_attrs` blob failed to decode during the
    /// scan's stream-attribute re-verification or `attrs`-column build
    /// (crate::logs_scan). This is the same data-integrity fault as the
    /// fetcher's own [`LogFetchError::Corrupt`] path -- a stored blob that
    /// failed integrity -- just detected one layer up, so it surfaces with the
    /// identical client class ([`ErrorClass::Unavailable`]) and message
    /// ([`MSG_CORRUPT`]) rather than collapsing into a generic internal error.
    /// The detail string carries no object key or tenant data and is logged
    /// server-side only.
    #[error("corrupt stream_attrs blob: {0}")]
    CorruptStreamAttrs(String),

    /// A pinned segment vanished and the re-resolve-and-retry contract was
    /// exhausted (docs/consistency-model.md).
    #[error("the pinned snapshot was invalidated during execution")]
    SnapshotInvalidated,

    /// The query wall deadline expired. Partial state is discarded.
    #[error("query exceeded its {millis} ms wall deadline")]
    DeadlineExceeded { millis: u64 },

    /// The post-dedup row count exceeded the configured `max_samples`
    /// budget (docs/query-engine.md "Budgets").
    #[error("query materialized too many samples: {count} exceeds max {max}")]
    TooManySamples { count: usize, max: usize },

    /// The resolved snapshot has more segments than `max_segments`.
    #[error("query fans out over too many segments: {count} exceeds max {max}")]
    TooManySegments { count: usize, max: usize },

    /// The distinct `series_id` count exceeded `max_series` while a scan
    /// partition was still building its runs. `count` is
    /// the distinct count observed by the one partition that tripped, not a
    /// cross-partition total; see crate::scan module doc.
    #[error("query matches too many series: {count} exceeds max {max}")]
    TooManySeries { count: usize, max: usize },

    /// The per-tenant bytes-scanned budget was exhausted mid-scan while a
    /// partition was still fetching segments (ADR-0061 decision 1). Distinct from [`SqlError::ResourcesExhausted`]: that bounds the
    /// query's decoded-memory pool, this bounds total S3 bytes scanned, a
    /// different resource, and the ADR requires the two stay distinguishable.
    /// Mirrors `ravel_query::QueryError::TooManyBytesScanned` so both query
    /// languages surface the same trip the same way. `scanned` and `max` are
    /// byte counts an operator needs, no server state, so it is echoed
    /// verbatim like the other budget errors.
    #[error("query scanned too many bytes: {scanned} exceeds max {max}")]
    TooManyBytesScanned { scanned: u64, max: u64 },

    /// The per-tenant S3 request budget was exhausted mid-scan, checked incrementally against
    /// `QueryAccounting::total_s3_requests()` at the same checkpoints as
    /// [`SqlError::TooManyBytesScanned`]. Mirrors
    /// `ravel_query::QueryError::RequestBudgetExceeded` so both query
    /// languages surface the same trip the same way; `requests` and `max`
    /// are counts an operator needs, no server state, so it is echoed
    /// verbatim like the other budget errors.
    #[error("query issued {requests} S3 requests, exceeding the budget of {max}")]
    RequestBudgetExceeded { requests: u64, max: u64 },

    /// The per-query or per-tenant byte budget was exhausted. The detail is
    /// the pool's own message (byte counts and limits only).
    #[error("query memory budget exhausted: {0}")]
    ResourcesExhausted(String),

    /// DataFusion could not plan the query. The payload is the full
    /// DataFusion message, kept for the server-side log only; it can carry
    /// schema and column detail and is never returned to a client.
    #[error("SQL planning failed: {0}")]
    Plan(String),

    /// DataFusion failed while executing the plan. Same redaction rule as
    /// [`SqlError::Plan`].
    #[error("SQL execution failed: {0}")]
    Execution(String),

    /// An invariant inside the pipeline was violated (schema mismatch,
    /// downcast failure). These are bugs, not input errors.
    #[error("internal ravel-sql error: {0}")]
    Internal(String),

    /// Reconstructed from a `DataFusionError::Shared` (checkpoint review
    /// finding, not in the original design): DataFusion wraps some errors
    /// in an `Arc` to hand the same error to multiple stream consumers, so
    /// the original `SqlError` cannot be moved out of it. `classify_shared`
    /// (crate::executor) captures this variant's `class()`/`client_message()`
    /// from the original error *before* it is behind the `Arc`, so a
    /// `TooManySamples` or `ResourcesExhausted` that happens to cross a
    /// `Shared` boundary still keeps its own class and text instead of
    /// collapsing into a generic execution failure. Never constructed
    /// outside `classify_shared`.
    #[error("{message}")]
    Shared { class: ErrorClass, message: String },
}

impl SqlError {
    /// The client-visible class, for HTTP status selection.
    pub fn class(&self) -> ErrorClass {
        match self {
            SqlError::Validation(_) | SqlError::CrossSignalQuery => ErrorClass::BadRequest,
            // An over-wide window refused before any LIST is a
            // resource-budget rejection, the same class as the segment/sample
            // budgets below: a well-formed request the server declines to
            // serve at this size (422), not a storage fault (503). Handled
            // ahead of the generic `Catalog(_)` arm so it does not collapse
            // into the transient-unavailable redaction.
            SqlError::Catalog(CatalogError::WindowTooWide { .. }) => ErrorClass::Unsupported,
            SqlError::Catalog(_)
            | SqlError::Fetch(_)
            | SqlError::LogFetch(_)
            | SqlError::SpanFetch(_)
            | SqlError::CorruptStreamAttrs(_)
            | SqlError::SnapshotInvalidated => ErrorClass::Unavailable,
            SqlError::DeadlineExceeded { .. } => ErrorClass::Timeout,
            SqlError::TooManySamples { .. }
            | SqlError::TooManySegments { .. }
            | SqlError::TooManySeries { .. }
            | SqlError::TooManyBytesScanned { .. }
            | SqlError::RequestBudgetExceeded { .. }
            | SqlError::ResourcesExhausted(_)
            | SqlError::Plan(_)
            | SqlError::Execution(_)
            | SqlError::Internal(_) => ErrorClass::Unsupported,
            SqlError::Shared { class, .. } => *class,
        }
    }

    /// The message a client may see. Storage-layer faults and DataFusion
    /// plan/execution errors collapse to fixed strings; validation and
    /// budget errors keep their own text, which is derived only from the
    /// caller's input or from counts and limits.
    ///
    /// The full `Display` of `self` stays available to the caller for
    /// server-side logging and is never produced here.
    pub fn client_message(&self) -> String {
        match self {
            SqlError::Validation(e) => e.to_string(),
            // Safe to echo: the text names only the two fixed table names.
            SqlError::CrossSignalQuery => self.to_string(),
            // Safe to echo: `WindowTooWide` carries only the estimate and the
            // limit (counts, no object key or tenant identity), and its text
            // tells the caller to narrow the window. Same
            // treatment as the budget errors below.
            SqlError::Catalog(catalog @ CatalogError::WindowTooWide { .. }) => catalog.to_string(),
            SqlError::Catalog(catalog) => redact_catalog(catalog).to_string(),
            SqlError::Fetch(fetch) => match fetch {
                FetchError::Corrupt { .. } => MSG_CORRUPT.to_string(),
                FetchError::Store { .. } | FetchError::EtagChanged { .. } => {
                    MSG_UNAVAILABLE.to_string()
                }
            },
            SqlError::LogFetch(fetch) => match fetch {
                LogFetchError::Corrupt { .. } => MSG_CORRUPT.to_string(),
                LogFetchError::Store { .. } => MSG_UNAVAILABLE.to_string(),
            },
            SqlError::SpanFetch(fetch) => match fetch {
                // A cross-tenant object is an integrity violation of the fetched
                // segment relative to the request, redacted like corruption
                // (the mismatching tenant identity never reaches the client).
                SpanFetchError::Corrupt { .. } | SpanFetchError::TenantMismatch { .. } => {
                    MSG_CORRUPT.to_string()
                }
                SpanFetchError::Store { .. } => MSG_UNAVAILABLE.to_string(),
            },
            SqlError::CorruptStreamAttrs(_) => MSG_CORRUPT.to_string(),
            SqlError::SnapshotInvalidated => MSG_UNAVAILABLE.to_string(),
            SqlError::DeadlineExceeded { .. }
            | SqlError::TooManySamples { .. }
            | SqlError::TooManySegments { .. }
            | SqlError::TooManySeries { .. }
            | SqlError::TooManyBytesScanned { .. }
            | SqlError::RequestBudgetExceeded { .. }
            | SqlError::ResourcesExhausted(_) => self.to_string(),
            SqlError::Plan(_) => MSG_PLAN.to_string(),
            SqlError::Execution(_) => MSG_EXECUTION.to_string(),
            SqlError::Internal(_) => MSG_INTERNAL.to_string(),
            SqlError::Shared { message, .. } => message.clone(),
        }
    }

    /// True when this error is a store `NotFound` on a pinned segment: the
    /// one condition that arms the re-resolve-and-retry contract
    /// (crate::executor).
    pub(crate) fn is_segment_not_found(&self) -> bool {
        matches!(
            self,
            SqlError::Fetch(FetchError::Store {
                source: ravel_object_store::StoreError::NotFound,
                ..
            }) | SqlError::LogFetch(LogFetchError::Store {
                source: ravel_object_store::StoreError::NotFound,
                ..
            }) | SqlError::SpanFetch(SpanFetchError::Store {
                source: ravel_object_store::StoreError::NotFound,
                ..
            })
        )
    }
}

/// Class-specific redaction for catalog errors, mirroring
/// `redacted_storage_message` on the PromQL path so both endpoints answer
/// the same way for the same fault.
fn redact_catalog(err: &CatalogError) -> &'static str {
    match err {
        CatalogError::UnsatisfiableToken { .. } => MSG_UNSATISFIABLE,
        CatalogError::Reconstruction { .. }
        | CatalogError::FieldMismatch { .. }
        | CatalogError::Record(_)
        | CatalogError::Key(_) => MSG_CORRUPT,
        // Store errors and any future variant redact to the transient
        // message rather than risk leaking backend text.
        _ => MSG_UNAVAILABLE,
    }
}

impl From<SqlError> for DataFusionError {
    fn from(err: SqlError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::StoreError;

    use super::*;

    /// A representative internal object key: tenant-hashed prefix, signal,
    /// level, shard, writer id, and the `.rseg` suffix (ADR-0010 layout).
    const LEAKY_KEY: &str = "t/deadbeefcafef00d/metrics/l0/0/writer-7.1.2.0123456789abcdef.rseg";
    const TENANT_HASH: &str = "deadbeefcafef00d";
    const RAW_STORE_TEXT: &str = "bucket=prod-telemetry endpoint=s3.internal request-id=abc";

    /// Same assertion set the PromQL boundary's tests use.
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

    #[test]
    fn fetch_store_error_is_redacted_but_detail_survives_for_the_log() {
        let err = SqlError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Transient(RAW_STORE_TEXT.to_string()),
        });
        let detail = err.to_string();
        assert!(detail.contains(LEAKY_KEY), "log detail lost the key");
        assert!(
            detail.contains(RAW_STORE_TEXT),
            "log detail lost store text"
        );

        assert_eq!(err.client_message(), MSG_UNAVAILABLE);
        assert_redacted(&err.client_message());
        assert_eq!(err.class(), ErrorClass::Unavailable);
    }

    #[test]
    fn catalog_field_mismatch_redacts_the_key_as_corrupt() {
        let err = SqlError::Catalog(CatalogError::FieldMismatch {
            key: LEAKY_KEY.to_string(),
            field: "tenant_hash",
            expected: "aaaa".to_string(),
            actual: TENANT_HASH.to_string(),
        });
        assert!(err.to_string().contains(LEAKY_KEY));
        assert_eq!(err.client_message(), MSG_CORRUPT);
        assert_redacted(&err.client_message());
    }

    #[test]
    fn catalog_store_error_redacts_to_unavailable() {
        let err = SqlError::Catalog(CatalogError::Store(StoreError::Permanent(
            RAW_STORE_TEXT.to_string(),
        )));
        assert!(err.to_string().contains(RAW_STORE_TEXT));
        assert_eq!(err.client_message(), MSG_UNAVAILABLE);
        assert_redacted(&err.client_message());
    }

    #[test]
    fn unsatisfiable_token_is_a_distinct_stable_class() {
        let err = SqlError::Catalog(CatalogError::UnsatisfiableToken {
            shard: 0,
            writer_id: "writer-7".to_string(),
            epoch: 1,
            seq: 2,
            ingest_hour_bucket: 3,
        });
        assert_eq!(err.client_message(), MSG_UNSATISFIABLE);
        assert_ne!(MSG_UNSATISFIABLE, MSG_UNAVAILABLE);
        assert_ne!(MSG_UNSATISFIABLE, MSG_CORRUPT);
    }

    /// The SQL-specific half of the boundary: a DataFusion error whose text
    /// embeds a wrapped object key must not reach the client either.
    #[test]
    fn datafusion_plan_and_execution_errors_are_not_echoed() {
        let leaky = format!(
            "Schema error: No field named samples.nope. Valid fields: ts, value. \
             Underlying: object store get failed for {LEAKY_KEY}: {RAW_STORE_TEXT}"
        );
        let plan = SqlError::Plan(leaky.clone());
        assert!(plan.to_string().contains(LEAKY_KEY), "log detail lost");
        assert_eq!(plan.client_message(), MSG_PLAN);
        assert_redacted(&plan.client_message());

        let exec = SqlError::Execution(leaky);
        assert_eq!(exec.client_message(), MSG_EXECUTION);
        assert_redacted(&exec.client_message());
        // plan and execution stay distinguishable to the caller.
        assert_ne!(MSG_PLAN, MSG_EXECUTION);
    }

    #[test]
    fn corrupt_stream_attrs_shares_the_fetcher_corruption_class_and_message() {
        // A malformed stream_attrs blob detected during re-verification must
        // surface as the same client class/message as the fetcher's own
        // LogFetchError::Corrupt path -- one corruption class, not two.
        let reverify = SqlError::CorruptStreamAttrs("stream_attrs truncated".to_string());
        assert_eq!(reverify.client_message(), MSG_CORRUPT);
        assert_eq!(reverify.class(), ErrorClass::Unavailable);

        let fetcher = SqlError::LogFetch(LogFetchError::Corrupt {
            key: LEAKY_KEY.to_string(),
            source: ravel_logseg::LogSegError::Corrupted("bad footer".into()),
        });
        assert_eq!(reverify.client_message(), fetcher.client_message());
        assert_eq!(reverify.class(), fetcher.class());
        // The detail string stays available server-side and leaks nothing.
        assert!(reverify.to_string().contains("stream_attrs truncated"));
        assert_redacted(&reverify.client_message());
    }

    #[test]
    fn internal_errors_are_not_echoed() {
        let err = SqlError::Internal(format!("downcast failed while reading {LEAKY_KEY}"));
        assert_eq!(err.client_message(), MSG_INTERNAL);
        assert_redacted(&err.client_message());
    }

    #[test]
    fn safe_errors_keep_their_own_text() {
        // Budget errors carry only counts and limits.
        let budget = SqlError::TooManySamples { count: 11, max: 10 };
        assert_eq!(budget.client_message(), budget.to_string());
        assert_eq!(budget.class(), ErrorClass::Unsupported);

        // The S3 request budget is the same shape:
        // counts and limits only, echoed verbatim, HTTP 422.
        let request_budget = SqlError::RequestBudgetExceeded {
            requests: 30_001,
            max: 30_000,
        };
        assert_eq!(request_budget.client_message(), request_budget.to_string());
        assert_eq!(request_budget.class(), ErrorClass::Unsupported);
        assert!(request_budget.client_message().contains("30001"));
        assert!(request_budget.client_message().contains("30000"));
        assert_redacted(&request_budget.client_message());

        // Validation errors quote only the caller's own input.
        let bad = SqlError::Validation(ValidationError::NotReadOnly { kind: "INSERT" });
        assert_eq!(bad.client_message(), bad.to_string());
        assert_eq!(bad.class(), ErrorClass::BadRequest);
    }

    #[test]
    fn cross_signal_query_is_a_bad_request_that_keeps_its_own_text() {
        let err = SqlError::CrossSignalQuery;
        assert_eq!(err.class(), ErrorClass::BadRequest);
        // Its own text is returned verbatim and names both tables.
        assert_eq!(err.client_message(), err.to_string());
        assert!(err.client_message().contains("samples"));
        assert!(err.client_message().contains("logs"));
        // It carries no server state to redact.
        assert_redacted(&err.client_message());
    }

    #[test]
    fn window_too_wide_is_a_422_that_keeps_its_counts() {
        // An over-wide window refused before any LIST is a
        // resource-budget rejection (422 Unsupported), not a storage fault
        // (503). Its text carries only the estimate and the limit, so it is
        // echoed to the client verbatim like the other budget errors, and it
        // is redaction-safe (no key, no tenant hash).
        let err = SqlError::Catalog(CatalogError::WindowTooWide {
            estimate: 496_089,
            limit: 100_000,
        });
        assert_eq!(err.class(), ErrorClass::Unsupported);
        // The client sees the inner catalog message (the counts and the
        // "narrow the window" guidance), without the "snapshot resolution
        // failed:" wrapper the server logs.
        assert!(err.client_message().contains("496089"));
        assert!(err.client_message().contains("100000"));
        assert!(err.client_message().contains("narrow"));
        assert_redacted(&err.client_message());
        // Distinct from the transient/corrupt catalog classes.
        assert_ne!(err.client_message(), MSG_UNAVAILABLE);
        assert_ne!(err.client_message(), MSG_CORRUPT);
    }

    #[test]
    fn snapshot_invalidated_is_a_redacted_unavailable() {
        let err = SqlError::SnapshotInvalidated;
        assert_eq!(err.client_message(), MSG_UNAVAILABLE);
        assert_eq!(err.class(), ErrorClass::Unavailable);
    }

    #[test]
    fn segment_not_found_is_the_only_retry_trigger() {
        let not_found = SqlError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::NotFound,
        });
        assert!(not_found.is_segment_not_found());

        let transient = SqlError::Fetch(FetchError::Store {
            key: LEAKY_KEY.to_string(),
            source: StoreError::Transient("flaky".to_string()),
        });
        assert!(!transient.is_segment_not_found());
        assert!(!SqlError::SnapshotInvalidated.is_segment_not_found());
    }
}
