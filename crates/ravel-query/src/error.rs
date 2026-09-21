//! Typed errors for snapshot resolution, segment fetch, and query
//! evaluation (docs/query-engine.md). Every fallible path here is a typed
//! error, never a silent partial result.

use std::time::Duration;

use ravel_catalog::CatalogError;

use crate::fetcher::FetchError;

/// Errors from resolving and evaluating one query (instant, range, or a
/// labels/series lookup).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueryError {
    #[error("promql parse error: {0}")]
    Parse(String),
    #[error("unsupported PromQL construct: {construct}")]
    Unsupported { construct: String },
    #[error("step must be positive, got {step_ms} ms")]
    NonPositiveStep { step_ms: i64 },
    #[error("range start {start_ms} ms is after end {end_ms} ms")]
    InvalidRange { start_ms: i64, end_ms: i64 },
    #[error("time value is out of representable range")]
    TimeOverflow,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Eval(#[from] ravel_promql::Error),
    #[error("query matched {count} segments, exceeding the limit of {max}")]
    TooManySegments { count: usize, max: usize },
    #[error("query matched {count} series, exceeding the limit of {max}")]
    TooManySeries { count: usize, max: usize },
    #[error("query matched {count} samples, exceeding the limit of {max}")]
    TooManySamples { count: usize, max: usize },
    #[error("query scanned {scanned} bytes, exceeding the budget of {max}")]
    TooManyBytesScanned { scanned: u64, max: u64 },
    /// A remote streamed more response frames for one slice than the
    /// coordinator accepts (issue #1687 part B,
    /// `distrib::codec::MAX_SLICE_RESPONSE_FRAMES`). A budget refusal like the
    /// caps above, not an outage: the counts are the coordinator's own and
    /// carry no server state, so they are echoed to the caller.
    #[error("slice returned {frames} response frames, exceeding the limit of {max}")]
    TooManySliceFrames { frames: usize, max: usize },
    /// A remote streamed more response frame bytes for one slice than the
    /// coordinator accepts (issue #1687 part B,
    /// `distrib::codec::MAX_SLICE_RESPONSE_BYTES`). Distinct from
    /// [`QueryError::TooManyBytesScanned`], which counts the store bytes a
    /// query read: this counts the protobuf-encoded size of one slice's
    /// response frames, a figure that appears in neither the query's accounting
    /// nor `stats.fragments[].bytesReported`, so the message names the quantity
    /// rather than leaving an operator to reconcile it against store bytes. The
    /// two limits are independent for the same reason: `max_bytes_scanned` does
    /// not raise or lower this cap. A
    /// budget refusal like the caps above; both figures are the coordinator's
    /// own and carry no server state, so they are echoed to the caller.
    #[error(
        "slice returned {bytes} response frame wire bytes, exceeding the per-slice limit of {max}"
    )]
    TooManySliceBytes { bytes: u64, max: u64 },
    #[error("query issued {requests} S3 requests, exceeding the budget of {max}")]
    RequestBudgetExceeded { requests: u64, max: u64 },
    #[error("query exceeded its deadline of {deadline:?}")]
    DeadlineExceeded { deadline: Duration },
    #[error("snapshot invalidated by a concurrent GC/compaction; retry also failed")]
    SnapshotInvalidated,
    #[error("distributed slice fetch failed: {reason}")]
    Distrib { reason: String },
    #[error("federated fetch from remote cluster {cluster:?} failed: {reason}")]
    Federation { cluster: String, reason: String },
    #[error("decoded segment run is not ascending by timestamp: {prev} was followed by {next}")]
    NonMonotonicSamples { prev: i64, next: i64 },
    #[error(
        "run carries {priorities} per-sample dedup priorities but {samples} samples; the column must be parallel to the samples"
    )]
    PrioritySampleCountMismatch { priorities: usize, samples: usize },
    #[error(
        "aggregation pushdown collected series {series_id} from more than one slice; the ADR-0103 eligibility gate guarantees each series lives on exactly one worker, so a repeat means the gate was violated and the query must fail closed rather than keep one of two partials"
    )]
    DuplicatePushdownSeries { series_id: String },
}
