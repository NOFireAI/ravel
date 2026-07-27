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
    #[error("query exceeded its deadline of {deadline:?}")]
    DeadlineExceeded { deadline: Duration },
    #[error("snapshot invalidated by a concurrent GC/compaction; retry also failed")]
    SnapshotInvalidated,
    #[error("decoded segment run is not ascending by timestamp: {prev} was followed by {next}")]
    NonMonotonicSamples { prev: i64, next: i64 },
}
