//! Errors surfaced by the ravel-sql pipeline.
//!
//! Every fetch/decode failure is a hard, typed error carried across the
//! DataFusion boundary as `DataFusionError::External` so no operator ever
//! observes partial or silently-wrong data (docs/consistency-model.md
//! "never silent partial results").

use datafusion::error::DataFusionError;
use ravel_query::FetchError;

/// A ravel-sql execution error.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// A segment fetch or decode failed mid-scan.
    #[error("segment fetch failed: {0}")]
    Fetch(#[from] FetchError),

    /// The post-dedup row count exceeded the configured `max_samples`
    /// budget (docs/query-engine.md "Budgets").
    #[error("query materialized too many samples: {count} exceeds max {max}")]
    TooManySamples { count: usize, max: usize },

    /// The resolved snapshot has more segments than `max_segments`.
    #[error("query fans out over too many segments: {count} exceeds max {max}")]
    TooManySegments { count: usize, max: usize },

    /// An invariant inside the pipeline was violated (schema mismatch,
    /// downcast failure). These are bugs, not input errors.
    #[error("internal ravel-sql error: {0}")]
    Internal(String),
}

impl From<SqlError> for DataFusionError {
    fn from(err: SqlError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}
