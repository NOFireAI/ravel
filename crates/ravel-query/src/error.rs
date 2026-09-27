//! Typed errors for snapshot resolution, segment fetch, and query
//! evaluation (docs/query-engine.md). Every fallible path here is a typed
//! error, never a silent partial result.

use std::fmt;
use std::time::Duration;

use ravel_catalog::CatalogError;

use crate::fetcher::FetchError;

/// The per-signal catalog fold-liveness gauge `ravel-server` renders
/// (`services/ravel-server/src/metrics.rs`), and the series the shipped
/// `RavelCatalogFoldStalled` alert reads (`deploy/prometheus/ravel.rules.yaml`).
/// A request-budget refusal caused by fold lag names it so an operator goes to
/// the fold rather than to the budget (ADR-1306 decision 6).
pub const FOLD_LAST_SUCCESS_GAUGE: &str = "ravel_catalog_fold_last_success_timestamp_seconds";

/// What the resolve behind a query's snapshot saw of the catalog's unsealed
/// tail, carried into a request-budget refusal (ADR-1306 decision 6).
///
/// The tail is the span from the newest sealed ingest hour to query time. A
/// fold that is keeping up holds it under
/// [`crate::config::fold_lag_tail_threshold`]: `healthy_tail_max` of the
/// catalog's seal margin, plus the fold interval it waits between cycles, plus
/// the HEAD cache TTL the resolve reads the watermark through. Past that the
/// fold is behind, the tail (not the query) is what made the query expensive,
/// and the refusal says so. A resolve that listed no unsealed data above a
/// watermark it actually read is [`FoldLag::Healthy`]: there is no tail it can
/// attribute to the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FoldLag {
    /// The resolved tail was at or under the fold-lag threshold, or the
    /// resolve produced no tail it could attribute to the fold. The refusal
    /// keeps its pre-ADR-1306 wording.
    #[default]
    Healthy,
    /// The resolved tail was longer than a fold that is keeping up can leave.
    Lagging {
        unsealed_tail: Duration,
        /// The [`crate::config::fold_lag_tail_threshold`] the tail passed.
        fold_lag_threshold: Duration,
    },
}

impl FoldLag {
    /// Classifies a resolve's unsealed tail against the engine's
    /// [`crate::config::fold_lag_tail_threshold`]. `None` (no tail this
    /// resolve can attribute to the fold) and a tail at or inside the
    /// threshold are both [`FoldLag::Healthy`].
    #[must_use]
    pub fn from_resolved_tail(
        unsealed_tail: Option<Duration>,
        fold_lag_threshold: Duration,
    ) -> FoldLag {
        match unsealed_tail {
            Some(tail) if tail > fold_lag_threshold => FoldLag::Lagging {
                unsealed_tail: tail,
                fold_lag_threshold,
            },
            _ => FoldLag::Healthy,
        }
    }

    /// True when the resolved tail exceeded the fold-lag threshold.
    #[must_use]
    pub fn is_lagging(&self) -> bool {
        matches!(self, FoldLag::Lagging { .. })
    }
}

impl fmt::Display for FoldLag {
    /// Renders as the empty string when the tail is healthy, so a refusal that
    /// fold lag did not cause keeps exactly the message it had before
    /// ADR-1306. Seconds, not a `Duration` debug form: the gauge an operator
    /// compares it against is in seconds too.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FoldLag::Healthy => Ok(()),
            FoldLag::Lagging {
                unsealed_tail,
                fold_lag_threshold,
            } => write!(
                f,
                "; the catalog's unsealed tail is {} s, longer than the {} s a catalog whose \
                 fold is keeping up can show, so the fold is behind and the tail is what the \
                 budget was spent on: check {FOLD_LAST_SUCCESS_GAUGE} before raising the budget",
                unsealed_tail.as_secs(),
                fold_lag_threshold.as_secs()
            ),
        }
    }
}

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
    /// The per-query object-store request budget was exhausted. `fold_lag`
    /// carries what the resolve saw of the catalog's unsealed tail (ADR-1306
    /// decision 6): a tail longer than a fold that is keeping up can leave
    /// appends the tail length and the fold-liveness gauge to the message, so
    /// an operator reads the refusal as fold lag rather than as a budget that
    /// is too small. Any other verdict renders nothing and keeps the
    /// pre-ADR-1306 message.
    #[error("query issued {requests} S3 requests, exceeding the budget of {max}{fold_lag}")]
    RequestBudgetExceeded {
        requests: u64,
        max: u64,
        fold_lag: FoldLag,
    },
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
