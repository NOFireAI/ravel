//! Engine-wide tunables (docs/query-engine.md "Budgets").

use std::time::Duration;

/// Default cap on segments a single query may fan out over.
pub const DEFAULT_MAX_SEGMENTS: usize = 1024;
/// Default cap on distinct series a single query may materialize.
pub const DEFAULT_MAX_SERIES: usize = 10_000;
/// Default cap on total samples (summed across series, after cross-segment
/// dedup) a single query may materialize.
pub const DEFAULT_MAX_SAMPLES: usize = 10_000_000;
/// Default wall-clock deadline for a single query.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);
/// Default bound on concurrent in-flight segment fetches per query.
pub const DEFAULT_FETCH_CONCURRENCY: usize = 8;
/// Default step for a subquery that omits its own (`expr[5m:]`), matching
/// Prometheus' global `evaluation_interval` default.
pub const DEFAULT_EVALUATION_INTERVAL: Duration = Duration::from_secs(60);

/// A per-tenant cap on the total S3 bytes a single query may scan, or an
/// explicit opt-in to no cap at all (ADR-0061 decision 1).
///
/// Mirrors `ravel_ingest::admission::CountLimit`'s shape deliberately: this
/// is the same enum operators already learned for ingest admission limits,
/// applied to a query-side resource. Enforcement is a typed error, never a
/// truncation; `Unlimited` is the explicit, config-review-visible way to opt
/// out of the cap rather than a silent absence of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteLimit {
    Bounded(u64),
    Unlimited,
}

impl ByteLimit {
    /// True when `bytes_scanned` has passed a bounded cap. `Unlimited` never
    /// trips, so a caller that does not opt in behaves exactly as before this
    /// limit existed.
    pub fn is_exceeded_by(self, bytes_scanned: u64) -> bool {
        match self {
            ByteLimit::Bounded(max) => bytes_scanned > max,
            ByteLimit::Unlimited => false,
        }
    }
}

/// [`crate::QueryEngine`] resource limits and concurrency. Every limit is
/// enforced as a typed error (docs/query-engine.md "never silent partial
/// results"), never a truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    pub max_segments: usize,
    pub max_series: usize,
    pub max_samples: usize,
    /// Per-tenant cap on total S3 bytes a single query may scan, checked
    /// incrementally during fetch (ADR-0061 decision 1). Defaults to
    /// [`ByteLimit::Unlimited`]: a bounded default would silently start
    /// rejecting existing deployments' large-but-legitimate queries on
    /// upgrade with no config change, so opting in is explicit.
    pub max_bytes_scanned: ByteLimit,
    pub deadline: Duration,
    pub fetch_concurrency: usize,
    /// Step for a subquery that does not specify its own (`expr[5m:]`).
    pub default_evaluation_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: DEFAULT_MAX_SEGMENTS,
            max_series: DEFAULT_MAX_SERIES,
            max_samples: DEFAULT_MAX_SAMPLES,
            max_bytes_scanned: ByteLimit::Unlimited,
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            default_evaluation_interval: DEFAULT_EVALUATION_INTERVAL,
        }
    }
}
