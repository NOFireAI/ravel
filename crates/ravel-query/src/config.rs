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

/// [`crate::QueryEngine`] resource limits and concurrency. Every limit is
/// enforced as a typed error (docs/query-engine.md "never silent partial
/// results"), never a truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    pub max_segments: usize,
    pub max_series: usize,
    pub max_samples: usize,
    pub deadline: Duration,
    pub fetch_concurrency: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: DEFAULT_MAX_SEGMENTS,
            max_series: DEFAULT_MAX_SERIES,
            max_samples: DEFAULT_MAX_SAMPLES,
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
        }
    }
}
