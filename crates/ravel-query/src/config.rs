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
/// Default cap on total S3 requests a single query may issue (ADR-0073
/// decision 3). Admits the worst legitimate open hour (~7,200 GETs per
/// shard-hour plus resolve and sealed fetch) with headroom, while bounding a
/// runaway query to a knowable spend.
pub const DEFAULT_MAX_S3_REQUESTS: u64 = 25_000;

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

/// A per-tenant cap on the total S3 requests a single query may issue, or an
/// explicit opt-in to no cap at all (ADR-0073 decision 3). Mirrors
/// [`ByteLimit`]'s shape: the recent-hour exemption from `max_segments`
/// (ADR-0073 decision 2) needs a governor that is not a count check, and this
/// is that governor, enforced the same incremental way the bytes-scanned
/// budget already is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestLimit {
    Bounded(u64),
    Unlimited,
}

impl RequestLimit {
    /// True when `requests` has passed a bounded cap. `Unlimited` never
    /// trips.
    pub fn is_exceeded_by(self, requests: u64) -> bool {
        match self {
            RequestLimit::Bounded(max) => requests > max,
            RequestLimit::Unlimited => false,
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
    /// Per-tenant cap on total S3 bytes a single query may scan, checked once
    /// per completed segment fetch inside the engine's two fetch fan-outs
    /// (`fetch_all_series` and `fetch_all_samples_and_histograms`), the stage
    /// that owns segment concurrency, so a tripped budget cancels the
    /// remaining in-flight fetches mid-scan (ADR-0061 decision 1). Defaults to
    /// [`ByteLimit::Unlimited`]: a bounded default would silently start
    /// rejecting existing deployments' large-but-legitimate queries on
    /// upgrade with no config change, so opting in is explicit.
    pub max_bytes_scanned: ByteLimit,
    /// Per-tenant cap on total S3 requests a single query may issue, checked
    /// at the same incremental points as `max_bytes_scanned` (ADR-0073
    /// decision 3). Governs the cost of segments exempted from
    /// `max_segments` by decision 2 (recent and token-resolved); defaults to
    /// [`RequestLimit::Bounded(DEFAULT_MAX_S3_REQUESTS)`].
    pub max_s3_requests: RequestLimit,
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
            max_s3_requests: RequestLimit::Bounded(DEFAULT_MAX_S3_REQUESTS),
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            default_evaluation_interval: DEFAULT_EVALUATION_INTERVAL,
        }
    }
}
