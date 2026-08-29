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
/// Numerator/denominator of the headroom factor applied to the per-shard
/// open-hour segment count when deriving the S3 request budget: 3/2, i.e. 50%
/// above the one-GET-per-recent-segment floor. That floor is the true cost of
/// a cold query over a busy shard's open hour; the extra half covers
/// per-segment fetch retries and the occasional cold-fetch footer read,
/// without widening the cap so far that it stops bounding a runaway query.
pub const REQUEST_BUDGET_HEADROOM_NUM: u64 = 3;
pub const REQUEST_BUDGET_HEADROOM_DEN: u64 = 2;

/// Shard-independent slack in the derived S3 request budget: resolve (catalog
/// manifest, fold, and token reads) plus the sealed-segment fetch tail, whose
/// count is bounded by `max_segments` and the catalog rather than by shard
/// count. Added once, not per shard.
pub const REQUEST_BUDGET_FIXED_OVERHEAD: u64 = 5_000;

/// Reference inputs for [`EngineConfig::default`]'s S3 request budget. The
/// running server does NOT use these: it derives the budget from its actual
/// `--shards` and ingest flush cadence (see [`derive_max_s3_requests`], wired
/// through `ravel-server`'s config path). They exist only so an `EngineConfig`
/// built with no deployment context (tests, alerting, other non-server
/// callers) still gets a sane multi-shard budget instead of a single-shard
/// one. ravel-ingest depends on ravel-query, so this crate cannot import
/// `IngestConfig`'s defaults to share them; ravel-server's reachability test
/// pins that the server threads the real values rather than these references.
pub const DEFAULT_BUDGET_REFERENCE_SHARDS: u32 = 4;
pub const DEFAULT_BUDGET_REFERENCE_FLUSH_DELAY: Duration = Duration::from_millis(500);

/// Derives the per-query S3 request budget from a deployment's shard count and
/// ingest flush cadence (ADR-0075 decisions 1 and 2):
///
/// ```text
/// budget = per_shard_allowance * shard_count + REQUEST_BUDGET_FIXED_OVERHEAD
/// per_shard_allowance = ceil(3600s / max_flush_delay) * NUM / DEN
/// ```
///
/// The request budget's cost is per shard-hour, not per query. A busy tenant
/// seals `ceil(3600s / max_flush_delay)` segments per shard per open hour
/// (7,200 at the 500ms default cadence), and a cold query over that hour GETs
/// each one, on every shard. The old flat 25,000 cap was correct only at 3
/// shards or fewer; at the default 4 it rejected the worst legitimate open
/// hour (4 x 7,200 = 28,800) before it could answer. Deriving from
/// `max_flush_delay` rather than hardcoding 7,200 means a deployment that
/// raises the flush delay (a supported cost lever) gets a correct cap with no
/// hand recomputation.
pub fn derive_max_s3_requests(shard_count: u32, max_flush_delay: Duration) -> u64 {
    // Guard a zero/absurd cadence: a zero delay is not a real configuration,
    // but dividing by it would panic. Clamp to one shard for the same reason a
    // zero-shard deployment is nonsensical.
    let flush_ms = max_flush_delay.as_millis().max(1);
    // ceil(3_600_000ms / flush_ms): the most segments one shard can seal in an
    // open hour, which is the most GETs a cold query pays for that shard.
    let segments_per_shard_hour = 3_600_000u128.div_ceil(flush_ms) as u64;
    let per_shard_allowance = segments_per_shard_hour.saturating_mul(REQUEST_BUDGET_HEADROOM_NUM)
        / REQUEST_BUDGET_HEADROOM_DEN;
    per_shard_allowance
        .saturating_mul(u64::from(shard_count.max(1)))
        .saturating_add(REQUEST_BUDGET_FIXED_OVERHEAD)
}

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
    /// decision 3). Governs the cost of segments exempted from `max_segments`
    /// by decision 2 (recent and token-resolved). The running server sets this
    /// to a value DERIVED from its shard count and flush cadence
    /// (ADR-0075, [`derive_max_s3_requests`]); [`EngineConfig::default`] uses
    /// the derivation at [`DEFAULT_BUDGET_REFERENCE_SHARDS`] shards as a
    /// no-deployment-context fallback.
    pub max_s3_requests: RequestLimit,
    pub deadline: Duration,
    pub fetch_concurrency: usize,
    /// Step for a subquery that does not specify its own (`expr[5m:]`).
    pub default_evaluation_interval: Duration,
    /// Object size above which a logs scan reads only the pruning-relevant
    /// blocks of an RLOG object instead of the whole object (ADR-0107), i.e.
    /// [`crate::LogSegmentFetcher::with_block_range_threshold`]. Not an engine
    /// limit like the fields above: it rides here because this is the one config
    /// the server folds its query flags into and hands to every fetcher it
    /// constructs (`services/ravel-server/src/query.rs`'s `build_sql_state`),
    /// which is where the logs fetcher is built.
    ///
    /// Defaults to [`crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`] (512 KiB).
    /// `u64::MAX` reads every object whole, the pre-ADR-0107 behavior and the
    /// mitigation for an operator who hits a regression on the block-range path;
    /// `0` sends every object through it.
    pub logs_block_range_threshold: u64,
    /// Cost of one object-store round trip, denominated in transfer bytes: a
    /// saved request is worth this many saved bytes (ADR-0904 decision 1). Like
    /// `logs_block_range_threshold` this is not an engine limit; it rides here
    /// because this is the config the server folds its query flags into and
    /// hands to every fetcher it builds.
    ///
    /// A property of the store and the instance (round-trip latency and
    /// single-stream bandwidth at the fetch concurrency in use), not of the
    /// RLOG format, which is why it is configurable rather than frozen. One
    /// value drives three derived decisions in the logs fetch layer, so
    /// recalibrating the store recalibrates all of them at once: the coalescing
    /// gap, the pre-probe whole-object crossover, and the projection routing of
    /// the whole-segment fast path (#887).
    ///
    /// Defaults to [`crate::DEFAULT_LOG_REQUEST_COST_BYTES`], whose doc comment
    /// carries the derivation. Raising it above the largest object a deployment
    /// writes collapses all three decisions to whole-object reads.
    pub logs_request_cost_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: DEFAULT_MAX_SEGMENTS,
            max_series: DEFAULT_MAX_SERIES,
            max_samples: DEFAULT_MAX_SAMPLES,
            max_bytes_scanned: ByteLimit::Unlimited,
            max_s3_requests: RequestLimit::Bounded(derive_max_s3_requests(
                DEFAULT_BUDGET_REFERENCE_SHARDS,
                DEFAULT_BUDGET_REFERENCE_FLUSH_DELAY,
            )),
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            default_evaluation_interval: DEFAULT_EVALUATION_INTERVAL,
            logs_block_range_threshold: crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            logs_request_cost_bytes: crate::DEFAULT_LOG_REQUEST_COST_BYTES,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_request_cost_is_the_compiled_constant() {
        assert_eq!(
            EngineConfig::default().logs_request_cost_bytes,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES
        );
        // The constant itself, not merely "nonzero": the knob ships with the
        // measured q20 latency break-even and no behavior change (ADR-0904
        // decision 5).
        assert_eq!(crate::DEFAULT_LOG_REQUEST_COST_BYTES, 1_887_437);
    }
}
