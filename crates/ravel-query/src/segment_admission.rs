//! Unified snapshot admission seam (ADR-0073 decision 4): the sealed-set
//! count check and the request-spend budget check, one call per resolve
//! replacing the eight divergent per-surface checks the ADR describes. This
//! crate's PromQL engine is the only site wired up here
//! (`crates/ravel-query/src/engine.rs`'s `resolve_bounded`); the SQL
//! executor, the five SQL providers, and the exemplars state move onto this
//! seam.

use ravel_catalog::{SegmentOrigins, Snapshot};

use crate::config::EngineConfig;
use crate::error::QueryError;

/// The admitted view of a resolved snapshot: the sealed-set count that was
/// checked against `max_segments` and the request budget recent/
/// token-resolved segments spend against (ADR-0073 decisions 2 and 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentAdmission {
    pub sealed_count: u64,
    pub exempt_count: u64,
}

/// Checks a resolved snapshot against `max_segments`, applied to the sealed,
/// below-watermark set only (ADR-0073 decision 2): recent and token-resolved
/// segments never count. Their cost is bounded separately, by the per-query
/// S3 request budget enforced incrementally during fetch (decision 3), not
/// here.
pub fn admit(
    snapshot: &Snapshot,
    origins: &SegmentOrigins,
    config: &EngineConfig,
) -> Result<SegmentAdmission, QueryError> {
    debug_assert_eq!(
        origins.origins.len(),
        snapshot.segments.len(),
        "SegmentOrigins must be parallel to Snapshot::segments"
    );
    let sealed_count = origins.sealed_count;
    if sealed_count as usize > config.max_segments {
        return Err(QueryError::TooManySegments {
            count: sealed_count as usize,
            max: config.max_segments,
        });
    }
    Ok(SegmentAdmission {
        sealed_count,
        exempt_count: origins.exempt_count,
    })
}

/// True when `requests` has passed `max_s3_requests`, mirroring
/// `bytes_scanned_exceeded`'s incremental-comparison shape (ADR-0073
/// decision 3): a typed error, checked at the same points the bytes-scanned
/// budget already checks, never a truncation.
pub fn request_budget_exceeded(
    requests: u64,
    max_s3_requests: crate::config::RequestLimit,
) -> Option<QueryError> {
    use crate::config::RequestLimit;
    match max_s3_requests {
        RequestLimit::Bounded(max) if requests > max => {
            Some(QueryError::RequestBudgetExceeded { requests, max })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::request_budget_exceeded;
    use crate::config::{RequestLimit, derive_max_s3_requests};

    /// The shard count a `ravel-server` process runs with by default
    /// (`services/ravel-server`'s `--shards`, `default_value_t = 4`). Named
    /// here, not imported: ravel-query cannot depend on ravel-server. The
    /// ravel-server reachability test pins that the real startup path derives
    /// the budget at this same value.
    const DEFAULT_SHARDS: u32 = 4;
    /// The ingest pipeline's default `max_flush_delay`
    /// (`ravel_ingest::IngestConfig::default`, the value the server's metrics
    /// ingest pipeline actually runs with). Named, not imported, for the same
    /// reason `DEFAULT_SHARDS` is: ravel-ingest depends on ravel-query, so
    /// importing it here would be a dependency cycle.
    const DEFAULT_FLUSH_DELAY: Duration = Duration::from_millis(500);

    /// The flat cap this derivation replaces. Its defect: it is a per-query
    /// TOTAL, but the cost is per shard-hour, so it under-budgets any
    /// deployment above 3 shards.
    const OLD_FLAT_CAP: u64 = 25_000;

    #[test]
    fn open_hour_at_default_shards_fits_the_derived_budget() {
        let budget = derive_max_s3_requests(DEFAULT_SHARDS, DEFAULT_FLUSH_DELAY);
        let limit = RequestLimit::Bounded(budget);

        // Half 1: a cold query over one tenant's open hour at the default shard
        // count must be admitted. A busy tenant seals 3600s / 500ms = 7,200
        // segments per shard per open hour, and a cold query GETs each one on
        // every shard: 4 x 7,200 = 28,800 requests. This is the exact cost the
        // old flat 25,000 cap rejected at 4 shards.
        let segments_per_shard_hour = 3_600_000u64 / 500;
        assert_eq!(segments_per_shard_hour, 7_200);
        let open_hour_cost = segments_per_shard_hour * u64::from(DEFAULT_SHARDS);
        assert_eq!(open_hour_cost, 28_800);
        assert!(
            request_budget_exceeded(open_hour_cost, limit).is_none(),
            "the worst legitimate open hour ({open_hour_cost} GETs) must fit the derived \
             budget ({budget})"
        );

        // Non-vacuity guard: the flat cap the derivation replaces MUST reject
        // this same cost. Without this the "fits" assertion above could pass
        // against any large-enough constant and prove nothing about the fix.
        assert!(
            request_budget_exceeded(open_hour_cost, RequestLimit::Bounded(OLD_FLAT_CAP)).is_some(),
            "the old flat {OLD_FLAT_CAP} cap must reject the 4-shard open hour \
             ({open_hour_cost} GETs); that rejection is the bug this task fixes"
        );

        // Half 2: the cap must keep bounding a runaway query. A "fix" that
        // admits everything is a regression, not a fix. Model a pathological
        // query doing three GETs per recent segment across every shard; its
        // true cost genuinely exceeds the derived budget and it is rejected.
        let runaway_cost = open_hour_cost * 3;
        assert!(
            runaway_cost > budget,
            "test setup: runaway cost {runaway_cost} must genuinely exceed budget {budget}"
        );
        assert!(
            request_budget_exceeded(runaway_cost, limit).is_some(),
            "a query at {runaway_cost} requests exceeds the derived budget {budget} and must be \
             rejected"
        );
        // Boundary: exactly at the budget is admitted, one over is rejected.
        assert!(request_budget_exceeded(budget, limit).is_none());
        assert!(request_budget_exceeded(budget + 1, limit).is_some());
    }

    #[test]
    fn budget_follows_flush_cadence() {
        // ADR-0075 decision 2: raising max_flush_delay (a supported cost lever)
        // lowers the per-shard segment count, and thus the budget, with no hand
        // recomputation. A slower cadence yields a strictly smaller budget.
        let fast = derive_max_s3_requests(DEFAULT_SHARDS, Duration::from_millis(500));
        let slow = derive_max_s3_requests(DEFAULT_SHARDS, Duration::from_millis(1000));
        assert!(
            slow < fast,
            "a slower flush cadence must yield a smaller budget: {slow} < {fast}"
        );
    }

    #[test]
    fn budget_scales_with_shard_count() {
        // ADR-0075 decision 1: the budget grows with shard count because the
        // cost is per shard-hour. The old flat cap sat between the 3-shard and
        // 4-shard true open-hour costs (3 x 7,200 = 21,600 < 25,000 < 28,800 =
        // 4 x 7,200), which is exactly why 4 shards broke: the derived budget
        // now grows to keep the legitimate open hour admitted at every count.
        let three = derive_max_s3_requests(3, DEFAULT_FLUSH_DELAY);
        let four = derive_max_s3_requests(4, DEFAULT_FLUSH_DELAY);
        assert!(four > three, "more shards must yield a larger budget");
        // The 4-shard open hour the old flat cap rejected fits the derived
        // 4-shard budget (a runtime check, not a constant illustration).
        let open_hour_4 = 4 * (3_600_000u64 / 500);
        assert!(!RequestLimit::Bounded(four).is_exceeded_by(open_hour_4));
        assert!(RequestLimit::Bounded(OLD_FLAT_CAP).is_exceeded_by(open_hour_4));
    }
}
