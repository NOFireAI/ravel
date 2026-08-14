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
