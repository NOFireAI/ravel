//! Unified snapshot admission seam (ADR-0073 decision 4): the sealed-set
//! count check and the request-spend budget check, one call per resolve
//! replacing the eight divergent per-surface checks the ADR describes. This
//! crate's PromQL engine is the only site wired up here
//! (`crates/ravel-query/src/engine.rs`'s `resolve_bounded`); the SQL
//! executor, the five SQL providers, and the exemplars state move onto this
//! seam.

use std::time::Duration;

use ravel_catalog::{SegmentOrigin, SegmentOrigins, Snapshot};

use crate::config::{EngineConfig, RequestLimit};
use crate::error::{FoldLag, QueryError};

/// Nanoseconds in one ingest-hour bucket. `ingest_hour_bucket` counts whole
/// hours since the epoch, so bucket `H` starts at `H * NS_PER_HOUR` and ends at
/// `(H + 1) * NS_PER_HOUR` (`ravel_catalog`'s `sealed_watermark_hour`).
const NS_PER_HOUR: i64 = 3_600_000_000_000;

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

/// True when this resolve's own outputs prove it read a folded snapshot part,
/// so its [`SegmentOrigin::Recent`] tags really do mean "above the fold
/// watermark".
///
/// Both witnesses are produced by the snapshot-extract step of
/// `Catalog::resolve_impl` and by nothing else: `sealed_count` counts
/// [`SegmentOrigin::SealedBelowWatermark`], which only that step assigns (the
/// read-your-write token paths tag [`SegmentOrigin::TokenResolved`]), and
/// `segments_pruned` counts postings pruning, which is applied to
/// snapshot-sourced segments only.
///
/// When neither fired, the resolve either found no usable snapshot (an absent
/// HEAD, or parts that were corrupt or unreadable) and listed the whole window
/// live, or the snapshot it did read contributed nothing to this window. Either
/// way every key it produced is tagged `Recent`, including hours that a fold
/// keeping up has already sealed, so the span those tags imply is not a tail
/// the fold can be held to.
fn resolve_read_a_snapshot_part(snapshot: &Snapshot, origins: &SegmentOrigins) -> bool {
    origins.sealed_count > 0 || snapshot.segments_pruned > 0
}

/// The unsealed tail one resolve saw: the span from the start of the oldest
/// ingest hour it listed live (above the fold watermark) to query time
/// (ADR-1306 decision 6). `None` when there is no such span it can attribute
/// to the fold: no unsealed segment was listed, or this resolve read no folded
/// snapshot part at all ([`resolve_read_a_snapshot_part`]).
///
/// Read off the resolve's own outputs, never from a fresh store request. When
/// a snapshot part was read, an origin of [`SegmentOrigin::Recent`] is exactly
/// "listed above the watermark", so the oldest such bucket starts at or after
/// the end of the newest sealed hour and this is a lower bound on
/// `now - end(watermark hour)`, never an over-estimate. When none was read the
/// `Recent` tags carry no watermark at all, which is why that case is `None`
/// rather than a span measured against a watermark this resolve never saw.
/// `TokenResolved` segments are excluded either way: a read-your-write token
/// can resolve a segment below the watermark, which is not tail.
#[must_use]
pub fn resolved_unsealed_tail(
    snapshot: &Snapshot,
    origins: &SegmentOrigins,
    now_ns: i64,
) -> Option<Duration> {
    if !resolve_read_a_snapshot_part(snapshot, origins) {
        return None;
    }
    let oldest_unsealed_hour = snapshot
        .segments
        .iter()
        .zip(origins.origins.iter())
        .filter(|(_, origin)| matches!(origin, SegmentOrigin::Recent))
        .map(|(segment, _)| segment.ingest_hour_bucket)
        .min()?;
    let hour_start_ns = i64::from(oldest_unsealed_hour).checked_mul(NS_PER_HOUR)?;
    let tail_ns = now_ns.checked_sub(hour_start_ns)?;
    u64::try_from(tail_ns).ok().map(Duration::from_nanos)
}

/// The fold lag a resolve's unsealed tail implies, for the request-budget
/// refusals downstream of it (ADR-1306 decision 6). `fold_lag_threshold` is
/// [`crate::config::EngineConfig::fold_lag_threshold`] of the engine the query
/// runs on.
#[must_use]
pub fn resolved_fold_lag(
    snapshot: &Snapshot,
    origins: &SegmentOrigins,
    now_ns: i64,
    fold_lag_threshold: Duration,
) -> FoldLag {
    FoldLag::from_resolved_tail(
        resolved_unsealed_tail(snapshot, origins, now_ns),
        fold_lag_threshold,
    )
}

/// A request budget together with what the resolve behind this query's
/// snapshot saw of the catalog's unsealed tail.
///
/// The two travel as one value so a check site cannot enforce the limit while
/// forgetting the tail: every refusal built from a [`RequestBudget`] carries
/// the fold-lag verdict the resolve computed (ADR-1306 decision 6). A bare
/// [`RequestLimit`] converts in, for a caller with no resolve in hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestBudget {
    pub limit: RequestLimit,
    pub fold_lag: FoldLag,
}

impl RequestBudget {
    /// This budget's limit with `fold_lag` attached.
    #[must_use]
    pub fn new(limit: RequestLimit, fold_lag: FoldLag) -> RequestBudget {
        RequestBudget { limit, fold_lag }
    }
}

impl From<RequestLimit> for RequestBudget {
    /// A limit checked with nothing known about the tail: a refusal keeps the
    /// pre-ADR-1306 message.
    fn from(limit: RequestLimit) -> RequestBudget {
        RequestBudget {
            limit,
            fold_lag: FoldLag::Healthy,
        }
    }
}

/// True when `requests` has passed the budget's limit, mirroring
/// `bytes_scanned_exceeded`'s incremental-comparison shape (ADR-0073
/// decision 3): a typed error, checked at the same points the bytes-scanned
/// budget already checks, never a truncation.
pub fn request_budget_exceeded(
    requests: u64,
    budget: impl Into<RequestBudget>,
) -> Option<QueryError> {
    let budget = budget.into();
    match budget.limit {
        RequestLimit::Bounded(max) if requests > max => Some(QueryError::RequestBudgetExceeded {
            requests,
            max,
            fold_lag: budget.fold_lag,
        }),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use ravel_catalog::{
        DeclaredColumnStats, SegmentLevel, SegmentOrigin, SegmentOrigins, Snapshot,
    };
    use uuid::Uuid;

    use super::{
        RequestBudget, admit, request_budget_exceeded, resolved_fold_lag, resolved_unsealed_tail,
    };
    use crate::config::{
        BUDGETED_REQUESTS_PER_UNSEALED_FLUSH, ByteLimit, EngineConfig, RequestLimit, SealMargin,
        covered_span, derive_max_s3_requests,
    };
    use crate::error::{FoldLag, QueryError};
    use crate::request_budgets::RequestBudgets;

    /// The shard count a `ravel-server` process runs with by default
    /// (`services/ravel-server`'s `--shards`, `default_value_t = 4`). Named
    /// here, not imported: ravel-query cannot depend on ravel-server. The
    /// ravel-server reachability test pins that the real startup path derives
    /// the budget at this same value.
    const DEFAULT_SHARDS: u32 = 4;
    /// The cadence `EngineConfig::default` derives its budget at
    /// (`DEFAULT_BUDGET_REFERENCE_FLUSH_DELAY`), and the fastest cadence any
    /// deployment here is sized for, so it is the largest open-hour cost a
    /// shard can present. `ravel_ingest::IngestConfig::default`'s
    /// `max_flush_delay` is the slower 2 s, whose open hour is a quarter of
    /// this one and fits the correspondingly smaller budget the same way;
    /// `derived_budget_covers_healthy_tail_plus_stall_alert_window` pins both.
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
        // segments per shard per open hour. At one GET each on every shard
        // that is 4 x 7,200 = 28,800 requests, the exact cost the old flat
        // 25,000 cap rejected at 4 shards; at the per-flush ceiling of any
        // flush size it is 7,200 x 8 x 4 = 230,400.
        let segments_per_shard_hour = 3_600_000u64 / 500;
        assert_eq!(segments_per_shard_hour, 7_200);
        let one_get_open_hour = segments_per_shard_hour * u64::from(DEFAULT_SHARDS);
        assert_eq!(one_get_open_hour, 28_800);
        assert_eq!(BUDGETED_REQUESTS_PER_UNSEALED_FLUSH, 8);
        let open_hour_cost = one_get_open_hour * BUDGETED_REQUESTS_PER_UNSEALED_FLUSH;
        assert_eq!(open_hour_cost, 230_400);
        for cost in [one_get_open_hour, open_hour_cost] {
            assert!(
                request_budget_exceeded(cost, limit).is_none(),
                "a legitimate open hour ({cost} requests) must fit the derived budget ({budget})"
            );
            // Non-vacuity guard: the flat cap the derivation replaces MUST
            // reject this same cost. Without this the "fits" assertion above
            // could pass against any large-enough constant and prove nothing.
            assert!(
                request_budget_exceeded(cost, RequestLimit::Bounded(OLD_FLAT_CAP)).is_some(),
                "the old flat {OLD_FLAT_CAP} cap must reject the 4-shard open hour ({cost} \
                 requests); that rejection is the bug the derivation fixes"
            );
        }

        // Half 2: the cap must keep bounding a runaway query. A "fix" that
        // admits everything is a regression, not a fix. The budget covers
        // covered_span (3 h 55 m) of flushes at the per-flush ceiling plus
        // headroom, far more than one hour, so the runaway is modelled at three
        // times the covered-span cost across every shard: 3 x 28,200 x 8 x 4.
        let covered_flushes = covered_span(SealMargin::REFERENCE)
            .as_millis()
            .div_ceil(DEFAULT_FLUSH_DELAY.as_millis());
        let covered_flushes = u64::try_from(covered_flushes).expect("fits u64");
        assert_eq!(covered_flushes, 28_200);
        let runaway_cost =
            3 * covered_flushes * BUDGETED_REQUESTS_PER_UNSEALED_FLUSH * u64::from(DEFAULT_SHARDS);
        assert_eq!(runaway_cost, 2_707_200);
        assert_eq!(budget, 1_358_600);
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

    /// ADR-1306 follow-up task 2, with the per-flush term at the ceiling that
    /// covers flushes above the fetcher's whole-object threshold.
    #[test]
    fn derived_budget_covers_healthy_tail_plus_stall_alert_window() {
        use crate::config::{
            ALERT_DELIVERY_SLACK, FOLD_STALL_ALERT_FOR, MAX_REQUESTS_PER_UNSEALED_FLUSH,
            REQUEST_BUDGET_FIXED_OVERHEAD, REQUEST_BUDGET_HEADROOM_DEN,
            REQUEST_BUDGET_HEADROOM_NUM, REQUESTS_PER_UNSEALED_FLUSH, derive_max_s3_requests_for,
            healthy_tail_max, request_budget_parts,
        };
        use crate::fetcher::{MAX_GETS_PER_L0_SEGMENT_FETCH, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT};

        const HOUR_S: u64 = 3_600;
        let cadence_2s = Duration::from_secs(2);
        let cadence_500ms = Duration::from_millis(500);
        let margin = SealMargin::REFERENCE;

        // The reference seal margin is the catalog's compiled-in 1 h + 5 m + 15 m.
        assert_eq!(margin.max_flush_lifetime, Duration::from_secs(HOUR_S));
        assert_eq!(margin.clock_skew_allowance, Duration::from_secs(5 * 60));
        assert_eq!(margin.fold_safety_margin, Duration::from_secs(15 * 60));
        assert_eq!(
            margin,
            SealMargin::from_catalog_config(&ravel_catalog::CatalogConfig::default())
        );
        let seal_margin_s = HOUR_S + 5 * 60 + 15 * 60;
        assert_eq!(margin.total().as_secs(), seal_margin_s);
        assert_eq!(seal_margin_s, 4_800);

        // covered_span = healthy_tail_max + lag_allowance = 8,400 s + 5,700 s.
        let healthy_tail_s = seal_margin_s + HOUR_S;
        assert_eq!(healthy_tail_s, 8_400);
        assert_eq!(
            healthy_tail_max(margin),
            Duration::from_secs(healthy_tail_s)
        );
        let lag_allowance_s =
            seal_margin_s + FOLD_STALL_ALERT_FOR.as_secs() + ALERT_DELIVERY_SLACK.as_secs();
        assert_eq!(lag_allowance_s, 5_700);
        let covered_s = healthy_tail_s + lag_allowance_s;
        assert_eq!(covered_s, 14_100);
        assert_eq!(covered_span(margin), Duration::from_secs(covered_s));
        assert_eq!(covered_span(margin), Duration::from_secs(14_100));

        // The per-flush term: the larger of the measured 2 for a flush at or
        // under the whole-object threshold and the ceiling above it, the
        // commit-record GET plus the fetcher's first GET, footer chase,
        // catalog GET and 4 page-range GETs.
        assert_eq!(REQUESTS_PER_UNSEALED_FLUSH, 2);
        assert_eq!(MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT, 4);
        let fetch_ceiling = 1 + 1 + 1 + MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64;
        assert_eq!(fetch_ceiling, 7);
        assert_eq!(MAX_GETS_PER_L0_SEGMENT_FETCH, fetch_ceiling);
        let per_flush = 1 + fetch_ceiling;
        assert_eq!(per_flush, 8);
        assert_eq!(MAX_REQUESTS_PER_UNSEALED_FLUSH, per_flush);
        assert_eq!(
            BUDGETED_REQUESTS_PER_UNSEALED_FLUSH,
            per_flush.max(REQUESTS_PER_UNSEALED_FLUSH)
        );
        assert_eq!(BUDGETED_REQUESTS_PER_UNSEALED_FLUSH, 8);

        // The four budgets: ceil(covered_span / cadence) x 8 x 3/2 x shards + 5,000.
        let formula = |shards: u64, flushes: u64| {
            flushes * per_flush * REQUEST_BUDGET_HEADROOM_NUM / REQUEST_BUDGET_HEADROOM_DEN * shards
                + REQUEST_BUDGET_FIXED_OVERHEAD
        };
        let flushes_2s = covered_s.div_ceil(2);
        assert_eq!(flushes_2s, 7_050);
        let flushes_500ms = (covered_s * 1_000).div_ceil(500);
        assert_eq!(flushes_500ms, 28_200);
        for (shards, cadence, flushes, stated) in [
            (1u32, cadence_2s, flushes_2s, 89_600u64),
            (4, cadence_2s, flushes_2s, 343_400),
            (8, cadence_2s, flushes_2s, 681_800),
            (4, cadence_500ms, flushes_500ms, 1_358_600),
        ] {
            let expected = formula(u64::from(shards), flushes);
            assert_eq!(
                expected, stated,
                "ADR figure at {shards} shards, {cadence:?}"
            );
            assert_eq!(
                derive_max_s3_requests(shards, cadence),
                stated,
                "derive_max_s3_requests at {shards} shards, {cadence:?}"
            );
            assert_eq!(derive_max_s3_requests_for(shards, cadence, margin), stated);
        }

        // The parts carry the pre-headroom allowance, so headroom 1 rebuilds
        // the bare covered-span cost plus the fixed overhead.
        let parts = request_budget_parts(cadence_2s, margin);
        assert_eq!(parts.per_shard_allowance, flushes_2s * per_flush);
        assert_eq!(parts.per_shard_allowance, 56_400);
        assert_eq!((parts.headroom_num, parts.headroom_den), (3, 2));
        assert_eq!(parts.fixed_overhead, 5_000);
        let bare = crate::config::RequestBudgetParts {
            headroom_num: 1,
            headroom_den: 1,
            ..parts
        };
        assert_eq!(bare.budget(4), flushes_2s * per_flush * 4 + 5_000);
        assert_eq!(bare.budget(4), 230_600);

        // Today's derivation (one hour, 1 request per flush, 3/2) at 4 shards
        // and 2 s, against the 2 h 20 m tail a healthy catalog reaches.
        let today = (HOUR_S / 2) * REQUEST_BUDGET_HEADROOM_NUM / REQUEST_BUDGET_HEADROOM_DEN * 4
            + REQUEST_BUDGET_FIXED_OVERHEAD;
        assert_eq!(today, 15_800);
        let tail_s = 2 * HOUR_S + 20 * 60;
        let tail_flushes = tail_s / 2;
        assert_eq!(tail_flushes, 4_200);
        let tail_cost = tail_flushes * per_flush * 4;
        assert_eq!(tail_cost, 134_400);
        // The fixed overhead stays reserved for requests outside the tail.
        let tail_query = tail_cost + REQUEST_BUDGET_FIXED_OVERHEAD;
        assert_eq!(tail_query, 139_400);
        assert!(request_budget_exceeded(tail_query, RequestLimit::Bounded(today)).is_some());
        assert!(request_budget_exceeded(tail_cost, RequestLimit::Bounded(today)).is_some());
        let new_budget = derive_max_s3_requests(4, cadence_2s);
        assert_eq!(new_budget, 343_400);
        assert!(request_budget_exceeded(tail_query, RequestLimit::Bounded(new_budget)).is_none());
        // The same tail at the small-flush cost fits too.
        let small_flush_tail = tail_flushes * REQUESTS_PER_UNSEALED_FLUSH * 4;
        assert_eq!(small_flush_tail, 33_600);
        assert!(
            request_budget_exceeded(small_flush_tail, RequestLimit::Bounded(new_budget)).is_none()
        );

        // A runaway at three times the covered-span cost is still refused.
        let runaway = 3 * flushes_2s * per_flush * 4;
        assert_eq!(runaway, 676_800);
        assert!(request_budget_exceeded(runaway, RequestLimit::Bounded(new_budget)).is_some());
    }

    /// A snapshot of one segment per `(ingest_hour_bucket, origin)` pair, in
    /// the order given, with `origins` parallel to `segments`.
    fn snapshot_of(entries: &[(u32, SegmentOrigin)]) -> (Snapshot, SegmentOrigins) {
        // One `SegmentRef` template from the fixture below, re-stamped with
        // each entry's hour: only `ingest_hour_bucket` matters here.
        let (template, _) = snapshot_with_sealed(0);
        let mut segments = Vec::with_capacity(entries.len());
        let mut origins = SegmentOrigins::default();
        for (hour, origin) in entries {
            let mut segment = template.segments[0].clone();
            segment.ingest_hour_bucket = *hour;
            segments.push(segment);
            origins.push(*origin);
        }
        (
            Snapshot {
                segments,
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            origins,
        )
    }

    /// ADR-1306 decision 6: the tail is measured from the START of the oldest
    /// hour the resolve listed live, which is at or after the end of the
    /// watermark hour, and only `Recent` segments count. A `TokenResolved`
    /// segment can sit below the watermark, so counting it would invent tail
    /// the fold is not behind on.
    #[test]
    fn the_resolved_tail_is_measured_from_the_oldest_recent_hour() {
        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let now = 100 * NS_PER_HOUR + 1_800 * 1_000_000_000;

        // Two recent hours: the older one sets the tail.
        let (snapshot, origins) = snapshot_of(&[
            (99, SegmentOrigin::Recent),
            (100, SegmentOrigin::Recent),
            (40, SegmentOrigin::SealedBelowWatermark),
        ]);
        assert_eq!(
            resolved_unsealed_tail(&snapshot, &origins, now),
            Some(Duration::from_secs(3_600 + 1_800))
        );

        // A long-since-sealed segment and an old token-resolved one are not
        // tail, however far back they sit.
        let (snapshot, origins) = snapshot_of(&[
            (10, SegmentOrigin::SealedBelowWatermark),
            (11, SegmentOrigin::TokenResolved),
            (100, SegmentOrigin::Recent),
        ]);
        assert_eq!(
            resolved_unsealed_tail(&snapshot, &origins, now),
            Some(Duration::from_secs(1_800))
        );

        // No unsealed data resolved: no tail to report, and no lag.
        let (snapshot, origins) = snapshot_of(&[(10, SegmentOrigin::SealedBelowWatermark)]);
        assert_eq!(resolved_unsealed_tail(&snapshot, &origins, now), None);
        assert_eq!(
            resolved_fold_lag(&snapshot, &origins, now, reference_threshold()),
            FoldLag::Healthy
        );
    }

    /// ADR-1306 "Amendment (2026-09-27, #1306)", finding 2: a resolve that read
    /// no folded snapshot part listed the whole window live and tagged every
    /// key `Recent`, including hours a fold that is keeping up has already
    /// sealed. There is no watermark behind those tags, so there is no tail to
    /// measure and nothing to blame the fold for. One sealed segment, or one
    /// postings-pruned one, is the witness that a part WAS read, and then the
    /// same origins do carry a tail.
    #[test]
    fn a_resolve_that_read_no_snapshot_part_reports_no_tail() {
        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let now = 100 * NS_PER_HOUR + 1_800 * 1_000_000_000;
        let threshold = reference_threshold();

        // Every key `Recent`: the listing fallback. Six hours of apparent tail
        // that the fold is not accountable for.
        let (fallback, fallback_origins) =
            snapshot_of(&[(94, SegmentOrigin::Recent), (100, SegmentOrigin::Recent)]);
        assert_eq!(
            resolved_unsealed_tail(&fallback, &fallback_origins, now),
            None
        );
        assert_eq!(
            resolved_fold_lag(&fallback, &fallback_origins, now, threshold),
            FoldLag::Healthy
        );

        // A token-resolved segment is not a witness either: the token paths
        // never read a snapshot part.
        let (tokens, token_origins) = snapshot_of(&[
            (94, SegmentOrigin::Recent),
            (11, SegmentOrigin::TokenResolved),
        ]);
        assert_eq!(resolved_unsealed_tail(&tokens, &token_origins, now), None);

        // One sealed segment: a part was read, so the same 6 h 30 m tail is
        // real and is lag.
        let (sealed, sealed_origins) = snapshot_of(&[
            (94, SegmentOrigin::Recent),
            (40, SegmentOrigin::SealedBelowWatermark),
        ]);
        let tail = Duration::from_secs(6 * 3_600 + 1_800);
        assert_eq!(
            resolved_unsealed_tail(&sealed, &sealed_origins, now),
            Some(tail)
        );
        assert_eq!(
            resolved_fold_lag(&sealed, &sealed_origins, now, threshold),
            FoldLag::Lagging {
                unsealed_tail: tail,
                fold_lag_threshold: threshold,
            }
        );

        // Postings pruning is the other witness: it only ever removes
        // snapshot-sourced segments, so a pruned count proves a part was read
        // even when nothing sealed survived into the window.
        let (mut pruned, pruned_origins) = snapshot_of(&[(94, SegmentOrigin::Recent)]);
        pruned.segments_pruned = 1;
        assert_eq!(
            resolved_unsealed_tail(&pruned, &pruned_origins, now),
            Some(tail)
        );
    }

    /// The reference fold-lag threshold: 8,400 s of healthy tail, plus the
    /// 300 s fold interval, plus the 30 s HEAD cache TTL.
    fn reference_threshold() -> Duration {
        crate::config::fold_lag_tail_threshold(
            SealMargin::REFERENCE,
            crate::config::REFERENCE_FOLD_INTERVAL,
            crate::config::REFERENCE_HEAD_CACHE_TTL,
        )
    }

    /// The classification boundary: exactly the fold-lag threshold is healthy,
    /// one nanosecond more is lag. A refusal at the boundary must not blame a
    /// fold that is keeping up.
    ///
    /// The threshold is not `healthy_tail_max`. A fold leaves at most
    /// `healthy_tail_max` at the instant it runs, then waits one fold interval
    /// while the tail grows, and the resolve reads the resulting watermark
    /// through a HEAD cache that may be one TTL stale. Classifying against
    /// 8,400 s would report a fold that is keeping up as lagging for the last
    /// 330 s of every cycle.
    #[test]
    fn fold_lag_starts_one_nanosecond_past_the_fold_lag_threshold() {
        let margin = SealMargin::REFERENCE;
        assert_eq!(
            crate::config::healthy_tail_max(margin),
            Duration::from_secs(8_400)
        );
        let threshold = reference_threshold();
        assert_eq!(threshold, Duration::from_secs(8_730));
        assert_eq!(
            EngineConfig::default().fold_lag_threshold(),
            Duration::from_secs(8_730),
            "the engine's default config must classify against the same 8,730 s"
        );

        assert_eq!(
            FoldLag::from_resolved_tail(Some(threshold), threshold),
            FoldLag::Healthy
        );
        assert_eq!(
            FoldLag::from_resolved_tail(Some(threshold + Duration::from_nanos(1)), threshold),
            FoldLag::Lagging {
                unsealed_tail: threshold + Duration::from_nanos(1),
                fold_lag_threshold: threshold,
            }
        );
        assert_eq!(
            FoldLag::from_resolved_tail(None, threshold),
            FoldLag::Healthy
        );

        // The window the fix opened: a tail a keeping-up fold really can show,
        // past the old 8,400 s bound and inside the new one.
        for tail in [
            Duration::from_secs(8_401),
            Duration::from_secs(8_700),
            Duration::from_secs(8_730),
        ] {
            assert_eq!(
                FoldLag::from_resolved_tail(Some(tail), threshold),
                FoldLag::Healthy,
                "{tail:?} is inside one fold interval plus one HEAD cache TTL of the healthy tail"
            );
        }

        // The bound follows the configured seal margin, not a constant: a
        // catalog that folds with a longer margin carries a longer healthy
        // tail, and the same tail stops counting as lag.
        let wide = crate::config::fold_lag_tail_threshold(
            SealMargin {
                max_flush_lifetime: Duration::from_secs(6 * 3_600),
                ..margin
            },
            crate::config::REFERENCE_FOLD_INTERVAL,
            crate::config::REFERENCE_HEAD_CACHE_TTL,
        );
        assert_eq!(
            FoldLag::from_resolved_tail(Some(threshold + Duration::from_nanos(1)), wide),
            FoldLag::Healthy
        );
    }

    /// A refusal renders the fold-lag clause; a refusal with a healthy tail
    /// renders exactly the message this variant had before ADR-1306.
    #[test]
    fn only_a_lagging_refusal_names_the_fold_gauge() {
        let lagging = FoldLag::Lagging {
            unsealed_tail: Duration::from_secs(19_800),
            fold_lag_threshold: Duration::from_secs(8_730),
        };
        let err = request_budget_exceeded(7, RequestBudget::new(RequestLimit::Bounded(1), lagging))
            .expect("7 requests exceed a budget of 1");
        let message = err.to_string();
        assert!(message.starts_with("query issued 7 S3 requests, exceeding the budget of 1;"));
        assert!(message.contains("19800 s"), "{message}");
        assert!(message.contains("8730 s"), "{message}");
        assert!(
            message.contains(crate::error::FOLD_LAST_SUCCESS_GAUGE),
            "{message}"
        );

        let healthy = request_budget_exceeded(7, RequestLimit::Bounded(1))
            .expect("7 requests exceed a budget of 1");
        assert_eq!(
            healthy.to_string(),
            "query issued 7 S3 requests, exceeding the budget of 1"
        );
    }

    /// A snapshot of `sealed` sealed, below-watermark segments and one recent
    /// (exempt) one, with `origins` parallel to `segments` as `admit`'s
    /// debug assertion requires.
    fn snapshot_with_sealed(sealed: usize) -> (Snapshot, SegmentOrigins) {
        let segment = ravel_catalog::SegmentRef {
            data_object_key: "irrelevant".to_string(),
            object_size: 4_096,
            min_event_ts_ns: 0,
            max_event_ts_ns: 1,
            ingest_hour_bucket: 0,
            sample_count: 1,
            series_count: 1,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::nil(),
            writer_epoch: 0,
            writer_seq: 0,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: 1,
            declared_column_stats: DeclaredColumnStats::default(),
        };
        let mut origins = SegmentOrigins::default();
        for _ in 0..sealed {
            origins.push(SegmentOrigin::SealedBelowWatermark);
        }
        origins.push(SegmentOrigin::Recent);
        let snapshot = Snapshot {
            segments: vec![segment; sealed + 1],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        (snapshot, origins)
    }

    /// ADR-1374 decision 3: a caller-supplied [`RequestBudgets`] can only
    /// LOWER a server ceiling. A value above the ceiling resolves to the
    /// ceiling, a value below it resolves to itself, and an absent value
    /// leaves the ceiling untouched.
    ///
    /// Asserted through the two enforcement seams, not only through `clamp`:
    /// the clamped values are what `admit` and `request_budget_exceeded`
    /// actually read, so a clamp that computed the right number but never
    /// reached the check would pass a `clamp`-only test.
    #[test]
    fn request_budget_cannot_be_raised_above_server_ceiling() {
        let ceiling = EngineConfig {
            max_segments: 10,
            max_bytes_scanned: ByteLimit::Bounded(1_000),
            max_s3_requests: RequestLimit::Bounded(100),
            ..EngineConfig::default()
        };

        // Above the ceiling in every field: each one clamps down to the
        // ceiling's exact value, never the caller's.
        let raised = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Bounded(1_000_000)),
            max_store_requests: Some(RequestLimit::Bounded(50_000)),
            max_segments: Some(9_999),
        }
        .clamp(&ceiling);
        assert_eq!(raised.max_bytes_scanned, ByteLimit::Bounded(1_000));
        assert_eq!(raised.max_store_requests, RequestLimit::Bounded(100));
        assert_eq!(raised.max_segments, 10);

        // `Unlimited` is the strongest possible ask and still cannot lift a
        // bounded ceiling.
        let unbounded = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Unlimited),
            max_store_requests: Some(RequestLimit::Unlimited),
            max_segments: None,
        }
        .clamp(&ceiling);
        assert_eq!(unbounded.max_bytes_scanned, ByteLimit::Bounded(1_000));
        assert_eq!(unbounded.max_store_requests, RequestLimit::Bounded(100));
        assert_eq!(unbounded.max_segments, 10);

        // Below the ceiling: the caller's own value, exactly.
        let lowered = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Bounded(256)),
            max_store_requests: Some(RequestLimit::Bounded(7)),
            max_segments: Some(2),
        }
        .clamp(&ceiling);
        assert_eq!(lowered.max_bytes_scanned, ByteLimit::Bounded(256));
        assert_eq!(lowered.max_store_requests, RequestLimit::Bounded(7));
        assert_eq!(lowered.max_segments, 2);

        // Absent entirely: byte-identical to the pre-ADR-1374 behavior.
        let absent = RequestBudgets::default().clamp(&ceiling);
        assert_eq!(absent.max_bytes_scanned, ceiling.max_bytes_scanned);
        assert_eq!(absent.max_store_requests, ceiling.max_s3_requests);
        assert_eq!(absent.max_segments, ceiling.max_segments);
        assert_eq!(absent, RequestBudgets::clamp_optional(None, &ceiling));

        // The request-budget check reads the effective value. 50 requests is
        // under the ceiling of 100 and over the lowered 7.
        assert!(request_budget_exceeded(50, raised.max_store_requests).is_none());
        assert!(request_budget_exceeded(50, absent.max_store_requests).is_none());
        match request_budget_exceeded(50, lowered.max_store_requests) {
            Some(QueryError::RequestBudgetExceeded {
                requests,
                max,
                fold_lag: _,
            }) => {
                assert_eq!(requests, 50);
                assert_eq!(max, 7);
            }
            other => panic!("a lowered request budget must reject 50 requests, got {other:?}"),
        }

        // `admit` reads the effective segment count the same way: 5 sealed
        // segments fit the ceiling of 10 and the raise-attempt's clamped 10,
        // and are rejected under the lowered 2.
        let (snapshot, origins) = snapshot_with_sealed(5);
        for effective in [raised, absent] {
            let admitted = admit(&snapshot, &origins, &effective.applied_to(&ceiling))
                .expect("5 sealed segments fit a ceiling of 10");
            assert_eq!(admitted.sealed_count, 5);
            assert_eq!(admitted.exempt_count, 1);
        }
        match admit(&snapshot, &origins, &lowered.applied_to(&ceiling)) {
            Err(QueryError::TooManySegments { count, max }) => {
                assert_eq!(count, 5);
                assert_eq!(max, 2);
            }
            other => panic!("a lowered max_segments must reject 5 sealed segments, got {other:?}"),
        }
    }
}
