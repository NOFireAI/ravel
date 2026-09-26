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
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use ravel_catalog::{
        DeclaredColumnStats, SegmentLevel, SegmentOrigin, SegmentOrigins, Snapshot,
    };
    use uuid::Uuid;

    use super::{admit, request_budget_exceeded};
    use crate::config::{ByteLimit, EngineConfig, RequestLimit, derive_max_s3_requests};
    use crate::error::QueryError;
    use crate::request_budgets::RequestBudgets;

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

    /// ADR-1306 follow-up task 2, at the figures of its 2026-09-26 amendment.
    #[test]
    fn derived_budget_covers_healthy_tail_plus_stall_alert_window() {
        use crate::config::{
            ALERT_DELIVERY_SLACK, FOLD_STALL_ALERT_FOR, REQUEST_BUDGET_FIXED_OVERHEAD,
            REQUEST_BUDGET_HEADROOM_DEN, REQUEST_BUDGET_HEADROOM_NUM, REQUESTS_PER_UNSEALED_FLUSH,
            SealMargin, covered_span, derive_max_s3_requests_for, healthy_tail_max,
            request_budget_parts,
        };

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

        // The four budgets: ceil(covered_span / cadence) x 2 x 3/2 x shards + 5,000.
        assert_eq!(REQUESTS_PER_UNSEALED_FLUSH, 2);
        let formula = |shards: u64, flushes: u64| {
            flushes * REQUESTS_PER_UNSEALED_FLUSH * REQUEST_BUDGET_HEADROOM_NUM
                / REQUEST_BUDGET_HEADROOM_DEN
                * shards
                + REQUEST_BUDGET_FIXED_OVERHEAD
        };
        let flushes_2s = covered_s.div_ceil(2);
        assert_eq!(flushes_2s, 7_050);
        let flushes_500ms = (covered_s * 1_000).div_ceil(500);
        assert_eq!(flushes_500ms, 28_200);
        for (shards, cadence, flushes, stated) in [
            (1u32, cadence_2s, flushes_2s, 26_150u64),
            (4, cadence_2s, flushes_2s, 89_600),
            (8, cadence_2s, flushes_2s, 174_200),
            (4, cadence_500ms, flushes_500ms, 343_400),
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
        assert_eq!(parts.per_shard_allowance, 7_050 * 2);
        assert_eq!((parts.headroom_num, parts.headroom_den), (3, 2));
        assert_eq!(parts.fixed_overhead, 5_000);
        let bare = crate::config::RequestBudgetParts {
            headroom_num: 1,
            headroom_den: 1,
            ..parts
        };
        assert_eq!(bare.budget(4), 7_050 * 2 * 4 + 5_000);

        // Today's derivation (one hour, 1 request per flush, 3/2) at 4 shards
        // and 2 s, against the 2 h 20 m tail a healthy catalog reaches.
        let today = (HOUR_S / 2) * REQUEST_BUDGET_HEADROOM_NUM / REQUEST_BUDGET_HEADROOM_DEN * 4
            + REQUEST_BUDGET_FIXED_OVERHEAD;
        assert_eq!(today, 15_800);
        let tail_s = 2 * HOUR_S + 20 * 60;
        let tail_flushes = tail_s / 2;
        assert_eq!(tail_flushes, 4_200);
        let tail_cost = tail_flushes * REQUESTS_PER_UNSEALED_FLUSH * 4;
        assert_eq!(tail_cost, 33_600);
        // The fixed overhead stays reserved for requests outside the tail.
        let tail_query = tail_cost + REQUEST_BUDGET_FIXED_OVERHEAD;
        assert!(request_budget_exceeded(tail_query, RequestLimit::Bounded(today)).is_some());
        assert!(request_budget_exceeded(tail_cost, RequestLimit::Bounded(today)).is_some());
        let new_budget = derive_max_s3_requests(4, cadence_2s);
        assert_eq!(new_budget, 89_600);
        assert!(request_budget_exceeded(tail_query, RequestLimit::Bounded(new_budget)).is_none());

        // A runaway at three times the covered-span cost is still refused.
        let runaway = 3 * flushes_2s * REQUESTS_PER_UNSEALED_FLUSH * 4;
        assert_eq!(runaway, 169_200);
        assert!(request_budget_exceeded(runaway, RequestLimit::Bounded(new_budget)).is_some());
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
            Some(QueryError::RequestBudgetExceeded { requests, max }) => {
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
