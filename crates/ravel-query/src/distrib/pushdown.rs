//! The ADR-0103 order-insensitive aggregation pushdown eligibility gate
//! (decision 1).
//!
//! Pushdown lets a slice worker compute a `count`/`min`/`max`/distinct-group
//! partial instead of shipping raw samples back. That is exact only when no
//! series can be held by two workers (or by a worker and a remote cluster) at
//! once: a worker merging only the runs IT holds cannot know whether a sample
//! it counted is about to lose to an overlapping duplicate elsewhere, so
//! combining such partials would be a silent wrong answer, not an
//! approximation. Two independent mechanisms can duplicate one series across
//! sources, and this module closes both:
//!
//! 1. **Online resharding (ADR-0052).** `shard_for(series_id, shard_count)` is
//!    a modulus over a per-generation `shard_count`, and existing data is never
//!    re-keyed when that count changes. A series written under two generations
//!    therefore carries two different shard values, and `partition_snapshot`
//!    cuts slices by shard, so the same series lands on two workers.
//! 2. **Federation.** `Federation` folds remote clusters' decoded series into
//!    the coordinator's merge pool under a shared `SeriesId`, with no relation
//!    to shard generations at all. A local partial is never complete for a
//!    series a remote may also hold.
//!
//! The gate is deliberately coarse: one boolean per query, computed from the
//! *resolved* segment set (not the query's stated event-time window --
//! `Catalog::window_hour_bounds` extends the ingest-hour scan range past the
//! event window's end so late-arriving writes are still found, so an
//! event-time check can pass while the resolve still straddles a reshard).
//! Ineligible means "falls back to today's raw-fetch-and-merge path", never
//! "wrong": the visible signal is "not accelerated".
//!
//! Nothing in the production query path calls this module yet. It is the
//! primitive ADR-0103's planner-integration task (epic #64, T4) wires into the
//! planner; the aggregate-expression-shape gate (ADR-0103 decision 4), which is
//! independent of this one, is separate work.

use ravel_catalog::{
    DEFAULT_SCAN_SLACK_HOURS, SegmentRef, ShardGeneration, stable_generation_for_hour,
};

use crate::distrib::federation::Federation;

/// Whether every ingest hour in `hours` is owned by the *same single* shard
/// generation (ADR-0103 decision 1(b)): the reshard half of the eligibility
/// gate.
///
/// `true` only when [`stable_generation_for_hour`] returns `Some` for every
/// hour and every one of those answers is the same generation id. Any hour that
/// is ambiguous (inside a generation boundary's slack margin, where ADR-0052's
/// scan rule still scans the retiring generation's shards) makes the whole set
/// ineligible, as does any pair of hours owned by two different generations.
///
/// An **empty** `hours` is `false`. A query that resolved no segments has no
/// data to push down, so eligibility is moot; answering "ineligible" keeps this
/// function's contract a single unambiguous rule ("exactly one generation owns
/// every hour") instead of a vacuous `true` a caller could mistake for a
/// positive verdict.
///
/// `generations` must be the normalized, validated history
/// `Catalog::resolve_pruned_with_generations` returns *for this same resolve*,
/// never a second, independently-read copy: see that method for why.
pub fn all_hours_in_one_stable_generation<I>(
    generations: &[ShardGeneration],
    hours: I,
    slack_hours: u32,
) -> bool
where
    I: IntoIterator<Item = u32>,
{
    let mut owner: Option<u32> = None;
    for hour in hours {
        match stable_generation_for_hour(generations, hour, slack_hours) {
            // Ambiguous hour: the scan set for it holds more than one
            // generation, so a series in it can carry two shard values.
            None => return false,
            Some(id) => match owner {
                None => owner = Some(id),
                Some(seen) if seen == id => {}
                Some(_) => return false,
            },
        }
    }
    owner.is_some()
}

/// Both halves of ADR-0103 decision 1: whether this query may attempt
/// order-insensitive aggregation pushdown.
///
/// - `false` immediately when `federation` is `Some` (decision 1(a)): any
///   configured remote means a local partial can be incomplete for a series the
///   remote also holds. Unconditional, regardless of how many remotes are
///   configured or whether they would actually contribute.
/// - Otherwise, `true` iff every resolved segment's `ingest_hour_bucket` is
///   owned by one and the same shard generation's stable interval (decision
///   1(b)), evaluated with [`DEFAULT_SCAN_SLACK_HOURS`].
///
/// `segments` are the segments the query actually resolved (post-resolve, one
/// pass over an already-materialized list), and `generations` is the history
/// that same resolve read
/// (`Catalog::resolve_pruned_with_generations`). Passing a freshly-read or
/// separately-cached history instead would reintroduce exactly the skew
/// decision 1(b) forbids: a generation appended between the two reads would be
/// invisible here while its segments are already in `segments`.
///
/// This is the whole eligibility gate, and it is intentionally not called by
/// any production query path yet (epic #64, T4 wires it into the planner). The
/// expression-shape gate (ADR-0103 decision 4) is a separate, independent
/// check; neither implies the other.
pub fn is_pushdown_eligible(
    federation: Option<&Federation>,
    segments: &[SegmentRef],
    generations: &[ShardGeneration],
) -> bool {
    if federation.is_some() {
        return false;
    }
    all_hours_in_one_stable_generation(
        generations,
        segments.iter().map(|seg| seg.ingest_hour_bucket),
        DEFAULT_SCAN_SLACK_HOURS,
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use ravel_catalog::SegmentLevel;
    use ravel_proto::queryfrag::v1 as pb;
    use uuid::Uuid;

    use super::*;
    use crate::distrib::client::{DistribError, SliceFetcher, SliceResponse};
    use crate::distrib::federation::RemoteCluster;

    fn sg(generation: u32, shard_count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation,
            shard_count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    /// A `SegmentRef` carrying only the fields this gate reads
    /// (`ingest_hour_bucket`, and `shard` for realism); nothing here fetches
    /// the object, so the identity fields are placeholders.
    fn seg(ingest_hour_bucket: u32, shard: u32) -> SegmentRef {
        SegmentRef {
            data_object_key: format!("t/aa/m/l0/{shard:04}/w.0.{ingest_hour_bucket:020}.x.rseg"),
            object_size: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 1,
            ingest_hour_bucket,
            sample_count: 1,
            series_count: 1,
            shard,
            content_hash: [0u8; 32],
            writer_id: Uuid::nil(),
            writer_epoch: 0,
            writer_seq: 0,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        }
    }

    /// A remote that is never dispatched to: the federation exclusion is
    /// decided by the presence of a `Federation`, not by any remote's behavior.
    struct NeverCalledFetcher;

    #[async_trait]
    impl SliceFetcher for NeverCalledFetcher {
        async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
            unreachable!("the eligibility gate never dispatches a fetch")
        }
    }

    fn federation() -> Federation {
        Federation::new(vec![RemoteCluster {
            name: "eu-west".to_string(),
            fetcher: Arc::new(NeverCalledFetcher),
            skip_unavailable: false,
            soft_timeout: Duration::from_secs(1),
        }])
    }

    /// Decision 1(a): a federated query is ineligible unconditionally, even
    /// when its segments and generation history would otherwise pass on their
    /// own (proved by the `None` federation case on the same inputs).
    #[test]
    fn federated_query_is_never_eligible() {
        let gens = [sg(0, 4, 0)];
        let segments = [seg(10, 0), seg(11, 1)];
        assert!(
            is_pushdown_eligible(None, &segments, &gens),
            "these inputs are eligible when not federated"
        );
        assert!(!is_pushdown_eligible(Some(&federation()), &segments, &gens));
        // Also ineligible for input shapes that would fail 1(b) anyway, and for
        // an empty snapshot: 1(a) short-circuits before either is consulted.
        let split = [seg(10, 0), seg(500, 1)];
        let two_gens = [sg(0, 4, 0), sg(1, 8, 100)];
        assert!(!is_pushdown_eligible(
            Some(&federation()),
            &split,
            &two_gens
        ));
        assert!(!is_pushdown_eligible(Some(&federation()), &[], &gens));
    }

    /// Decision 1(b), the trivial case that must stay fast: a tenant that never
    /// resharded has one generation, so every resolved hour is owned by it and
    /// every query is eligible.
    #[test]
    fn single_generation_is_eligible() {
        let gens = [sg(0, 4, 0)];
        let segments = [seg(0, 0), seg(1, 3), seg(9_999, 2), seg(u32::MAX, 1)];
        assert!(is_pushdown_eligible(None, &segments, &gens));
    }

    /// Decision 1(b): resolved segments spanning two generations' stable
    /// intervals are ineligible. `shard_for` is not constant across them, so
    /// one series can carry two shard values and land on two workers.
    #[test]
    fn segments_spanning_two_generations_are_ineligible() {
        // gen0 count 4 @0, gen1 count 8 @100, S = 3: gen0's stable interval is
        // [0, 100), gen1's is [103, inf).
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        let same_generation = [seg(50, 0), seg(99, 1)];
        assert!(
            is_pushdown_eligible(None, &same_generation, &gens),
            "both hours inside gen0's stable interval"
        );
        let spanning = [seg(99, 1), seg(200, 5)];
        assert!(
            !is_pushdown_eligible(None, &spanning, &gens),
            "hour 99 is gen0's, hour 200 is gen1's"
        );
        // Order is irrelevant: the gate is a set property.
        let spanning_reversed = [seg(200, 5), seg(99, 1)];
        assert!(!is_pushdown_eligible(None, &spanning_reversed, &gens));
    }

    /// Decision 1(b): every resolved hour inside ONE generation's stable
    /// interval is eligible, including the post-reshard generation (a tenant
    /// that resharded long ago is not disqualified forever, only for the
    /// queries whose scan set touches the boundary).
    #[test]
    fn all_segments_in_one_stable_interval_are_eligible() {
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        let after = [seg(103, 0), seg(150, 7), seg(9_000, 3)];
        assert!(is_pushdown_eligible(None, &after, &gens));
        let before = [seg(0, 0), seg(99, 3)];
        assert!(is_pushdown_eligible(None, &before, &gens));
    }

    /// Decision 1(b): a single segment from inside a boundary's slack margin
    /// disqualifies the query on its own, even though every resolved hour maps
    /// to the same *shard count*. ADR-0052's scan rule still scans the retiring
    /// generation's shards for that hour, so the hour is genuinely ambiguous.
    #[test]
    fn one_segment_inside_the_slack_margin_is_ineligible() {
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        for hour in 100..100 + DEFAULT_SCAN_SLACK_HOURS {
            assert!(
                !is_pushdown_eligible(None, &[seg(hour, 0)], &gens),
                "hour {hour} is inside gen1's slack margin"
            );
            assert!(
                !is_pushdown_eligible(None, &[seg(150, 0), seg(hour, 1)], &gens),
                "one ambiguous hour {hour} disqualifies an otherwise-stable set"
            );
        }
        assert!(
            is_pushdown_eligible(None, &[seg(100 + DEFAULT_SCAN_SLACK_HOURS, 0)], &gens),
            "the first hour past the margin is gen1's alone"
        );
    }

    /// An empty resolved segment set is ineligible: there is nothing to push
    /// down, and the raw-fetch path handles an empty snapshot identically.
    #[test]
    fn empty_segment_set_is_ineligible() {
        let gens = [sg(0, 4, 0)];
        assert!(!is_pushdown_eligible(None, &[], &gens));
        assert!(!all_hours_in_one_stable_generation(
            &gens,
            std::iter::empty(),
            DEFAULT_SCAN_SLACK_HOURS
        ));
    }

    /// An empty (never-validated) generation history owns no hour, so it is
    /// ineligible rather than treated as "assume generation 0".
    #[test]
    fn empty_generation_history_is_ineligible() {
        assert!(!is_pushdown_eligible(None, &[seg(10, 0)], &[]));
    }

    /// `all_hours_in_one_stable_generation` is the reshard half on its own,
    /// including duplicate hours (many segments per hour, the common case) and
    /// an explicit slack argument.
    #[test]
    fn hours_check_is_independent_of_multiplicity_and_slack() {
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        let s = DEFAULT_SCAN_SLACK_HOURS;
        assert!(all_hours_in_one_stable_generation(
            &gens,
            [50, 50, 50, 99],
            s
        ));
        assert!(!all_hours_in_one_stable_generation(&gens, [50, 50, 100], s));
        // With no slack the margin vanishes, so the activation hour itself is
        // gen1's alone. Pins that the slack window is the argument's, not a
        // hard-coded constant inside the check.
        assert!(all_hours_in_one_stable_generation(&gens, [100, 150], 0));
        assert!(!all_hours_in_one_stable_generation(&gens, [99, 100], 0));
    }
}
