//! Per-query I/O shape (issue #1214): the dependency structure between a
//! query's object-store stages, kept separate from `phase_accounting`'s cost
//! split. `phase_accounting` answers "how many requests and bytes did each
//! phase spend"; this module answers "how many of those requests had to run
//! one after another because a later stage needed an earlier stage's bytes
//! to know its own keys." A query can be cheap by [`crate::phase_accounting`]
//! (few requests, few bytes) and still be slow because its requests chain,
//! or expensive by request count and still be fast because every request
//! runs in one parallel round. Neither dimension predicts the other, which is
//! exactly why both are reported.
//!
//! # What is knowable from `ravel-query`, and what is not
//!
//! The per-segment fetch pipeline this crate owns
//! (`SegmentFetcher::open_segment` -> `decode_selected` -> `fetch_scalar_pages`
//! / `fetch_histogram_pages`, `crate::fetcher`) is visible here, so
//! [`dependency_depth`](QueryIoShape::dependency_depth) reflects it: whether a
//! segment resolves in one whole-object GET (depth 1) or needs a
//! footer-tail-then-dependent-fetch sequence, reported as an upper bound of
//! depth 3 to cover a possible footer-range chase (`depth_for_object` below),
//! classified structurally from `SegmentRef::object_size` against
//! `SegmentFetcher::whole_object_threshold`, the same size test `open_segment`
//! itself branches on. This mirrors `CostEstimate`'s existing convention: an
//! upper envelope, not a per-request trace, because tracing the real request
//! graph would require instrumenting every private call inside the fetch
//! pipeline for a benefit this structural classification already gets from a
//! field the snapshot already carries.
//!
//! What is NOT knowable from this crate: the catalog snapshot resolve's own
//! internal dependency chain (`Catalog::resolve_pruned_with_generations` and
//! the snapshot-window HEAD-then-part-GET sequence it can take) lives
//! entirely inside `ravel-catalog`, which this task's scope excludes editing.
//! [`dependency_depth`] therefore reports only the segment-fetch chain's
//! depth; the true end-to-end depth (resolve's own chain, however deep,
//! prefixed to the segment-fetch chain) can only be equal to or greater than
//! what is reported here. Likewise LIST pagination is entirely internal to
//! `ravel-catalog`'s per-shard bounded listing (`Catalog::list_shard_hours`):
//! [`list_page_depth`](QueryIoShape::list_page_depth) reports the resolve
//! phase's total LIST request count as an upper-bound serial depth, because
//! this crate cannot see whether two shards' LIST pages actually ran
//! concurrently with each other or serially -- only that they happened.
//!
//! # Fields
//!
//! - [`dependency_depth`](QueryIoShape::dependency_depth): the longest chain
//!   of object-store stages, across every segment this query opened, where a
//!   stage needs a previous stage's bytes to know its own keys. Four
//!   independent segment GETs report depth 1 (they run concurrently, none
//!   depends on another); a segment whose footer read is followed by a
//!   dependent page fetch reports depth 3, an upper bound covering a
//!   possible footer-range chase this crate cannot rule out from
//!   `object_size` alone (see `depth_for_object`).
//! - [`list_page_depth`](QueryIoShape::list_page_depth): serial LIST pages,
//!   recorded separately from `dependency_depth` because pagination
//!   (`Catalog::list_shard_hours`) and the GET dependency chain are different
//!   object-store mechanisms with different causes.
//! - [`service_batches`](QueryIoShape::service_batches): batches this query's
//!   per-segment fan-out was forced into by its concurrency permit
//!   (`ceil(segment_count / fetch_concurrency)`): 64 segments under 16
//!   concurrent permits is 4 batches at whatever depth those segments'
//!   fetches classify at.
//! - [`unfolded_segments_resolved`](QueryIoShape::unfolded_segments_resolved):
//!   the EXACT count of segments this query's resolve took from the recent
//!   (unfolded) listing path (`SegmentOrigin::Recent`) rather than a folded
//!   snapshot part. Exact, not estimated, because it gates a downstream
//!   fold-benefit decision that an approximation would silently corrupt. A
//!   SEGMENT count, not a commit-record count: `SegmentOrigin` is parallel to
//!   `Snapshot::segments` (one entry per resolved `SegmentRef`), and one L1
//!   compaction record can produce several `SegmentRef` parts
//!   (`SegmentLevel::L1`'s `part_index`s) that all inherit that record's
//!   `Recent` origin. The two counts coincide for L0 (one commit record names
//!   one segment) and diverge for a multi-part L1 compaction, where this
//!   figure counts every part. `ravel-query` cannot recover the underlying
//!   record count from `SegmentOrigin` alone -- the field is named, and
//!   documented, for the quantity it actually counts.
//! - [`plan_class`](QueryIoShape::plan_class): decided before any segment is
//!   opened, from the query's shape and the resolve's own pruning outcome.
//!
//! # Tests
//!
//! Unit tests live in this module (`tests` submodule below) and exercise
//! [`IoShapeCounts`] directly: they construct chained versus parallel
//! recording sequences and assert on the resulting [`IoShapeCounts`], with no
//! dependency on a live catalog or object store.

use ravel_catalog::SegmentOrigin;

/// Coarse pre-execution classification of a query's expected access pattern,
/// decided once, before any segment is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanClass {
    /// No page fetch: a labels/label-values/series discovery query, which
    /// opens and catalog-decodes segments but never reaches
    /// `fetch_scalar_pages`/`fetch_histogram_pages`.
    MetadataOnly,
    /// The resolve's postings-based pruning excluded at least one
    /// snapshot-sourced segment (`Snapshot::segments_pruned > 0`): the fetch
    /// touches a strict subset of the window's listed segments.
    SelectiveIndexed,
    /// Every listed segment in the resolved window is opened: no pruning
    /// narrowed the set (or none was possible for this query shape).
    ExhaustiveScan,
    /// The lane's own resolve carries no signal this crate can use to tell
    /// a pruned fetch from a full scan (the log lane's `resolve_bounded`
    /// call always passes `name_filter: None`, so `Snapshot::segments_pruned`
    /// is structurally always 0 for it, regardless of whether the resolved
    /// window is actually narrow). Reporting `ExhaustiveScan` here would be
    /// fabricated: this lane genuinely does not know, and the honest
    /// classification says so rather than defaulting to the most severe
    /// value. See [`merge_plan_class`] for how this ranks against a lane
    /// that DOES know.
    Unclassified,
}

impl PlanClass {
    /// Stable lowercase-snake-case name, matching `QueryPhase::name`'s
    /// rendering convention for the JSON surface.
    pub fn name(self) -> &'static str {
        match self {
            PlanClass::MetadataOnly => "metadata_only",
            PlanClass::SelectiveIndexed => "selective_indexed",
            PlanClass::ExhaustiveScan => "exhaustive_scan",
            PlanClass::Unclassified => "unclassified",
        }
    }
}

/// Recorded per query: the dependency shape of its object-store I/O,
/// alongside (never instead of) `phase_accounting`'s request/byte cost split.
/// See the module docs above for what each field means and its known
/// limitations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryIoShape {
    pub dependency_depth: u32,
    pub list_page_depth: u32,
    pub service_batches: u32,
    pub unfolded_segments_resolved: u64,
    pub plan_class: PlanClass,
}

impl Default for QueryIoShape {
    /// `PlanClass` has no natural zero value, but `Default` is also what a
    /// lane that never ran (no metric selector in a log-only query, or vice
    /// versa) contributes before [`merge_plan_class`] combines it with the
    /// lane that did run. `MetadataOnly` is the correct identity for that
    /// merge -- it never outranks a real lane's classification -- whereas
    /// `ExhaustiveScan` would wrongly force every literal-only query (no
    /// storage touched at all) to report the plan class of a full scan.
    fn default() -> Self {
        QueryIoShape {
            dependency_depth: 0,
            list_page_depth: 0,
            service_batches: 0,
            unfolded_segments_resolved: 0,
            plan_class: PlanClass::MetadataOnly,
        }
    }
}

/// Combines two lanes' plan classes (a federated query can run a metrics
/// lane and a log lane, each independently classified) into the single
/// value `QueryIoShape` reports. Ranked: `MetadataOnly` (0) < `Unclassified`
/// (1) < `SelectiveIndexed` (2) < `ExhaustiveScan` (3), most severe wins.
/// `Unclassified` sits strictly between the never-ran identity and either
/// real scan classification: a lane that genuinely resolved segments but
/// cannot say whether pruning narrowed them is worse than a lane that never
/// touched storage, but merging it against a lane that DID classify itself
/// must never silently promote a real `SelectiveIndexed` result up to
/// `ExhaustiveScan` -- that would be exactly the fabrication this value
/// exists to avoid. This is the same conservative direction as
/// `CostEstimate`'s upper-envelope convention otherwise: if either lane
/// touched an unpruned full window, the combined figure says so rather than
/// averaging it away against a lane that pruned well.
pub fn merge_plan_class(a: PlanClass, b: PlanClass) -> PlanClass {
    fn rank(c: PlanClass) -> u8 {
        match c {
            PlanClass::MetadataOnly => 0,
            PlanClass::Unclassified => 1,
            PlanClass::SelectiveIndexed => 2,
            PlanClass::ExhaustiveScan => 3,
        }
    }
    if rank(a) >= rank(b) { a } else { b }
}

/// Structural `dependency_depth` classification for one segment's fetch
/// pipeline, from the same size test `SegmentFetcher::open_segment` branches
/// on. A segment strictly between 0 and the whole-object threshold resolves
/// everything (footer, catalog, pages) from its first GET: depth 1, no later
/// stage depends on this one's output to name its own keys.
///
/// Every other case reports depth 3, as an upper bound rather than a single
/// true value, because this function only has `(object_size,
/// whole_object_threshold)` to go on and `open_segment`'s real chain length
/// past the whole-object branch depends on state this crate cannot see from
/// those two numbers alone:
/// - `object_size == 0` (size unknown ahead of the fetch) takes a
///   `GetRange::Suffix` read (a footer-tail guess, not a whole-object read),
///   which is then followed by dependent page GETs: at least 2 stages, and a
///   `FooterOutcome::NeedRange` on that suffix guess (the tail didn't reach
///   far enough back to cover the real footer) inserts one more dependent
///   footer-range GET before the page fetch, making 3.
/// - `object_size > whole_object_threshold` takes the same footer-tail path
///   and is subject to the same possible `NeedRange` chase, for the same
///   2-or-3 range.
///
/// Neither branch can be resolved to an exact depth here: whether the tail
/// guess (`SegmentFetcher`'s private `suffix_len`) covers a given segment's
/// real footer size is not observable from this function's inputs, and no
/// public accessor exposes it. Reporting the upper bound (3) rather than the
/// optimistic case (2) matches the module's existing "never underestimate"
/// convention (see `CostEstimate`'s upper envelope): a chain this function
/// undercounts would silently understate how serial a query's fetch plan
/// is, which is the failure mode this field exists to surface.
///
/// This also ignores in-memory region reuse and cache warmth by design: a
/// fully-cached large segment still reports its structural depth, because
/// the *plan* has that many dependent stages even when a cache absorbs one.
pub fn depth_for_object(object_size: u64, whole_object_threshold: u64) -> u32 {
    if object_size != 0 && object_size <= whole_object_threshold {
        1
    } else {
        3
    }
}

/// Counts `SegmentOrigin::Recent` entries: segments this resolve took from
/// the recent (unfolded) listing path rather than a folded snapshot part.
/// `origins` is parallel to `Snapshot::segments` (one entry per resolved
/// `SegmentRef`), so this counts SEGMENTS, not commit records -- see the
/// module docs' `unfolded_segments_resolved` entry for when a segment and its
/// underlying record diverge (a multi-part L1 compaction).
///
/// Deliberately excludes `SegmentOrigin::SealedBelowWatermark` (already
/// folded: the quantity this figure gates is unfolded exposure, and a sealed
/// segment carries none) and `SegmentOrigin::TokenResolved`. The
/// `TokenResolved` exclusion is a deliberate cost-shape decision, not merely
/// "not a listing outcome": a token-resolved segment costs exactly one GET by
/// its explicit `min_commit_token` key (`Catalog::resolve_min_token`)
/// regardless of whether a fold has run, so folding can never remove that
/// GET. Counting it here would overstate the fold-benefit figure with a cost
/// no fold could ever recover. A `Recent` segment's cost is different: it is
/// discovered by the listing path specifically because no fold covers it
/// yet, so folding it into a snapshot part is exactly the benefit this
/// figure gates. An exact count over the full `origins` slice, never an
/// estimate.
pub fn count_unfolded_segments(origins: &[SegmentOrigin]) -> u64 {
    origins
        .iter()
        .filter(|o| matches!(o, SegmentOrigin::Recent))
        .count() as u64
}

/// Batches one concurrency-bounded fan-out over `item_count` items is forced
/// into under `concurrency` simultaneous permits: `ceil(item_count /
/// concurrency)`. `concurrency` is clamped to at least 1 (mirrors every fetch
/// call site's own `self.config.fetch_concurrency.max(1)`), so this never
/// divides by zero.
pub fn service_batches(item_count: u64, concurrency: u64) -> u32 {
    let concurrency = concurrency.max(1);
    item_count.div_ceil(concurrency).min(u64::from(u32::MAX)) as u32
}

/// Accumulates the three fan-out-shaped `QueryIoShape` fields
/// (`dependency_depth`, `list_page_depth`, `service_batches`) across however
/// many independent stage chains and fan-out rounds one query's resolve and
/// fetch produce. Each field takes the MAXIMUM ever recorded, never a sum: a
/// query's `dependency_depth` is the length of its single longest chain, not
/// the total number of stages across every chain that ran (most of which ran
/// concurrently with each other, not after each other).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoShapeCounts {
    pub dependency_depth: u32,
    pub list_page_depth: u32,
    pub service_batches: u32,
}

impl IoShapeCounts {
    /// Folds in one independent stage chain's depth: keeps the larger of the
    /// current recorded depth and `depth`.
    pub fn record_dependency_chain(&mut self, depth: u32) {
        self.dependency_depth = self.dependency_depth.max(depth);
    }

    /// Folds in one LIST call's serial page count, independently of
    /// `dependency_depth`.
    pub fn record_list_pages(&mut self, pages: u32) {
        self.list_page_depth = self.list_page_depth.max(pages);
    }

    /// Folds in one concurrency-bounded fan-out's forced batch count.
    pub fn record_service_batches(&mut self, batches: u32) {
        self.service_batches = self.service_batches.max(batches);
    }

    /// Combines these fan-out-shaped counts with the two values only
    /// knowable directly at the resolve call site into a complete
    /// [`QueryIoShape`].
    pub fn into_shape(
        self,
        unfolded_segments_resolved: u64,
        plan_class: PlanClass,
    ) -> QueryIoShape {
        QueryIoShape {
            dependency_depth: self.dependency_depth,
            list_page_depth: self.list_page_depth,
            service_batches: self.service_batches,
            unfolded_segments_resolved,
            plan_class,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_catalog::{DeclaredColumnStats, SegmentLevel, SegmentOrigins, SegmentRef, Snapshot};
    use uuid::Uuid;

    /// A chained stage sequence (a HEAD-shaped stage whose bytes name the
    /// next stage's keys, repeated) must report a strictly greater
    /// `dependency_depth` than a parallel fan-out issuing the same total
    /// request count, because `IoShapeCounts` takes the max of recorded
    /// chain depths, never a sum. Flip `.max(depth)` in
    /// `record_dependency_chain` to `+= depth` to watch this fail: both
    /// counts land in the same bucket and the assertion that chained must
    /// exceed parallel breaks (parallel: 1+1+1+1=4, chained: 1+2+3+4=10, so a
    /// summing bug would even invert which one this test expects to be
    /// larger were the chain shorter than the fan-out -- the `assert_eq!`
    /// pins on the exact depth, not just an inequality, so a `+=` bug is
    /// caught either way).
    #[test]
    fn depth_counts_chained_stages_not_parallel_batches() {
        let mut parallel = IoShapeCounts::default();
        // Four independent segment GETs: none depends on another, so each
        // chain recorded is depth 1.
        for _ in 0..4 {
            parallel.record_dependency_chain(1);
        }

        let mut chained = IoShapeCounts::default();
        // One chain of four dependent stages (e.g. a footer read, then three
        // successively-dependent page fetches): depths 1, 2, 3, 4 recorded
        // as the chain grows, same total request count as `parallel`.
        for depth in 1..=4 {
            chained.record_dependency_chain(depth);
        }

        assert_eq!(parallel.dependency_depth, 1, "independent GETs never chain");
        assert_eq!(
            chained.dependency_depth, 4,
            "the chain's own longest depth survives"
        );
        assert!(
            chained.dependency_depth > parallel.dependency_depth,
            "a chained stage sequence must report strictly greater dependency_depth \
             than a parallel fan-out issuing the same number of requests"
        );
    }

    /// `list_page_depth` must be reported independently of
    /// `dependency_depth`: a query with several serial LIST pages and a flat
    /// (unchained) GET fan-out reports high `list_page_depth` and low
    /// `dependency_depth`. Flip `record_list_pages` to update
    /// `self.dependency_depth` instead of `self.list_page_depth` to watch
    /// this fail: `dependency_depth` would then read 13, not 1.
    #[test]
    fn list_page_depth_reported_independently_of_dependency_depth() {
        let mut counts = IoShapeCounts::default();
        // 13 serial LIST pages (the reference S3 run this task investigates:
        // 4 shards paginated at 1000 records/page plus one pending-erasure
        // LIST), against a flat 4-way parallel GET fan-out.
        counts.record_list_pages(13);
        for _ in 0..4 {
            counts.record_dependency_chain(1);
        }

        assert_eq!(counts.list_page_depth, 13);
        assert_eq!(counts.dependency_depth, 1);
        assert!(counts.list_page_depth > counts.dependency_depth);
    }

    /// `depth_for_object` classifies structurally from the same size test
    /// `SegmentFetcher::open_segment` branches on: strictly between 0 and
    /// the threshold is a whole-object read (depth 1); every other case --
    /// including `object_size == 0`, which `open_segment` sends down the
    /// `GetRange::Suffix` footer-tail path, not the whole-object path --
    /// reports the upper bound 3, because a `FooterOutcome::NeedRange` chase
    /// on the tail guess is a real third stage this function cannot rule
    /// out from `object_size` alone. Flip the `object_size != 0 &&` guard
    /// off (restoring the old `object_size <= threshold` test alone) to
    /// watch the `object_size == 0` case fail: it would wrongly report 1
    /// instead of 3.
    #[test]
    fn depth_for_object_matches_whole_object_threshold_branch() {
        assert_eq!(
            depth_for_object(1_000, 2_000),
            1,
            "strictly between 0 and threshold: whole object"
        );
        assert_eq!(
            depth_for_object(2_000, 2_000),
            1,
            "exactly at threshold: whole object"
        );
        assert_eq!(
            depth_for_object(2_001, 2_000),
            3,
            "above threshold: footer tail, upper-bounded for a possible NeedRange chase"
        );
        assert_eq!(
            depth_for_object(0, 2_000),
            3,
            "unknown size takes the suffix footer-tail path, not whole-object, \
             even though 0 <= threshold"
        );
    }

    /// `count_unfolded_segments` counts `Recent` origins EXACTLY, excluding
    /// both `SealedBelowWatermark` (already folded) and `TokenResolved` (its
    /// GET cost survives a fold, so it is not fold-benefit exposure). Pinned
    /// to an exact figure, not `> 0`: this count gates a downstream
    /// fold-benefit decision that a merely-nonzero check would not catch an
    /// under-count on.
    #[test]
    fn unfolded_segments_resolved_counts_recent_origins_exactly() {
        let origins = vec![
            SegmentOrigin::SealedBelowWatermark,
            SegmentOrigin::Recent,
            SegmentOrigin::Recent,
            SegmentOrigin::TokenResolved,
            SegmentOrigin::Recent,
            SegmentOrigin::SealedBelowWatermark,
        ];
        assert_eq!(count_unfolded_segments(&origins), 3);
    }

    /// A deliberate under-count: folding `TokenResolved` into the `Recent`
    /// tally too (as if the filter were `!= SealedBelowWatermark` instead of
    /// `== Recent`) would report 4, not 3, on the same fixture -- this test
    /// pins the exact figure so that regression is visible, not just "some
    /// segments counted."
    #[test]
    fn unfolded_segments_resolved_excludes_token_resolved() {
        let origins = vec![SegmentOrigin::TokenResolved, SegmentOrigin::Recent];
        assert_eq!(count_unfolded_segments(&origins), 1);
    }

    /// Pins the segments-versus-records divergence the field name and doc
    /// comment now spell out explicitly, against REAL `ravel-catalog` types
    /// rather than a bare `Vec<SegmentOrigin>` with no `Snapshot` behind it:
    /// a `Snapshot` holding one L0 `SegmentRef` plus a 3-part L1 compaction
    /// (`SegmentLevel::L1` at `part_index` 0, 1, 2, sharing one
    /// `input_set_hash` -- the real shape one compaction record fans out
    /// into) has 4 `segments` entries from only 2 underlying commit/
    /// compaction records. `SegmentOrigins::origins` is built parallel to
    /// `snapshot.segments` (same length, same order, all `Recent`, matching
    /// how a real live-listing resolve populates both), and the assertion
    /// checks that parallelism explicitly before checking the count, so a
    /// carriage bug that let the two vectors drift out of sync would fail
    /// here even before the count assertion. Flip the L1 fixture down to 1
    /// part (drop two of the three `part_index` entries and their origins)
    /// to watch the count assertion fail: it would then read 2, not 4.
    #[test]
    fn unfolded_segments_resolved_counts_compaction_parts_not_records() {
        fn segment_ref(level: SegmentLevel) -> SegmentRef {
            SegmentRef {
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
                level,
                segment_format_version: 1,
                declared_column_stats: DeclaredColumnStats::default(),
            }
        }

        let input_set_hash = [7u8; 32];
        let snapshot = Snapshot {
            segments: vec![
                // 1 underlying L0 commit record -> 1 segment.
                segment_ref(SegmentLevel::L0),
                // 1 underlying L1 compaction record, fanned out into 3
                // parts -> 3 segments.
                segment_ref(SegmentLevel::L1 {
                    input_set_hash,
                    part_index: 0,
                }),
                segment_ref(SegmentLevel::L1 {
                    input_set_hash,
                    part_index: 1,
                }),
                segment_ref(SegmentLevel::L1 {
                    input_set_hash,
                    part_index: 2,
                }),
            ],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        let mut origins = SegmentOrigins::default();
        for _ in &snapshot.segments {
            origins.push(SegmentOrigin::Recent);
        }

        assert_eq!(
            origins.origins.len(),
            snapshot.segments.len(),
            "origins must stay parallel to snapshot.segments"
        );
        assert_eq!(
            count_unfolded_segments(&origins.origins),
            4,
            "4 segments (1 L0 + 3 L1 parts) from only 2 underlying commit/compaction records"
        );
    }

    #[test]
    fn service_batches_rounds_up_and_clamps_concurrency() {
        assert_eq!(service_batches(64, 16), 4);
        assert_eq!(
            service_batches(65, 16),
            5,
            "one leftover item forces another batch"
        );
        assert_eq!(
            service_batches(4, 0),
            4,
            "zero concurrency clamps to 1 permit"
        );
    }

    /// `merge_plan_class` picks the more severe scan class, in either
    /// argument order, and a lane that never ran (`MetadataOnly`, the
    /// `Default`) never outranks a lane that actually scanned.
    #[test]
    fn merge_plan_class_picks_more_severe_scan() {
        assert_eq!(
            merge_plan_class(PlanClass::MetadataOnly, PlanClass::SelectiveIndexed),
            PlanClass::SelectiveIndexed
        );
        assert_eq!(
            merge_plan_class(PlanClass::ExhaustiveScan, PlanClass::SelectiveIndexed),
            PlanClass::ExhaustiveScan
        );
        assert_eq!(
            merge_plan_class(PlanClass::SelectiveIndexed, PlanClass::ExhaustiveScan),
            PlanClass::ExhaustiveScan
        );
        assert_eq!(
            merge_plan_class(PlanClass::MetadataOnly, PlanClass::MetadataOnly),
            PlanClass::MetadataOnly
        );
    }
}
