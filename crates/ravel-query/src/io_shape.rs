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
//! depth 4 to cover a possible footer-range chase plus the catalog and page
//! fetch stages (`depth_for_object` below),
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
//!   dependent catalog fetch and a dependent page fetch reports depth 4, an
//!   upper bound covering a possible footer-range chase this crate cannot
//!   rule out from `object_size` alone (see `depth_for_object`).
//! - [`list_page_depth`](QueryIoShape::list_page_depth): serial LIST pages,
//!   recorded separately from `dependency_depth` because pagination
//!   (`Catalog::list_shard_hours`) and the GET dependency chain are different
//!   object-store mechanisms with different causes.
//! - [`service_batches`](QueryIoShape::service_batches): a LOWER bound on
//!   the serial service rounds this query's per-segment fan-out needed,
//!   under a wave-synchronous model of the nested fan-out
//!   (`crates/ravel-query/src/engine.rs`) with three levels of admission,
//!   all of which bound the round count:
//!   1. the OUTER `buffer_unordered(promql_fetch_fanout)` over distinct
//!      matcher plans (`distinct_plans_by_matcher`), which admits at most
//!      `promql_fetch_fanout` plans at once -- never
//!      `service_fetch_multiplier` plans, however many distinct matcher sets
//!      the query has;
//!   2. each admitted plan's own INNER
//!      `buffer_unordered(promql_fetch_fanout)` over that plan's segments;
//!   3. the one [`crate::GetLimiter`] the engine shares across every fetcher
//!      it owns (ADR-1195), sized by the resolved
//!      `EngineConfig::store_get_concurrency` (see "GET concurrency" in
//!      docs/query-engine.md), which every GET from every plan passes
//!      through regardless of which plan or segment issued it.
//!
//!   It is a lower bound, never an upper bound or an exact count, because
//!   the OUTER `buffer_unordered` is a SLIDING window, not a wave-synchronous
//!   one: as soon as one admitted plan finishes, the next plan is admitted
//!   immediately, without waiting for its siblings. A model that assumed the
//!   whole outer window empties and refills together (`ceil(segment_count *
//!   service_fetch_multiplier / min(promql_fetch_fanout *
//!   min(service_fetch_multiplier, promql_fetch_fanout),
//!   shared_get_permits))`, dividing TOTAL work by PEAK capacity) undercounts
//!   whenever the outer window is not full for the query's entire duration:
//!   3 distinct plans, 1 segment each, `promql_fetch_fanout` 2, and a
//!   1000-permit limiter gives peak capacity `min(2 * 2, 1000) = 4`, so that
//!   model reports `ceil(3 / 4) = 1` round -- but plans 1 and 2 admit
//!   together in the first round, and plan 3 is only admitted after one of
//!   them finishes, in a second round: the real scheduler takes at least 2
//!   rounds, never fewer than the wave-synchronous model computes, because a
//!   sliding window can only pack work as tightly as a synchronized one, not
//!   more loosely.
//!
//!   So this figure is computed per WAVE of the outer fan-out instead of
//!   once over the total: `waves = ceil(service_fetch_multiplier /
//!   promql_fetch_fanout)`. For 0-based wave `w`, `active_w =
//!   min(promql_fetch_fanout, service_fetch_multiplier - w *
//!   promql_fetch_fanout)` is the count of distinct plans admitted in that
//!   wave, `capacity_w = min(promql_fetch_fanout * active_w,
//!   shared_get_permits)` is that wave's binding concurrency, and `batches_w
//!   = ceil(segment_count * active_w / capacity_w)` is the serial rounds
//!   that wave alone needs. `service_batches` sums `batches_w` over every
//!   wave: because the outer stream never admits a plan from wave `w + 1`
//!   until wave `w`'s plans have all been admitted (though not necessarily
//!   finished -- see the sliding-window caveat above, which is exactly why
//!   this sum is a lower rather than an exact bound), the waves are at least
//!   this serial, and their round counts add.
//!
//!   Reviewer's case above: 2 waves. Wave 0: `active_0 = min(2, 3) = 2`,
//!   `capacity_0 = min(2 * 2, 1000) = 4`, `batches_0 = ceil(1 * 2 / 4) = 1`.
//!   Wave 1: `active_1 = min(2, 1) = 1`, `capacity_1 = min(2 * 1, 1000) =
//!   2`, `batches_1 = ceil(1 * 1 / 2) = 1`. Sum: 2, matching the real
//!   schedule. 64 segments, `promql_fetch_fanout` 8, 2 distinct matcher
//!   sets, and 16 shared permits: one wave (`ceil(2 / 8) = 1`), `active_0 =
//!   2`, `capacity_0 = min(16, 16) = 16`, `batches_0 = ceil(128 / 16) = 8`.
//!   `promql_fetch_fanout` 1 with 3 distinct matcher sets, 20 segments, and
//!   1000 permits: three waves (`ceil(3 / 1) = 3`), each `active_w = 1`,
//!   `capacity_w = min(1, 1000) = 1`, `batches_w = ceil(20 / 1) = 20`, sum
//!   60. A single-plan query (`service_fetch_multiplier == 1`) always has
//!   exactly one wave with `active_0 = 1`, so it reduces to
//!   `ceil(segment_count / min(promql_fetch_fanout, shared_get_permits))`,
//!   the same figure the un-waved formula already gave for that case.
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
//! - [`unfolded_segments_served_from_cache`](QueryIoShape::unfolded_segments_served_from_cache):
//!   of the segments `unfolded_segments_resolved` counts, how many came from
//!   a commit record already sitting in `ravel-catalog`'s local record cache
//!   rather than fetched fresh. Derived from this resolve's OWN
//!   `QueryAccounting` handle's cache hit/miss delta, never a pooled process
//!   counter: a `PhaseAccounting::resolve()` handle is built fresh per query
//!   attempt, so its counters ARE that attempt's resolve-phase delta, with
//!   nothing to subtract. See [`unfolded_segments_served_from_cache`] (the
//!   function) for the exact derivation and the shape it is proven for. Like
//!   `unfolded_segments_resolved`, this is a SEGMENT figure gated by a
//!   commit-RECORD-level cache counter -- the two coincide for the all-L0
//!   unsealed tail this derivation covers, and diverge exactly where
//!   `unfolded_segments_resolved` already says a segment and its record
//!   diverge (a multi-part L1 compaction). The derivation additionally
//!   assumes this tenant/signal has no snapshot HEAD from an earlier fold
//!   (see the function's own doc comment, "The constant decoded-HEAD
//!   probe"); when one exists, the figure can read up to 1 away from the
//!   true count. This field does not decide cold/warm; see
//!   docs/query-engine.md for that classification rule and why it lives
//!   with the reader of both figures, not here.
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
    pub unfolded_segments_served_from_cache: u64,
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
            unfolded_segments_served_from_cache: 0,
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
/// Every other case reports depth 4, as an upper bound rather than a single
/// true value, because this function only has `(object_size,
/// whole_object_threshold)` to go on and `open_segment`'s real chain past
/// the whole-object branch can stack up to four dependent stages, each
/// needing the previous stage's bytes to know its own keys
/// (`crate::fetcher`):
/// 1. **Footer tail**: a `GetRange::Suffix` (size unknown) or a tail
///    `GetRange::Range` (size known but over threshold) guess at the
///    footer (`SegmentFetcher::open_segment`).
/// 2. **`FooterOutcome::NeedRange` chase**: only when the tail guess didn't
///    reach far enough back to cover the real footer, a second dependent
///    GET at the exact offset the first GET's bytes revealed
///    (`open_segment`).
/// 3. **Catalog `ensure_ranges`**: `decode_selected` fetches LABEL_DICT /
///    SERIES_IDS / SERIES_META (or the sparse SERIES_IDX / chunked variant)
///    from the offsets the footer named; these sections sit at the object
///    front, never inside the footer-tail bytes already in hand, so this is
///    a separate dependent GET whenever the footer stage's regions don't
///    already cover them.
/// 4. **Page `ensure_ranges`**: `fetch_scalar_pages` /
///    `fetch_histogram_pages` fetch the run page ranges the catalog decode
///    just named; same reasoning, a separate dependent GET unless already
///    covered.
///
/// Stage 1 always runs; stages 2-4 are each conditional on whether an
/// earlier GET's bytes happen to already cover the next stage's ranges
/// (`FetchedRegions::covers`), which this function cannot observe from
/// `object_size` alone. Reporting the upper bound (4) rather than an
/// optimistic count matches the module's existing "never underestimate"
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
        4
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

/// Derives, from one resolve's OWN `QueryAccounting` cache hit/miss delta
/// (never a pooled process counter -- a `PhaseAccounting::resolve()` handle
/// is built fresh per query attempt, so its counters ARE that attempt's
/// resolve-phase delta, with nothing to subtract), how many of that
/// resolve's `Recent`-origin segments were served from `ravel-catalog`'s
/// local commit record cache rather than fetched fresh.
///
/// # Why this is derived, not read directly
///
/// `ravel-catalog`'s plain L0 fast path (`Catalog::process_bucket`, out of
/// this crate's scope to read or edit) touches every commit record key
/// TWICE per resolve: once in `prewarm_commit_records` (a concurrent
/// warm-up pass, a REAL hit or miss against whatever the cache held before
/// this resolve started) and once in `include_l0_if_overlaps` (a sequential
/// include pass, which always hits because the prewarm pass just populated
/// the cache for that key, barring an eviction race in between). So for
/// `n_warm` keys already cached before this resolve started and `n_cold`
/// keys that were not:
///
/// - each `n_warm` key contributes 2 hits, 0 misses (prewarm hit, include
///   hit);
/// - each `n_cold` key contributes 1 hit, 1 miss (prewarm miss populates
///   the cache, include then hits).
///
/// So `cache_hits == 2 * n_warm + n_cold` and `cache_misses == n_cold`,
/// which solves to `n_warm == (cache_hits - cache_misses) / 2`: exactly the
/// count of records served from cache, under this shape.
///
/// # Segments, not records
///
/// `ravel-catalog`'s cache counters are per commit RECORD, while
/// `unfolded_segments_resolved` is a per-SEGMENT count (see that field's own
/// doc comment for when a segment and its underlying record diverge: a
/// multi-part L1 compaction). The two coincide for the all-L0 case this
/// derivation covers, one commit record naming one segment.
///
/// # Scope this is exact for
///
/// `cache_hits`/`cache_misses` on `QueryAccounting` are POOLED across every
/// cache a resolve's catalog call can touch: the L0 commit-record cache
/// this derivation models, the compaction-record cache, the rewrite-record
/// cache, the decoded-HEAD cache, the postings cache, and the
/// folded-snapshot-part byte cache (`Catalog::fetch_content_addressed`).
/// This function's arithmetic is only proven for a resolve whose window
/// contains ONLY plain L0 records, with no compaction, no rewrite, and no
/// sealed-below-watermark folded segment in the same resolve: any of those
/// add hits or misses this formula does not model, silently widening the
/// gap between this figure and the true served-from-cache count with no
/// signal that it happened.
///
/// # The constant decoded-HEAD probe
///
/// Even in that narrowed scope, `ravel-catalog`'s `resolve_snapshot_window`
/// (`crates/ravel-catalog/src/snapshot_resolve.rs`, out of this crate's
/// scope to edit) unconditionally calls `read_head` once per resolve,
/// whenever the window has any hour bounds at all, regardless of whether
/// the window's segments turn out to be all-L0. `read_head` checks the
/// decoded-HEAD cache first and records exactly one hit or miss into the
/// SAME pooled `QueryAccounting` counters this function reads, before
/// falling back to a store GET that itself returns `Ok(None)` (never
/// populating the cache) when no fold has ever produced a HEAD object for
/// this tenant and signal. A tenant/signal with no snapshot HEAD at all
/// (the common case for data recent enough that ADR-1199's cold/warm gate
/// even applies -- a fold has to run before a HEAD exists) therefore
/// contributes exactly one constant, never-converting miss on every
/// resolve, independent of segment count or warm/cold shape: `cache_hits ==
/// 2 * n_warm + n_cold` still holds, but `cache_misses == n_cold + 1`, one
/// more than the double-touch model alone predicts. This function corrects
/// for exactly that known, constant offset:
/// `n_warm == (cache_hits - (cache_misses - 1)) / 2`.
///
/// This correction is itself a narrowing, not a generalization: it assumes
/// the decoded-HEAD probe ran exactly once and missed. If this tenant and
/// signal DOES have a snapshot HEAD from an earlier fold (even one that
/// covers none of this resolve's own all-L0 window), `read_head` can hit
/// instead, which both drops the `+ 1` this correction subtracts from
/// `cache_misses` AND adds an uncounted `+ 1` to `cache_hits` -- moving the
/// result up to 1 too high rather than removing the offset this comment
/// documents. `ravel-catalog` exposes no counter that separates the
/// decoded-HEAD probe from the record-cache double-touch this derivation
/// otherwise models, and adding one is out of this task's scope
/// (`ravel-types`, `ravel-catalog`). See docs/query-engine.md for this
/// caveat restated for an operator reading the figure, and the final
/// report for how it was found (issue #1219): a live debug trace against
/// this crate's own test engine showed `cache_hits` matching the
/// double-touch model exactly while `cache_misses` was consistently one
/// higher, in both a fully-cold and an immediately-following fully-warm
/// resolve, which rules out an ordinary TTL hit/miss cycle (that would
/// convert to a hit on the second resolve) and points at a probe that
/// never gets a chance to populate its own cache in a never-folded
/// fixture.
///
/// Saturates to 0 rather than underflowing if `cache_misses` is 0 (no
/// decoded-HEAD probe ran at all -- a degenerate window with no hour
/// bounds) or if it otherwise exceeds `cache_hits` (not expected under the
/// modeled shape, but these are live counters, not a closed system, so
/// this function does not assume they stay consistent with each other).
pub fn unfolded_segments_served_from_cache(cache_hits: u64, cache_misses: u64) -> u64 {
    cache_hits.saturating_sub(cache_misses.saturating_sub(1)) / 2
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

/// A LOWER bound on the serial service rounds a nested plan/segment fan-out
/// needs, computed per wave of the OUTER `buffer_unordered(fanout)` over
/// `distinct_plans` distinct matcher plans rather than once over the total
/// work -- see the module docs' `service_batches` entry for why a single
/// total-work-over-peak-capacity division undercounts, and for the worked
/// examples this function's wave loop reproduces exactly.
///
/// `waves = ceil(distinct_plans / fanout)`. For 0-based wave `w`, `active_w
/// = min(fanout, distinct_plans - w * fanout)` distinct plans are admitted,
/// `capacity_w = min(fanout * active_w, shared_permits)` is that wave's
/// binding concurrency, and `batches_w = service_batches(segments *
/// active_w, capacity_w)` is the wave's own serial round count. The result
/// is the sum of `batches_w` across every wave, saturating throughout and
/// clamped to `u32::MAX` like [`service_batches`]. `fanout` and
/// `distinct_plans` are each clamped to at least 1, matching
/// [`service_batches`]'s own concurrency clamp, so this never divides by
/// zero and never iterates zero waves.
pub fn service_batches_over_plan_waves(
    segments: u64,
    distinct_plans: u64,
    fanout: u64,
    shared_permits: u64,
) -> u32 {
    let fanout = fanout.max(1);
    let distinct_plans = distinct_plans.max(1);
    let waves = distinct_plans.div_ceil(fanout);
    let mut total: u64 = 0;
    for w in 0..waves {
        let admitted_before = w.saturating_mul(fanout);
        let active = fanout.min(distinct_plans.saturating_sub(admitted_before));
        let capacity = fanout.saturating_mul(active).min(shared_permits);
        let batches = service_batches(segments.saturating_mul(active), capacity);
        total = total.saturating_add(u64::from(batches));
    }
    total.min(u64::from(u32::MAX)) as u32
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
        unfolded_segments_served_from_cache: u64,
        plan_class: PlanClass,
    ) -> QueryIoShape {
        QueryIoShape {
            dependency_depth: self.dependency_depth,
            list_page_depth: self.list_page_depth,
            service_batches: self.service_batches,
            unfolded_segments_resolved,
            unfolded_segments_served_from_cache,
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
    /// reports the upper bound 4, because a `FooterOutcome::NeedRange` chase
    /// on the tail guess, plus the catalog and page fetch stages, are real
    /// further stages this function cannot rule out from `object_size`
    /// alone. Flip the `object_size != 0 &&` guard off (restoring the old
    /// `object_size <= threshold` test alone) to watch the `object_size ==
    /// 0` case fail: it would wrongly report 1 instead of 4.
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
            4,
            "above threshold: footer tail, upper-bounded for a possible NeedRange \
             chase plus the catalog and page fetch stages"
        );
        assert_eq!(
            depth_for_object(0, 2_000),
            4,
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

    /// Pins `count_unfolded_segments`'s arithmetic against a realistic
    /// segment shape: a `Snapshot` holding one L0 `SegmentRef` plus a 3-part
    /// L1 compaction (`SegmentLevel::L1` at `part_index` 0, 1, 2, sharing one
    /// `input_set_hash` -- the real shape one compaction record fans out
    /// into) has 4 `segments` entries. This test builds `origins` itself
    /// (one `SegmentOrigin::Recent` per `snapshot.segments` entry, by
    /// construction), so it does NOT demonstrate that a real resolve
    /// actually produces one `Recent` origin per L1 part rather than one per
    /// compaction record -- that invariant is `ravel-catalog`'s own, made in
    /// its per-listed-key insert (`origin_by_key.insert(key.clone(),
    /// SegmentOrigin::Recent)` inside the listing loop,
    /// `crates/ravel-catalog/src/catalog.rs:1687`, where `key` is one listed
    /// segment object -- one entry per L1 part, not per compaction record),
    /// which is out of this crate's scope to prove. What this test DOES pin
    /// is that `count_unfolded_segments` counts every `Recent` entry in a
    /// same-length `origins` slice exactly, including when that slice came
    /// from a 4-segment/2-record snapshot shape rather than a flatter one.
    /// The parallelism assertion below (`origins.origins.len() ==
    /// snapshot.segments.len()`) checks the fixture is well-formed before
    /// the count assertion runs, so a fixture bug would fail there first.
    /// Flip the L1 fixture down to 1 part (drop two of the three
    /// `part_index` entries and their origins) to watch the count assertion
    /// fail: it would then read 2, not 4.
    #[test]
    fn count_unfolded_segments_counts_recent_origins_over_an_l0_plus_l1_parts_snapshot() {
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

    /// The double-touch derivation, plus the constant decoded-HEAD probe
    /// miss (see the function's own doc comment): a resolve with 3 keys
    /// already warm before it started and 5 cold contributes `2*3 + 5 =
    /// 11` hits and `5 + 1 = 6` misses (one extra miss from `read_head`
    /// finding no snapshot HEAD), so `(11 - (6 - 1)) / 2 == 3` recovers
    /// `n_warm` exactly. Flip the trailing `- 1` to nothing (as if the
    /// decoded-HEAD probe never contributed a miss) to watch this fail: it
    /// would then read 2, not 3.
    #[test]
    fn unfolded_segments_served_from_cache_recovers_warm_count_from_double_touch() {
        assert_eq!(unfolded_segments_served_from_cache(11, 6), 3);
    }

    /// A fully cold resolve (nothing warm beforehand): every key is a
    /// prewarm miss plus an include hit, so `cache_hits == n_cold` and
    /// `cache_misses == n_cold + 1` once the constant decoded-HEAD probe
    /// miss is folded in; no key was served from cache.
    #[test]
    fn unfolded_segments_served_from_cache_is_zero_when_fully_cold() {
        assert_eq!(unfolded_segments_served_from_cache(5, 6), 0);
    }

    /// A fully warm resolve (every key already cached beforehand): every
    /// key is a prewarm hit plus an include hit, so `cache_misses == 1`
    /// (only the constant decoded-HEAD probe miss) and `n_warm` equals half
    /// the hit count once that 1 is corrected for.
    #[test]
    fn unfolded_segments_served_from_cache_equals_resolved_when_fully_warm() {
        assert_eq!(unfolded_segments_served_from_cache(10, 1), 5);
    }

    /// Live counters are not a closed system: this function must not panic
    /// or underflow when `cache_misses` exceeds `cache_hits`, nor when
    /// `cache_misses` is 0 (the `- 1` correction must not underflow either).
    #[test]
    fn unfolded_segments_served_from_cache_saturates_instead_of_underflowing() {
        assert_eq!(unfolded_segments_served_from_cache(0, 5), 0);
        assert_eq!(unfolded_segments_served_from_cache(0, 0), 0);
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
