//! Always-on wall-clock phase timers for issue #2468's stage 0 per-phase
//! query time measurement. Each wrapped call site does exactly two atomic
//! adds (elapsed nanoseconds, call count); no per-item work, no cloning,
//! no recording beyond the two adds (mirrors `ravel_promql::op_timers`).
//!
//! These cover the phase boundaries `QueryStats`/`PhaseAccounting` (issue
//! #796) does not time: that accounting is request-count and byte-count
//! only, never wall-clock (see `phase_accounting.rs`'s module doc). The
//! operator phase already has its own always-on timers
//! (`ravel_promql::op_timers::{AGG_NS, MATCH_NS}`) and is not duplicated
//! here.

use std::sync::atomic::AtomicU64;

/// Catalog resolve: nanoseconds inside `Engine::resolve_bounded`, summed
/// across the first attempt and any not-found retry in
/// `resolve_snapshot_with_retry`.
pub static RESOLVE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `resolve_bounded` calls folded into `RESOLVE_NS`.
pub static RESOLVE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Object fetch: nanoseconds inside `SegmentFetcher::store_get`'s
/// `self.store.get(...).await`, the single funnel every ranged GET in
/// `fetcher.rs` passes through (footer, catalog sections, and page ranges
/// alike). Excludes the `get_limiter` semaphore-acquire wait ahead of it,
/// and any cache hit that never reaches the store.
pub static FETCH_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `store_get` calls folded into `FETCH_NS`.
pub static FETCH_CALLS: AtomicU64 = AtomicU64::new(0);

/// Segment decode: nanoseconds turning already-fetched bytes into typed
/// data, summed across the catalog-decode call sites in
/// `decode_selected`/`decode_sparse_catalog` (catalog bytes -> series
/// entries) and `decode_run`/`decode_histogram_run` (TS/VAL or TS/HIST
/// page bytes -> typed sample columns).
pub static DECODE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of decode calls folded into `DECODE_NS`.
pub static DECODE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Fetch+decode wall span: nanoseconds spent inside the per-plan
/// `buffer_unordered` fan-out in `prefetch_metric_plans`, i.e. the
/// wall-clock span occupied by concurrent per-segment fetch+decode, as
/// opposed to `FETCH_NS`/`DECODE_NS`'s totals summed across segments. Use
/// this, not the sum of the two above, for share-of-wall accounting when
/// segments fetch/decode concurrently.
pub static FETCH_DECODE_WALL_NS: AtomicU64 = AtomicU64::new(0);
/// Number of fan-out spans folded into `FETCH_DECODE_WALL_NS` (one per
/// `prefetch_metric_plans` call, i.e. one per query attempt).
pub static FETCH_DECODE_WALL_CALLS: AtomicU64 = AtomicU64::new(0);

/// Series materialisation: nanoseconds inside `merge_soa_runs` plus the
/// per-query label-set sort immediately after it, in
/// `Engine::prefetch_metric_plans`. Sequential, so summed and wall time
/// coincide for this phase.
pub static MATERIALIZE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of materialisation passes folded into `MATERIALIZE_NS`.
pub static MATERIALIZE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Result assembly: nanoseconds from the moment the operator call
/// (`Engine::evaluate`, timed by `ravel_promql::op_timers`) returns to the
/// moment `instant_inner` returns. Does not cover
/// `instant_with_stats_annotated`'s enclosing `tokio::time::timeout`/
/// `unify_deadline`, or `instant`'s `Coverage::from_stats`: both are thin,
/// await-free wrapping with no phase of their own, and fall into the
/// unattributed remainder instead of this timer.
pub static RESULT_ASSEMBLY_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `instant_inner` completions folded into `RESULT_ASSEMBLY_NS`.
pub static RESULT_ASSEMBLY_CALLS: AtomicU64 = AtomicU64::new(0);

// --- Issue #2479 stage 0b: per-segment future tiling, plus the two gaps
// outside it. Same always-on, zero-per-item-overhead convention as above.
// The per-segment future is `SegmentFetcher::fetch_runs_and_histograms`,
// the body of the inner (per-segment) `buffer_unordered` fan-out inside
// `Engine::fetch_all_samples_and_histograms`; the "count" half of each pair
// below is an item count (series, lookups, parses), not always a call
// count, incremented once per call or once after a loop per the task's
// tiling rule.

/// Per-segment future: nanoseconds from the moment the per-segment future in
/// `Engine::fetch_all_samples_and_histograms`'s fan-out closure starts
/// (before `fetch_soa_and_histograms_phase_accounted` is awaited) to the
/// moment it returns. Sum this and compare to `FETCH_DECODE_WALL_NS`: the
/// futures run concurrently on one task inside that wall span, so the sum
/// is expected to exceed the wall span, not equal it.
pub static FUTURE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of per-segment futures folded into `FUTURE_NS`.
pub static FUTURE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Limiter wait: nanoseconds inside `SegmentFetcher::store_get`'s
/// `self.get_limiter.acquire().await`, immediately ahead of `FETCH_NS`'s own
/// timer. Concurrency-limiting queue time, not fetch time; timed separately
/// so it is never silently folded into either `FETCH_NS` or the per-future
/// remainder.
pub static LIMITER_WAIT_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `get_limiter.acquire()` calls folded into `LIMITER_WAIT_NS`.
pub static LIMITER_WAIT_CALLS: AtomicU64 = AtomicU64::new(0);

/// Footer parse: nanoseconds inside `open_from_suffix` in
/// `SegmentFetcher::open_segment` (both the first-GET parse and, when the
/// footer chases a `NeedRange`, the second parse). CPU-only, no I/O; was
/// previously inside `open_segment`'s untimed remainder.
pub static FOOTER_PARSE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `open_from_suffix` calls folded into `FOOTER_PARSE_NS`.
pub static FOOTER_PARSE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Catalog retention accounting: nanoseconds inside
/// `SegmentFetcher::shrink_to_retained`'s call in `decode_selected` (the
/// `retained_catalog_len` summation over matched entries plus the
/// memory-budget reservation shrink), the one statement in `decode_selected`
/// that runs after `DECODE_NS`'s own timer closes.
pub static CATALOG_RETAIN_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `shrink_to_retained` calls folded into `CATALOG_RETAIN_NS`.
pub static CATALOG_RETAIN_CALLS: AtomicU64 = AtomicU64::new(0);

/// Page plan construction: nanoseconds inside the `plan_ranges_v4` calls in
/// `SegmentFetcher::fetch_pages`'s local `plan` closure (one call for the
/// scalar series slice, one for the histogram slice; a call on an empty
/// slice returns before reaching `plan_ranges_v4` and is not counted here).
/// This is the "page selection"/plan-construction step named in the task:
/// deciding which byte ranges to fetch, before `ensure_ranges` issues them.
pub static PAGE_PLAN_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `plan_ranges_v4` calls folded into `PAGE_PLAN_NS`.
pub static PAGE_PLAN_CALLS: AtomicU64 = AtomicU64::new(0);

/// Run-plan lookup: nanoseconds inside `find_run_plan`'s linear scan over
/// `planned`, called once per (series, run) pair in
/// `SegmentFetcher::build_scalar_decodes` and `build_histogram_decodes`.
/// This is the closest real analog in this crate to the pre-registered
/// "per-series map/set inserts" step: there is no actual map or set here
/// (see `stage0b-promql-fanout.md` for why), just an O(n) `Iterator::find`
/// re-scanning a same-order-of-magnitude slice once per series -- an O(n^2)
/// hazard this timer exists to confirm or rule out.
pub static RUN_PLAN_LOOKUP_NS: AtomicU64 = AtomicU64::new(0);
/// Number of `find_run_plan` calls folded into `RUN_PLAN_LOOKUP_NS`, i.e.
/// the number of (series, run) pairs looked up.
pub static RUN_PLAN_LOOKUP_CALLS: AtomicU64 = AtomicU64::new(0);

/// Label-set construction: nanoseconds inside `entry.entry.labels.clone()`,
/// the per-series (per-run, for an L1 part) deep clone of a `LabelSet` into
/// the emitted `RunDecode`/`RunHistogramDecode`, in
/// `SegmentFetcher::build_scalar_decodes` and `build_histogram_decodes`. A
/// series matched in k segments clones its label set once per segment (k
/// times), never once per query; see `stage0b-promql-fanout.md`'s multiplier
/// section.
pub static LABEL_CLONE_NS: AtomicU64 = AtomicU64::new(0);
/// Number of label-set clones folded into `LABEL_CLONE_NS`.
pub static LABEL_CLONE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Sample-run assembly: nanoseconds building and pushing one
/// `RunDecode`/`RunHistogramDecode` unit (struct construction plus its
/// `concat_priority_column`/`run_priority_column` call and the `Vec::push`),
/// excluding the label-set clone above (timed separately) and the page
/// decode itself (`DECODE_NS`), in `SegmentFetcher::build_scalar_decodes`
/// and `build_histogram_decodes`.
pub static SAMPLE_ASSEMBLY_NS: AtomicU64 = AtomicU64::new(0);
/// Number of emitted units folded into `SAMPLE_ASSEMBLY_NS`.
pub static SAMPLE_ASSEMBLY_CALLS: AtomicU64 = AtomicU64::new(0);

/// Pre-fan-out gap: nanoseconds in `Engine::prefetch_metric_plans` before
/// the per-segment fan-out starts, excluding `RESOLVE_NS`. Two disjoint
/// regions feed this one timer: the window/padding/name-filter/multiplier
/// setup that runs once per `prefetch_metric_plans` call before the
/// `attempt` closure is defined, and the `distinct_plans_by_matcher`
/// (recomputed inside the closure)/`is_pushdown_eligible`/
/// `count_over_time_pushdown_target` setup that runs once per attempt
/// (first try, and again on a not-found retry) immediately before
/// `fetch_decode_start`. Does not cover `resolve_snapshot_with_retry`'s own
/// handful of pre-resolve statements (`estimated_catalog_requests`,
/// `promql_fetch_fanout`, `PhaseAccounting::new`): that setup is generic
/// over the log lane too and is small enough (no per-series work) to leave
/// in the unattributed remainder; see `stage0b-promql-fanout.md`.
pub static PRE_FANOUT_NS: AtomicU64 = AtomicU64::new(0);
/// Number of regions folded into `PRE_FANOUT_NS` (two per attempt: see
/// above).
pub static PRE_FANOUT_CALLS: AtomicU64 = AtomicU64::new(0);

/// Post-fan-out gap: nanoseconds in `Engine::prefetch_metric_plans` between
/// the per-segment fan-out ending and the PromQL operator starting,
/// excluding `MATERIALIZE_NS`. Two disjoint regions feed this one timer:
/// the per-plan results collection loop plus the `federate_scalar` cross-
/// cluster fan-out (runs before `MATERIALIZE_NS`'s span), and the histogram
/// sort plus `precomputed_count` construction and the closure's `Ok(..)`
/// return (runs after it). Does not cover `resolve_snapshot_with_retry`'s
/// own post-attempt `QueryStats::new` construction, `prefetch`'s thin
/// metric/log-plan split wrapper, or `instant_inner`'s `Evaluator::new`
/// setup: all three are small, generic (shared with the log lane or with
/// every evaluator call) and await-free; left in the unattributed
/// remainder, see `stage0b-promql-fanout.md`.
pub static POST_FANOUT_NS: AtomicU64 = AtomicU64::new(0);
/// Number of regions folded into `POST_FANOUT_NS` (two per attempt: see
/// above).
pub static POST_FANOUT_CALLS: AtomicU64 = AtomicU64::new(0);
