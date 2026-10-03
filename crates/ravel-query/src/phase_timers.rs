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
