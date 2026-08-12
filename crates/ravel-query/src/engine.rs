//! QueryEngine: snapshot resolution, bounded-concurrency segment fetch,
//! cross-segment duplicate-sample resolution, and PromQL evaluation
//! (docs/query-engine.md "Flow", docs/catalog-and-mvcc.md).

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use promql_parser::parser::{Expr, Offset};
use ravel_catalog::{Catalog, SegmentRef, Snapshot};
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_promql::{
    Annotations, Evaluator, FloatHistogram, HistogramSeriesData, LabelMatcher, MatchOp, PlanAnchor,
    SelectorPlan, SeriesData, SeriesSource, SourceError, Span, Value, from_ast_matchers,
    has_or_group, matches_series, ms_to_ns, plan_selectors,
};
use ravel_segment::ReaderLimits;
use ravel_types::accounting::{CostEstimate, QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{
    CommitToken, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TimeRange,
};

use crate::config::{ByteLimit, EngineConfig};
use crate::erasure::ErasurePredicate;
use crate::error::QueryError;
use crate::fetcher::{
    CacheFetchError, FetchError, FetchStats, FetchedHistogramSeries, FetchedSeriesSoa,
    SegmentFetcher,
};

/// Which evaluation shape a prefetch is being computed for: an instant
/// query has one lookup instant, a range query spans a step grid whose
/// first and last instants bound every `PlanAnchor::Window` selector's own
/// lookup window (mirrors `ravel_promql::eval`'s per-step grid exactly, so
/// the prefetched window covers what evaluation will request).
enum EvalWindow {
    Instant {
        t_ns: i64,
    },
    Range {
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    },
}

/// Segment-level counters for one query's snapshot resolution
/// (docs/metric-index-plan.md P5b). `segments_pruned` counts only
/// snapshot-sourced segments postings-based pruning excluded; it is always
/// 0 when pruning did not apply -- no shared equality `__name__` filter
/// across the query's selectors, no usable postings, or a window served
/// entirely by listing/`min_token` lookup, both structurally unprunable
/// (docs/metric-index-plan.md P5b, `SnapshotWindow::extract_into`). This is
/// an exact count, never an estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryStats {
    /// Segments actually fetched (the post-pruning snapshot size).
    pub segments_fetched: u64,
    /// Snapshot-sourced segments postings pruning excluded.
    pub segments_pruned: u64,
    /// Page-fetch counters summed across every segment this query opened
    /// (issue #25, X1's `FetchStats`), no longer discarded at the
    /// `fetch_all_samples_and_histograms` call site (ADR-0044). Distinct
    /// from `accounting.decompressed_bytes`: this counts only
    /// `ValPageKind::RawF64` pages, `accounting` counts every decoded
    /// sample's typed-output footprint regardless of encoding.
    pub page_stats: FetchStats,
    /// Actual per-query store/decode counters (ADR-0044 decision 1),
    /// recorded at the fetcher's funnels.
    pub accounting: QueryAccountingSnapshot,
    /// Upper-envelope cost estimate computed after snapshot resolution and
    /// before any page fetch (ADR-0044 decision 3).
    pub estimate: CostEstimate,
    /// True when the query's coverage is partial: at least one federated
    /// remote cluster was skipped because it was unavailable and its
    /// `skip_unavailable` was set (ADR-0071, issue #868). Always false for a
    /// fully cluster-local query. Set after construction, in `prefetch`, once
    /// the federation fan-out has run.
    pub partial: bool,
    /// Prometheus-compatible `warnings` accumulated by the federation fan-out:
    /// one message per skipped remote cluster (ADR-0071, issue #868). Empty for
    /// a fully cluster-local query, or a federated query with every remote
    /// healthy. Surfaced into the query response's `warnings` array alongside
    /// the evaluator's own [`Annotations`] warnings.
    pub warnings: Vec<String>,
}

impl QueryStats {
    fn new(
        segments_fetched: u64,
        segments_pruned: u64,
        page_stats: FetchStats,
        accounting: QueryAccountingSnapshot,
        estimate: CostEstimate,
    ) -> Self {
        QueryStats {
            segments_fetched,
            segments_pruned,
            page_stats,
            accounting,
            estimate,
            partial: false,
            warnings: Vec::new(),
        }
    }
}

impl Default for QueryStats {
    /// `CostEstimate` has no `Default` (ADR-0044: an estimate is only ever
    /// computed from a real resolved snapshot, never assumed), so this is
    /// spelled out by hand rather than derived.
    fn default() -> Self {
        QueryStats {
            segments_fetched: 0,
            segments_pruned: 0,
            page_stats: FetchStats::default(),
            accounting: QueryAccountingSnapshot::default(),
            estimate: CostEstimate::new(0, 0, 0, 0, 0),
            partial: false,
            warnings: Vec::new(),
        }
    }
}

/// Requests one segment open (`fetcher.rs::open_segment`) can cost in the
/// worst case: the initial suffix/full GET, plus exactly one more GET if the
/// footer chase reports `NeedRange` (a second `NeedRange` there is treated
/// as corrupt, so this never recurses further).
const OPEN_REQUESTS_PER_SEGMENT: u64 = 2;

/// Requests the sparse catalog-probe path can cost for one segment
/// (`fetcher.rs::decode_sparse_catalog`): up to four named sections
/// (LABEL_DICT, SERIES_IDS, SERIES_IDX, SERIES_META_CHUNKS). `ensure_ranges`
/// coalesces sections that are contiguous or within its `coalesce_gap`, so
/// the real request count is usually 1; taking the un-coalesced count keeps
/// the estimate an upper bound regardless of section layout.
const CATALOG_REQUESTS_PER_SEGMENT: u64 = 4;

/// Requests one run's page fetch can cost (`fetcher.rs::fetch_scalar_pages` /
/// `fetch_histogram_pages`): a TS range and a VAL/HIST range, worst case two
/// separate GETs when `ensure_ranges` cannot coalesce them.
const PAGE_REQUESTS_PER_RUN: u64 = 2;

/// Safety factor applied to a segment's `object_size` for the store-bytes
/// estimate. `open_segment`'s `NeedRange` chase issues its second GET
/// without first checking `regions.covers()` (fetcher.rs), so in the worst
/// case the initial suffix GET and the chase GET overlap and both get
/// counted -- up to roughly the object read twice. `ensure_ranges` itself
/// dedups via `covers()`, so page fetches alone would never exceed one
/// object's worth of bytes; a factor of 2 covers both paths together
/// without modeling their exact overlap.
const STORE_BYTES_SAFETY_FACTOR: u64 = 2;

/// Upper bound on one segment's total decompressed sample bytes, derived
/// from the segment's own value kinds rather than one constant (ADR-0044
/// decision 3, amended). `SegmentRef::sample_count` aggregates scalar and
/// native-histogram samples with no split by kind (neither the catalog's
/// `SegmentRef` nor the persisted `CommitRecord`/`CompactionPart` it is
/// built from records one -- proto/ravel/commit.proto), so which samples in
/// a given segment are histograms is not knowable before the segment is
/// opened and its catalog decoded, which is exactly the work this estimate
/// exists to run ahead of. A scalar sample decodes to exactly 16 bytes
/// (`i64` timestamp + `f64` value, `decode_run`'s accounting); a
/// native-histogram sample decodes to roughly
/// `45 + 8 * (buckets + spans + custom_values)` (`histogram_value_footprint`,
/// fetcher.rs) with no reader-enforced cap on the bucket count, so no
/// per-sample constant loose enough for a wide histogram and tight enough
/// for scalars exists.
///
/// Absent a persisted per-kind split, this bounds every sample at the one
/// per-sample ceiling every decoded sample is structurally guaranteed not
/// to exceed regardless of kind: the page containing it
/// (`ReaderLimits::max_page_uncompressed_bytes`, docs/segment-format.md). A
/// segment carries at most one VAL_PAGES and one HIST_PAGES section
/// (`section_kind`), each independently capped at
/// `max_section_uncompressed_bytes`, so the segment's total decompressed
/// sample bytes cannot exceed twice that either -- taking the smaller of
/// the two keeps the bound tight for segments large enough that the
/// per-sample scaling alone would be absurd, without ever under-shooting.
/// This never under-shoots, which is the one hard requirement; it is
/// deliberately loose for scalar-only segments, which is the still-open gap
/// recorded on issue #418 (closing it needs a persisted per-kind sample
/// count, a frozen-format change out of this fix's scope).
fn segment_decompressed_bytes_upper_bound(seg: &SegmentRef, limits: &ReaderLimits) -> u64 {
    let per_sample_scaled = seg
        .sample_count
        .saturating_mul(limits.max_page_uncompressed_bytes);
    let section_capped = limits.max_section_uncompressed_bytes.saturating_mul(2);
    per_sample_scaled.min(section_capped)
}

/// Computes the upper-envelope [`CostEstimate`] for a resolved snapshot
/// (ADR-0044 decision 3), before any page fetch. `catalog_requests` is the
/// separately-computed catalog term (`Catalog::estimated_catalog_requests`),
/// bounded before `Catalog::resolve` runs; folded in here unconditionally so
/// a window that resolves to zero segments (e.g. an all-pruned window with
/// no snapshot HEAD) still carries a non-zero estimate for the LISTs
/// `resolve` actually issued, instead of a zero estimate against a non-zero
/// actual (`CostEstimate::divergence`'s `f64::INFINITY` case). `fetch_multiplier`
/// scales for query shapes that re-open every segment in the snapshot more
/// than once: `prefetch` fans out one independent fetch per selector
/// (`plans.len()`), so an N-selector query's actual cost is up to Nx a
/// single per-segment pass over the snapshot; `resolve_series_inner` fetches
/// the snapshot once, so its multiplier is 1. Without this factor the
/// estimate would not be a genuine upper bound for multi-selector queries.
fn estimate_cost(
    snapshot: &Snapshot,
    fetch_multiplier: u64,
    catalog_requests: u64,
) -> CostEstimate {
    let segments = snapshot.segments.len() as u64;
    let series: u64 = snapshot.segments.iter().map(|s| s.series_count).sum();
    let limits = ReaderLimits::default();
    let mut estimated_requests =
        catalog_requests + segments * (OPEN_REQUESTS_PER_SEGMENT + CATALOG_REQUESTS_PER_SEGMENT);
    let mut estimated_store_bytes: u64 = 0;
    let mut estimated_decompressed_bytes: u64 = 0;
    for seg in &snapshot.segments {
        estimated_requests += seg.series_count.saturating_mul(PAGE_REQUESTS_PER_RUN);
        estimated_store_bytes = estimated_store_bytes
            .saturating_add(seg.object_size.saturating_mul(STORE_BYTES_SAFETY_FACTOR));
        estimated_decompressed_bytes = estimated_decompressed_bytes
            .saturating_add(segment_decompressed_bytes_upper_bound(seg, &limits));
    }
    CostEstimate::new(
        estimated_requests.saturating_mul(fetch_multiplier),
        estimated_store_bytes.saturating_mul(fetch_multiplier),
        estimated_decompressed_bytes.saturating_mul(fetch_multiplier),
        segments,
        series,
    )
}

/// Resolves snapshots, fetches segments, merges cross-segment duplicates,
/// and evaluates PromQL over the result (docs/query-engine.md).
pub struct QueryEngine {
    catalog: Arc<Catalog>,
    fetcher: SegmentFetcher,
    config: EngineConfig,
    /// ADR-0071 distributed read fan-out (issue #864). `None` is the default:
    /// the engine runs the local fetch path untouched. `Some` opts this engine
    /// into cost-gated fan-out to slice workers; see
    /// [`QueryEngine::with_distributed`].
    distributed: Option<Arc<crate::distrib::Distributed>>,
    /// ADR-0071 cross-cluster federation (issue #868). `None` is the default:
    /// no remote clusters, a fully cluster-local query. `Some` opts this engine
    /// into fanning each query's matchers/window out to the configured remote
    /// clusters and unioning their series into the merge pool; see
    /// [`QueryEngine::with_federation`].
    federation: Option<Arc<crate::distrib::Federation>>,
}

impl QueryEngine {
    pub fn new(
        catalog: Arc<Catalog>,
        store: Arc<dyn ObjectStoreBackend>,
        config: EngineConfig,
    ) -> Self {
        QueryEngine {
            catalog,
            fetcher: SegmentFetcher::new(store),
            config,
            distributed: None,
            federation: None,
        }
    }

    /// Opts this engine into ADR-0071 distributed read fan-out (issue #864).
    /// Mirrors [`Self::with_cache`]: `QueryEngine::new` builds a local-only
    /// engine, and this is the single seam a caller uses to attach a
    /// distributed context after construction. Off by default; with no
    /// distributed context every query runs the byte-identical local path.
    #[must_use]
    pub fn with_distributed(mut self, distributed: Arc<crate::distrib::Distributed>) -> Self {
        self.distributed = Some(distributed);
        self
    }

    /// Opts this engine into ADR-0071 cross-cluster federation (issue #868).
    /// Mirrors [`Self::with_distributed`]: off by default, and with no
    /// federation context every query runs the cluster-local path untouched.
    /// When set, a query fans its matchers/window out to each configured remote
    /// cluster, unions the returned series into the merge pool, and surfaces
    /// any skipped-cluster warnings and partial-coverage flag.
    #[must_use]
    pub fn with_federation(mut self, federation: Arc<crate::distrib::Federation>) -> Self {
        self.federation = Some(federation);
        self
    }

    /// Attaches the ADR-0046 read cache to this engine's fetcher, via
    /// [`SegmentFetcher::with_cache`]. `QueryEngine::new` builds its fetcher
    /// privately, so this is the only seam a caller has to opt a `QueryEngine`
    /// into the cache after construction.
    #[must_use]
    pub fn with_cache(mut self, cache: Arc<ravel_cache::Cache<CacheFetchError>>) -> Self {
        self.fetcher = self.fetcher.with_cache(cache);
        self
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub async fn instant(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        t_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<Value, QueryError> {
        let (value, _stats) = self
            .instant_with_stats(tenant_hash, query, t_ms, min_tokens, now_ns, deadline)
            .await?;
        Ok(value)
    }

    /// Same as [`Self::instant`], additionally returning this query's
    /// segment counters (docs/metric-index-plan.md P5b). Additive: `instant`
    /// keeps its original signature and behavior unchanged, mirroring the
    /// `Catalog::resolve`/`resolve_pruned` split.
    pub async fn instant_with_stats(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        t_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<(Value, QueryStats), QueryError> {
        let (value, _annotations, stats) = self
            .instant_with_stats_annotated(tenant_hash, query, t_ms, min_tokens, now_ns, deadline)
            .await?;
        Ok((value, stats))
    }

    /// Same as [`Self::instant_with_stats`], additionally returning the
    /// evaluation [`Annotations`] (Prometheus' separate `warnings` and
    /// `infos`). The HTTP query handlers use this so both fields reach the
    /// response envelope; the stats-only and value-only wrappers discard the
    /// annotations, keeping their original signatures.
    pub async fn instant_with_stats_annotated(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        t_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<(Value, Annotations, QueryStats), QueryError> {
        let eval_deadline = Instant::now() + deadline;
        let outcome = tokio::time::timeout(
            deadline,
            self.instant_inner(tenant_hash, query, t_ms, min_tokens, now_ns, eval_deadline),
        )
        .await;
        unify_deadline(outcome, deadline)
    }

    async fn instant_inner(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        t_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        eval_deadline: Instant,
    ) -> Result<(Value, Annotations, QueryStats), QueryError> {
        let t_ns = ms_to_ns(t_ms)?;
        let plans = plan_selectors(query, t_ms, t_ms)?;
        let eval_window = EvalWindow::Instant { t_ns };
        let (source, stats) = self
            .prefetch(tenant_hash, &plans, &eval_window, min_tokens, now_ns)
            .await?;
        let evaluator = Evaluator::new()
            .with_default_step(self.config.default_evaluation_interval)?
            .with_deadline(eval_deadline);
        let (value, annotations) = tracing::debug_span!("evaluate", eval_kind = "instant")
            .in_scope(|| evaluator.eval_instant_annotated(&source, query, t_ms))?;
        Ok((value, annotations, stats))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn range(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<Value, QueryError> {
        let (value, _stats) = self
            .range_with_stats(
                tenant_hash,
                query,
                start_ms,
                end_ms,
                step_ms,
                min_tokens,
                now_ns,
                deadline,
            )
            .await?;
        Ok(value)
    }

    /// Same as [`Self::range`], additionally returning this query's segment
    /// counters (docs/metric-index-plan.md P5b). Additive: `range` keeps its
    /// original signature and behavior unchanged, mirroring the
    /// `Catalog::resolve`/`resolve_pruned` split.
    #[allow(clippy::too_many_arguments)]
    pub async fn range_with_stats(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<(Value, QueryStats), QueryError> {
        let (value, _annotations, stats) = self
            .range_with_stats_annotated(
                tenant_hash,
                query,
                start_ms,
                end_ms,
                step_ms,
                min_tokens,
                now_ns,
                deadline,
            )
            .await?;
        Ok((value, stats))
    }

    /// Same as [`Self::range_with_stats`], additionally returning the
    /// evaluation [`Annotations`] (Prometheus' separate `warnings` and
    /// `infos`), for the HTTP range-query handler's response envelope.
    #[allow(clippy::too_many_arguments)]
    pub async fn range_with_stats_annotated(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<(Value, Annotations, QueryStats), QueryError> {
        let eval_deadline = Instant::now() + deadline;
        let outcome = tokio::time::timeout(
            deadline,
            self.range_inner(
                tenant_hash,
                query,
                start_ms,
                end_ms,
                step_ms,
                min_tokens,
                now_ns,
                eval_deadline,
            ),
        )
        .await;
        unify_deadline(outcome, deadline)
    }

    #[allow(clippy::too_many_arguments)]
    async fn range_inner(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
        eval_deadline: Instant,
    ) -> Result<(Value, Annotations, QueryStats), QueryError> {
        if step_ms <= 0 {
            return Err(QueryError::NonPositiveStep { step_ms });
        }
        if start_ms > end_ms {
            return Err(QueryError::InvalidRange { start_ms, end_ms });
        }
        let start_ns = ms_to_ns(start_ms)?;
        let end_ns = ms_to_ns(end_ms)?;
        let step_ns = ms_to_ns(step_ms)?;
        let plans = plan_selectors(query, start_ms, end_ms)?;
        let eval_window = EvalWindow::Range {
            start_ns,
            end_ns,
            step_ns,
        };
        let (source, stats) = self
            .prefetch(tenant_hash, &plans, &eval_window, min_tokens, now_ns)
            .await?;
        let evaluator = Evaluator::new()
            .with_default_step(self.config.default_evaluation_interval)?
            .with_deadline(eval_deadline);
        let (value, annotations) =
            tracing::debug_span!("evaluate", eval_kind = "range").in_scope(|| {
                evaluator.eval_range_annotated(&source, query, start_ms, end_ms, step_ms)
            })?;
        Ok((value, annotations, stats))
    }

    /// Resolves the series (labels only, no samples) matching `matchers` in
    /// `window`, for the labels/label-values/series HTTP endpoints.
    pub async fn resolve_series(
        &self,
        tenant_hash: TenantHash,
        matchers: &[LabelMatcher],
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<Vec<(SeriesId, LabelSet)>, QueryError> {
        let (series, _stats) = self
            .resolve_series_with_stats(tenant_hash, matchers, window, min_tokens, now_ns, deadline)
            .await?;
        Ok(series)
    }

    /// Same as [`Self::resolve_series`], additionally returning this query's
    /// segment counters (docs/metric-index-plan.md P5b). Additive:
    /// `resolve_series` keeps its original signature and behavior unchanged,
    /// mirroring the `Catalog::resolve`/`resolve_pruned` split.
    pub async fn resolve_series_with_stats(
        &self,
        tenant_hash: TenantHash,
        matchers: &[LabelMatcher],
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        deadline: Duration,
    ) -> Result<(Vec<(SeriesId, LabelSet)>, QueryStats), QueryError> {
        tokio::time::timeout(
            deadline,
            self.resolve_series_inner(tenant_hash, matchers, window, min_tokens, now_ns),
        )
        .await
        .map_err(|_| QueryError::DeadlineExceeded { deadline })?
    }

    async fn resolve_series_inner(
        &self,
        tenant_hash: TenantHash,
        matchers: &[LabelMatcher],
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<(Vec<(SeriesId, LabelSet)>, QueryStats), QueryError> {
        let name_filter = equality_name_filter(matchers);
        let attempt = |snapshot: Snapshot, accounting: QueryAccounting| async move {
            // The bytes-scanned budget (ADR-0061 decision 1) is enforced
            // inside `fetch_all_series`, once per completed segment fetch, so a
            // labels/series query that matches zero series still evaluates the
            // budget against every segment its snapshot actually fetched -- the
            // `build_series_by_id` loop below never runs for an empty result.
            let fetched = self
                .fetch_all_series(
                    tenant_hash,
                    &snapshot,
                    matchers,
                    &accounting,
                    self.config.max_bytes_scanned,
                )
                .await?;
            let by_id = build_series_by_id(
                fetched
                    .into_iter()
                    .flatten()
                    .map(|entry| (entry.series_id, entry.labels)),
                self.config.max_series,
            )?;
            Ok((by_id.into_iter().collect(), FetchStats::default()))
        };

        // resolve_series never re-opens the snapshot's segments more than
        // once, unlike `prefetch`'s per-selector fan-out.
        self.resolve_snapshot_with_retry(
            tenant_hash,
            window,
            min_tokens,
            now_ns,
            name_filter.as_deref(),
            1,
            attempt,
        )
        .await
    }

    /// Prefetches every selector `plan_selectors` reported: one shared
    /// snapshot resolved against the union of every selector's own fetch
    /// window (`padded_range`, docs/query-engine.md), then one
    /// concurrency-bounded, independently budget-checked fetch+merge per
    /// selector's own matchers against that snapshot. A selector's fetch
    /// only prunes by its own matchers server-side; a later
    /// `SeriesSource::query` call still clips to its own window, so
    /// combining every selector's already-merged series into one flat
    /// source is correct regardless of how widely the selectors' windows
    /// or matchers differ. An empty plan list (a query with no selectors,
    /// e.g. a bare scalar or string literal) skips storage entirely.
    async fn prefetch(
        &self,
        tenant_hash: TenantHash,
        plans: &[SelectorPlan],
        eval_window: &EvalWindow,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<(MergedSource, QueryStats), QueryError> {
        if plans.is_empty() {
            return Ok((
                MergedSource {
                    series: Vec::new(),
                    histogram_series: Vec::new(),
                },
                QueryStats::default(),
            ));
        }

        let windows: Vec<TimeRange> = plans
            .iter()
            .map(|plan| selector_fetch_window(plan, eval_window))
            .collect::<Result<_, _>>()?;
        let mut padded = windows[0];
        for w in &windows[1..] {
            padded.start_ns = padded.start_ns.min(w.start_ns);
            padded.end_ns = padded.end_ns.max(w.end_ns);
        }

        let name_filter = shared_equality_name_filter(plans);
        let max_series = self.config.max_series;
        let max_samples = self.config.max_samples;
        let max_bytes_scanned = self.config.max_bytes_scanned;
        let concurrency = self.config.fetch_concurrency.max(1);
        // One independent fetch per selector against the same snapshot
        // (below): an N-selector query re-opens every snapshot segment up to
        // N times, so the pre-fetch cost estimate must scale by this same
        // factor to stay a genuine upper bound.
        let fetch_multiplier = plans.len() as u64;
        // Captured by reference (like `plans`), so the `FnMut` attempt copies
        // the reference on each retry rather than moving the owned `Vec` out of
        // its environment. The per-plan futures below build owned pairs from it.
        let windows_ref = &windows;
        let attempt = |snapshot: Snapshot, accounting: QueryAccounting| async move {
            // Owned clones, not borrowed slice items: a closure capturing a
            // reference into `plans` through this combinator chain makes
            // rustc infer a fixed (non-higher-ranked) lifetime for the
            // closure, which later fails to unify with axum's `Handler`
            // blanket impl ("implementation of FnOnce is not general
            // enough") at the router call site in `http/mod.rs`. Cloning
            // each `SelectorPlan` into the future sidesteps that entirely.
            // Per-plan local result: this selector's raw scalar runs (kept
            // un-merged so cross-cluster federation runs can join the same
            // pool and merge once, below), the merged native-histogram series
            // (federation contributes no histograms), and the page stats.
            type PerPlan = (
                Vec<Vec<FetchedSeriesSoa>>,
                Vec<HistogramSeriesData>,
                FetchStats,
            );
            let results: Vec<Result<PerPlan, QueryError>> = stream::iter(plans.to_vec())
                .map(|plan| {
                    let snapshot = &snapshot;
                    let accounting = &accounting;
                    async move {
                        // A selector's segments carry scalar and/or
                        // native-histogram series; fetch both kinds (a series is
                        // one kind for its whole life, so the two sets never
                        // overlap). The two kinds come off each segment in one
                        // open+decode pass (#278 item 1), not two independent
                        // segment opens.
                        // The bytes-scanned budget (ADR-0061 decision 1) is
                        // enforced inside `fetch_all_samples_and_histograms`
                        // itself, once per completed segment fetch, against the
                        // shared accounting handle every selector's concurrent
                        // fetch charges into -- so the budget bounds the whole
                        // query's scan and cancels it mid-fetch, not after the
                        // merge has already paid for every byte.
                        let (scalar_fetched, page_stats, hist_fetched) = self
                            .fetch_samples_and_histograms_maybe_distributed(
                                tenant_hash,
                                snapshot,
                                &plan.matchers,
                                accounting,
                                max_bytes_scanned,
                            )
                            .await?;
                        let histograms =
                            merge_histogram_soa_runs(hist_fetched, max_series, max_samples)?;
                        Ok::<PerPlan, QueryError>((scalar_fetched, histograms, page_stats))
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            // Collect every plan's raw scalar runs into one pool and every
            // plan's histogram series into one deduplicated map. Scalar runs
            // are merged once, below, after federation has appended its runs,
            // so a single k-way `merge_soa_runs` unions across selectors,
            // segments, AND clusters with the one deterministic `is_greater`
            // total order. Two plans that select the same series decode
            // byte-identical raw runs (the decode is a pure function of
            // segment bytes + series id, no per-plan window trim; per-selector
            // window clipping happens downstream in `MergedSource::query`), so
            // pooling their runs is loss-free -- documented in
            // docs/query-engine.md.
            //
            // Enforcement scope: because the runs are pooled, the single
            // `merge_soa_runs` below enforces `max_series`/`max_samples` ONCE
            // over the union of every selector's (and every remote cluster's)
            // runs. That makes the caps a query-wide bound on the
            // coordinator's peak memory, not a per-selector budget: a
            // multi-selector or federated query is bounded by the cap as a
            // whole rather than by cap-times-selectors. This is intentional --
            // the caps exist to bound this process's memory for the whole
            // query -- and it is why the merge must be the one reachable place
            // the scalar caps are checked (the per-selector loop above
            // deliberately does not check them for scalars; histograms, merged
            // per selector, still check there).
            let mut all_scalar_runs: Vec<Vec<FetchedSeriesSoa>> = Vec::new();
            let mut combined_histograms: HashMap<LabelSet, HistogramSeriesData> = HashMap::new();
            let mut page_stats = FetchStats::default();
            for r in results {
                let (scalar_runs, histograms, per_plan_stats) = r?;
                all_scalar_runs.extend(scalar_runs);
                for series in histograms {
                    combined_histograms
                        .entry(series.labels.clone())
                        .or_insert(series);
                }
                page_stats.raw_f64_pages += per_plan_stats.raw_f64_pages;
                page_stats.raw_f64_bytes += per_plan_stats.raw_f64_bytes;
            }

            // Cross-cluster federation (ADR-0071, issue #868): fan each
            // selector's matchers and window out to every configured remote
            // cluster and append the returned runs to the same pool the local
            // fetch produced, so the k-way merge below unions across cluster
            // boundaries exactly as it unions across segments. Each remote
            // resolves its own snapshot and enforces its own
            // admission/limits/erasure, so the coordinator sends no erasure
            // predicates and no cluster-local commit tokens across the
            // boundary (the remote's tenant identity comes from its own auth
            // over the operator credential baked into the fetcher, never from
            // the wire and never a client credential). Run sequentially,
            // outside the `buffer_unordered` above: an `async_trait` fetch
            // future nested inside that combinator makes the whole query
            // future fail the higher-ranked `Send` bound axum's `Handler`
            // blanket impl requires.
            // Owned `(matchers, start, end)` tuples: the fan-out runs in a
            // named `async fn` (below), whose future is higher-ranked over its
            // input lifetimes; passing owned data avoids tying this query
            // future to the borrowed `plans` fn-param, which would fail axum's
            // `Handler` `Send` bound (the same reason the local fetch clones
            // each `SelectorPlan`).
            let fed_plans: Vec<(Vec<LabelMatcher>, i64, i64)> = plans
                .iter()
                .zip(windows_ref.iter())
                .map(|(plan, w)| (plan.matchers.clone(), w.start_ns, w.end_ns))
                .collect();
            let (fed_runs, fed_stats, fed_warnings, fed_partial) = self
                .federate_scalar(tenant_hash, fed_plans, &accounting)
                .await?;
            all_scalar_runs.extend(fed_runs);
            // `fed_stats` is reported by a remote cluster over the wire, so a
            // buggy or hostile remote could report values that overflow a plain
            // `+=` (a debug-build panic, a release-build wrap). Saturate instead:
            // these are diagnostic page/byte counters, so pinning at u64::MAX is
            // correct and never affects query results.
            page_stats.raw_f64_pages = page_stats
                .raw_f64_pages
                .saturating_add(fed_stats.raw_f64_pages);
            page_stats.raw_f64_bytes = page_stats
                .raw_f64_bytes
                .saturating_add(fed_stats.raw_f64_bytes);

            let mut series = merge_soa_runs(all_scalar_runs, max_series, max_samples)?;
            series.sort_by(|a, b| a.labels.iter().cmp(b.labels.iter()));
            let mut histogram_series: Vec<HistogramSeriesData> =
                combined_histograms.into_values().collect();
            histogram_series.sort_by(|a, b| a.labels.iter().cmp(b.labels.iter()));
            Ok((
                (
                    MergedSource {
                        series,
                        histogram_series,
                    },
                    fed_warnings,
                    fed_partial,
                ),
                page_stats,
            ))
        };

        let ((source, warnings, partial), mut stats) = self
            .resolve_snapshot_with_retry(
                tenant_hash,
                padded,
                min_tokens,
                now_ns,
                name_filter.as_deref(),
                fetch_multiplier,
                attempt,
            )
            .await?;
        stats.partial = partial;
        stats.warnings = warnings;
        Ok((source, stats))
    }

    /// Resolves a snapshot, enforces `max_segments`, runs `attempt` once,
    /// and on a store `NotFound` (a pinned segment vanished under a
    /// concurrent GC/compaction) re-resolves and retries the whole query
    /// exactly once before giving up with `SnapshotInvalidated`
    /// (docs/catalog-and-mvcc.md).
    #[allow(clippy::too_many_arguments)]
    async fn resolve_snapshot_with_retry<T, F, Fut>(
        &self,
        tenant_hash: TenantHash,
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        fetch_multiplier: u64,
        mut attempt: F,
    ) -> Result<(T, QueryStats), QueryError>
    where
        F: FnMut(Snapshot, QueryAccounting) -> Fut,
        Fut: std::future::Future<Output = Result<(T, FetchStats), QueryError>>,
    {
        // The catalog term (ADR-0044 decision 3) is the same for both
        // attempts: `window` and `now_ns` do not change on retry, only the
        // snapshot resolve's outcome does.
        let catalog_requests = self.catalog.estimated_catalog_requests(window, now_ns);
        // Fresh handle per attempt, created before `resolve_bounded` runs so
        // the same handle that goes on to fetch segments also receives
        // resolve's own catalog-side counters (ADR-0044 decision 1: "created
        // once per query" -- here, per the query attempt that wins). A
        // retried attempt re-resolves and re-fetches from scratch, so the
        // discarded first attempt's in-flight counts must not bleed into the
        // attempt that actually produced the result.
        let first_accounting = QueryAccounting::new();
        let first = self
            .resolve_bounded(
                tenant_hash,
                window,
                min_tokens,
                now_ns,
                name_filter,
                &first_accounting,
            )
            .await?;
        let first_estimate = estimate_cost(&first, fetch_multiplier, catalog_requests);
        let first_segments = first.segments.len() as u64;
        let first_pruned = first.segments_pruned;
        match attempt(first, first_accounting.clone()).await {
            Err(QueryError::Fetch(FetchError::Store {
                source: StoreError::NotFound,
                ..
            })) => {
                let second_accounting = QueryAccounting::new();
                let second = self
                    .resolve_bounded(
                        tenant_hash,
                        window,
                        min_tokens,
                        now_ns,
                        name_filter,
                        &second_accounting,
                    )
                    .await?;
                let second_estimate = estimate_cost(&second, fetch_multiplier, catalog_requests);
                let second_segments = second.segments.len() as u64;
                let second_pruned = second.segments_pruned;
                match attempt(second, second_accounting.clone()).await {
                    Err(QueryError::Fetch(FetchError::Store {
                        source: StoreError::NotFound,
                        ..
                    })) => Err(QueryError::SnapshotInvalidated),
                    Ok((t, page_stats)) => Ok((
                        t,
                        QueryStats::new(
                            second_segments,
                            second_pruned,
                            page_stats,
                            second_accounting.snapshot(),
                            second_estimate,
                        ),
                    )),
                    Err(other) => Err(other),
                }
            }
            Ok((t, page_stats)) => Ok((
                t,
                QueryStats::new(
                    first_segments,
                    first_pruned,
                    page_stats,
                    first_accounting.snapshot(),
                    first_estimate,
                ),
            )),
            Err(other) => Err(other),
        }
    }

    // The `catalog_resolve` span now lives on `Catalog::resolve_impl`
    // (crates/ravel-catalog/src/catalog.rs), so every caller of
    // `Catalog::resolve*` gets it, including ravel-sql's executor which calls
    // `resolve_pruned_with_accounting` directly and never reaches this wrapper
    // (ADR-0044 decision 5, issue #642). Instrumenting here too would span a
    // resolve reached through ravel-query twice.
    async fn resolve_bounded(
        &self,
        tenant_hash: TenantHash,
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, QueryError> {
        let snapshot = self
            .catalog
            .resolve_pruned_with_accounting(
                &tenant_hash,
                Signal::Metrics,
                window,
                min_tokens,
                now_ns,
                name_filter,
                accounting,
            )
            .await?;
        if snapshot.segments.len() > self.config.max_segments {
            return Err(QueryError::TooManySegments {
                count: snapshot.segments.len(),
                max: self.config.max_segments,
            });
        }
        Ok(snapshot)
    }

    /// Fetches every matched series from each snapshot segment in a single
    /// open+decode pass per segment (#278 item 1), returning the scalar SoA
    /// runs and the native-histogram runs as parallel per-segment vectors
    /// (one entry per segment, same order), plus the per-segment
    /// `FetchStats` summed across the snapshot (issue #25, X1; no longer
    /// discarded, ADR-0044). Replaces the former separate
    /// `fetch_all_samples_soa` + `fetch_all_histograms` passes, which opened
    /// and catalog-decoded every segment twice.
    async fn fetch_all_samples_and_histograms(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
        max_bytes_scanned: ByteLimit,
    ) -> Result<
        (
            Vec<Vec<FetchedSeriesSoa>>,
            FetchStats,
            Vec<Vec<FetchedHistogramSeries>>,
        ),
        QueryError,
    > {
        let concurrency = self.config.fetch_concurrency.max(1);
        let matchers: Arc<Vec<LabelMatcher>> = Arc::new(matchers.to_vec());
        let mut stream = stream::iter(snapshot.segments.iter().cloned())
            .map(|seg_ref| {
                let fetcher = self.fetcher.clone();
                let matchers = Arc::clone(&matchers);
                let accounting = accounting.clone();
                async move {
                    fetcher
                        .fetch_soa_and_histograms_accounted(
                            tenant_hash,
                            &seg_ref,
                            matchers.as_slice(),
                            &accounting,
                        )
                        .await
                }
            })
            .buffer_unordered(concurrency);
        let mut scalar_out = Vec::with_capacity(snapshot.segments.len());
        let mut histogram_out = Vec::with_capacity(snapshot.segments.len());
        let mut page_stats = FetchStats::default();
        // Consume the fetch fan-out one completed segment at a time so the
        // bytes-scanned budget (ADR-0061 decision 1) can short-circuit here,
        // at the stage that owns segment concurrency, instead of after
        // `.collect()` has already drained (and paid for) every segment.
        // Returning early drops `stream`, which stops polling the remaining
        // segment fetch futures and cancels their in-flight GETs
        // cooperatively (proven by tests/bytes_scanned_budget.rs).
        while let Some(r) = stream.next().await {
            let (scalar, stats, histograms) = r?;
            scalar_out.push(scalar);
            histogram_out.push(histograms);
            page_stats.raw_f64_pages += stats.raw_f64_pages;
            page_stats.raw_f64_bytes += stats.raw_f64_bytes;
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), max_bytes_scanned)
            {
                return Err(err);
            }
        }
        // Selective-erasure exclusion (ADR-0064 decision 2, EJ-T3): drop
        // erased series/samples from every segment's decoded results, after the
        // fetch and after any cache tier `fetch_soa_and_histograms_accounted`
        // consulted, before these results reach the PromQL evaluator. A no-op
        // when the snapshot carries no pending erasure predicate.
        let erasure = snapshot_erasure_predicates(snapshot);
        if !erasure.is_empty() {
            for series in &mut scalar_out {
                crate::erasure::retain_series_soa(series, &erasure);
            }
            for series in &mut histogram_out {
                crate::erasure::retain_histogram_series(series, &erasure);
            }
        }
        Ok((scalar_out, page_stats, histogram_out))
    }

    /// The distribution seam (ADR-0071, issue #864). When this engine has a
    /// distributed context AND the pre-fetch cost estimate trips the gate, fan
    /// the snapshot out to slice workers; otherwise (the default, or a
    /// below-threshold query, or a worker that signalled a fall-back) run the
    /// byte-identical local [`Self::fetch_all_samples_and_histograms`].
    ///
    /// The distributed path returns the same triple the local path does, with
    /// erasure already applied worker-side (so this method does not re-apply
    /// it), so the caller's merge step is identical for both. When
    /// `self.distributed` is `None` this is exactly a call to the local
    /// fetch, which is why an engine that never opts in behaves precisely as
    /// it did before this seam existed.
    async fn fetch_samples_and_histograms_maybe_distributed(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
        max_bytes_scanned: ByteLimit,
    ) -> Result<
        (
            Vec<Vec<FetchedSeriesSoa>>,
            FetchStats,
            Vec<Vec<FetchedHistogramSeries>>,
        ),
        QueryError,
    > {
        if let Some(distributed) = &self.distributed {
            // The cost gate reads the same estimate shape the engine already
            // computes (ADR-0044); a single per-segment pass (multiplier 1, no
            // catalog term) is the right coarse signal for "is this snapshot
            // big enough to distribute".
            let cost = estimate_cost(snapshot, 1, 0);
            if crate::distrib::partition::should_distribute(distributed.thresholds(), &cost) {
                let erasure = snapshot_erasure_predicates(snapshot);
                if let Some(triple) = distributed
                    .fetch(
                        tenant_hash,
                        Signal::Metrics,
                        snapshot,
                        matchers,
                        &erasure,
                        accounting,
                        &self.config,
                    )
                    .await?
                {
                    // Bytes-scanned enforcement is two-tiered (ADR-0071):
                    // slice-granularity short-circuit at the coordinator (it
                    // folds each slice's accounting as that slice completes and
                    // trips this budget before draining the rest) plus
                    // segment-granularity enforcement on each worker. This
                    // final re-check is a backstop against a worker that
                    // under-reports: it re-evaluates the same budget over the
                    // fully-folded accounting the distributed fetch returned.
                    if let Some(err) = bytes_scanned_exceeded(
                        accounting.snapshot().total_s3_bytes(),
                        max_bytes_scanned,
                    ) {
                        return Err(err);
                    }
                    return Ok(triple);
                }
                // `None`: a worker asked for local fallback (version skew, a
                // histogram-bearing slice). Fall through to the local path.
                // The distributed attempt's real S3 spend was already folded
                // into `accounting` (ADR-0071: cost is exact and visible), so
                // the local path below re-enforces `max_bytes_scanned` against
                // a handle that carries the remote spend too. Consequence: a
                // fallback query can trip the byte budget with distribution
                // enabled where the identical query succeeds without it --
                // that is the true total cost, not a double-count.
            }
        }
        self.fetch_all_samples_and_histograms(
            tenant_hash,
            snapshot,
            matchers,
            accounting,
            max_bytes_scanned,
        )
        .await
    }

    /// Cross-cluster federation fan-out for the scalar-sample path (ADR-0071).
    ///
    /// Returns the remote series runs (to be unioned into the local pool and
    /// merged once, since [`merge_soa_runs`] keys by canonical `SeriesId`),
    /// the folded remote `FetchStats`, the deduplicated `skip_unavailable`
    /// warnings, and whether any cluster was skipped (partial coverage).
    ///
    /// This is a named `async fn` on purpose: its future is higher-ranked over
    /// its input lifetimes, so borrows taken across the inner `SliceFetcher`
    /// await stay encapsulated here instead of leaking into the generic
    /// snapshot-resolution future, which must satisfy axum's `Handler` `Send`
    /// bound. The `plan_matchers_windows` argument is owned for the same
    /// reason. No federation configured is a cheap empty result.
    async fn federate_scalar(
        &self,
        tenant_hash: TenantHash,
        plan_matchers_windows: Vec<(Vec<LabelMatcher>, i64, i64)>,
        accounting: &QueryAccounting,
    ) -> Result<(Vec<Vec<FetchedSeriesSoa>>, FetchStats, Vec<String>, bool), QueryError> {
        let mut runs: Vec<Vec<FetchedSeriesSoa>> = Vec::new();
        let mut stats = FetchStats::default();
        let mut warnings: Vec<String> = Vec::new();
        let mut partial = false;
        let Some(federation) = &self.federation else {
            return Ok((runs, stats, warnings, partial));
        };
        for (matchers, start_ns, end_ns) in plan_matchers_windows {
            // Empty erasure and empty min-commit-tokens cross the boundary: the
            // remote resolves its own snapshot and enforces its own erasure,
            // and commit tokens are cluster-local (also structurally prevents
            // leaking this cluster's tokens). The operator credential is baked
            // into the fetcher, so no client credential is threaded here.
            // Owned args (accounting is an `Arc` clone that folds into the same
            // handle) keep the fan-out future higher-ranked `Send`.
            let outcome = federation
                .fetch(
                    tenant_hash,
                    Signal::Metrics,
                    matchers,
                    Vec::new(),
                    start_ns,
                    end_ns,
                    Vec::new(),
                    accounting.clone(),
                    self.config,
                )
                .await?;
            // Re-enforce the coordinator's bytes-scanned budget over the
            // combined local+remote spend now folded into `accounting`, so a
            // tight cap trips typed on the union of both clusters' frames. A
            // budget trip is never masked by skip_unavailable.
            if let Some(err) = bytes_scanned_exceeded(
                accounting.snapshot().total_s3_bytes(),
                self.config.max_bytes_scanned,
            ) {
                return Err(err);
            }
            // `outcome.stats` came back from a remote cluster over the wire;
            // saturate rather than `+=` so a remote reporting an overflowing
            // count cannot panic (debug) or wrap (release) this diagnostic
            // accumulator. Pinning at u64::MAX never affects query results.
            stats.raw_f64_pages = stats
                .raw_f64_pages
                .saturating_add(outcome.stats.raw_f64_pages);
            stats.raw_f64_bytes = stats
                .raw_f64_bytes
                .saturating_add(outcome.stats.raw_f64_bytes);
            partial |= outcome.partial;
            for w in outcome.warnings {
                if !warnings.contains(&w) {
                    warnings.push(w);
                }
            }
            runs.extend(outcome.series);
        }
        Ok((runs, stats, warnings, partial))
    }

    async fn fetch_all_series(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        accounting: &QueryAccounting,
        max_bytes_scanned: ByteLimit,
    ) -> Result<Vec<Vec<ravel_segment::SeriesEntry>>, QueryError> {
        let concurrency = self.config.fetch_concurrency.max(1);
        let matchers: Arc<Vec<LabelMatcher>> = Arc::new(matchers.to_vec());
        let mut stream = stream::iter(snapshot.segments.iter().cloned())
            .map(|seg_ref| {
                let fetcher = self.fetcher.clone();
                let matchers = Arc::clone(&matchers);
                let accounting = accounting.clone();
                async move {
                    fetcher
                        .fetch_series_accounted(
                            tenant_hash,
                            &seg_ref,
                            matchers.as_slice(),
                            &accounting,
                        )
                        .await
                }
            })
            .buffer_unordered(concurrency);
        let mut out = Vec::with_capacity(snapshot.segments.len());
        // Short-circuit the same way as `fetch_all_samples_and_histograms`:
        // the bytes-scanned budget is checked once per completed segment
        // fetch, so a labels/series query whose matchers resolve to zero
        // series still evaluates the budget against every segment fetched here
        // (the caller's `build_series_by_id` loop never runs for an empty
        // result). Returning early drops the stream and cancels the remaining
        // in-flight fetches.
        while let Some(r) = stream.next().await {
            out.push(r?);
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), max_bytes_scanned)
            {
                return Err(err);
            }
        }
        // Selective-erasure exclusion for the labels/label-values/series
        // metadata path (ADR-0064 decision 2, EJ-T3): a series erased by a
        // windowless predicate must not appear in label enumeration either. A
        // windowed predicate erases only some samples and leaves the series
        // enumerable (see `erasure::retain_series_entries`). No-op when empty.
        let erasure = snapshot_erasure_predicates(snapshot);
        if !erasure.is_empty() {
            for entries in &mut out {
                crate::erasure::retain_series_entries(entries, &erasure);
            }
        }
        Ok(out)
    }
}

/// The pending selective-erasure predicates attached to a resolved snapshot
/// (ADR-0064 decision 2, EJ-T3, issue #753). The scan layer excludes every
/// series/sample matching any of these after fetch and after cache.
///
/// EJ-T2 (#752) is the resolver task that lists `t/<th>/<sig>/del/` per resolve
/// and attaches the decoded pending requests to [`Snapshot::pending_erasure`],
/// already scoped to this resolve's (tenant, signal). This is the single
/// connection point between that attachment and the filter machinery: each
/// `ErasureRequest` becomes one [`ErasurePredicate`], mapping its repeated
/// `predicate` matchers into `(key, value)` pairs and carrying the half-open
/// `[window_start_ns, window_end_ns)` event-time bounds through unchanged
/// (`0` on a bound is "unset" per `ErasurePredicate`). The request's other
/// fields (`format_version`, `tenant_hash`, `signal`, `request_id`,
/// `created_unix_ns`, `reason`) play no part in filtering.
///
/// Returns an empty vec when the snapshot carries no pending erasure, which is
/// the common case and makes every call site below a no-op.
pub fn snapshot_erasure_predicates(snapshot: &Snapshot) -> Vec<ErasurePredicate> {
    snapshot
        .pending_erasure
        .iter()
        .map(|request| {
            let matchers = request
                .predicate
                .iter()
                .map(|matcher| (matcher.key.clone(), matcher.value.clone()))
                .collect();
            ErasurePredicate::new(matchers, request.window_start_ns, request.window_end_ns)
        })
        .collect()
}

/// Builds a `series_id -> labels` map incrementally, rejecting the moment a
/// new id would push it past `max_series` (issue #470, ADR-0051 S7 / S4-07)
/// instead of finishing the build and checking the total afterward. Peak map
/// size is therefore bounded by `max_series`, so the cap protects the memory
/// the construction itself consumes, not just the size of the result it
/// would have returned.
fn build_series_by_id(
    entries: impl IntoIterator<Item = (SeriesId, LabelSet)>,
    max_series: usize,
) -> Result<HashMap<SeriesId, LabelSet>, SeriesBuildError> {
    let mut by_id: HashMap<SeriesId, LabelSet> = HashMap::new();
    for (series_id, series_labels) in entries {
        if !by_id.contains_key(&series_id) && by_id.len() >= max_series {
            return Err(SeriesBuildError::TooManySeries(SeriesCapExceeded {
                by_id,
                max: max_series,
            }));
        }
        by_id.entry(series_id).or_insert(series_labels);
    }
    Ok(by_id)
}

/// The partial map at the moment `max_series` was exceeded, kept around
/// (instead of dropped immediately) so a test can assert its length directly
/// rather than trusting only the `count` the resulting [`QueryError`] reports.
#[derive(Debug)]
struct SeriesCapExceeded {
    by_id: HashMap<SeriesId, LabelSet>,
    max: usize,
}

/// Why [`build_series_by_id`] stopped early: the incremental `max_series`
/// count cap. The per-tenant bytes-scanned budget (ADR-0061 decision 1) is no
/// longer enforced here; it moved up into `fetch_all_series`, which returns
/// [`QueryError::TooManyBytesScanned`] directly (via [`bytes_scanned_exceeded`])
/// before the labels/series map is even built, so this build step now has one
/// failure mode.
#[derive(Debug)]
enum SeriesBuildError {
    TooManySeries(SeriesCapExceeded),
}

impl From<SeriesCapExceeded> for QueryError {
    fn from(e: SeriesCapExceeded) -> Self {
        QueryError::TooManySeries {
            count: e.by_id.len() + 1,
            max: e.max,
        }
    }
}

impl From<SeriesBuildError> for QueryError {
    fn from(e: SeriesBuildError) -> Self {
        match e {
            SeriesBuildError::TooManySeries(inner) => inner.into(),
        }
    }
}

/// Returns the bytes-scanned budget error (ADR-0061 decision 1) when
/// `bytes_scanned` has passed a bounded `max_bytes_scanned`, else `None`.
/// Checked once per completed segment fetch inside the two fetch fan-outs
/// ([`QueryEngine::fetch_all_series`] and
/// [`QueryEngine::fetch_all_samples_and_histograms`]), the stage that owns
/// segment concurrency, so a tripped budget cancels the remaining in-flight
/// fetches instead of paying for them; [`ByteLimit::Unlimited`] never trips,
/// so a caller that does not opt in behaves exactly as before.
pub(crate) fn bytes_scanned_exceeded(
    bytes_scanned: u64,
    max_bytes_scanned: ByteLimit,
) -> Option<QueryError> {
    match max_bytes_scanned {
        ByteLimit::Bounded(max) if bytes_scanned > max => Some(QueryError::TooManyBytesScanned {
            scanned: bytes_scanned,
            max,
        }),
        _ => None,
    }
}

/// Collapses the two ways a deadline can surface into the one
/// `QueryError::DeadlineExceeded` callers already match on (issue #193).
///
/// Before the evaluator itself checked a deadline, the only source of
/// `DeadlineExceeded` was `tokio::time::timeout` elapsing (`Err(_)` here,
/// the `Elapsed` case): the query kept running synchronously inside the
/// evaluator with no yield point, so the outer timeout could only ever fire
/// *after* the call returned control to the runtime. The evaluator now
/// checks its own deadline between subquery grid steps and can return
/// `QueryError::Eval(ravel_promql::Error::DeadlineExceeded)` while still
/// well inside the outer timeout's budget (`Ok(Err(..))` here) -- that is
/// the case that actually demonstrates early interruption. Both are the
/// same condition from a caller's perspective, so both collapse to the one
/// variant already documented and tested against.
fn unify_deadline<T>(
    outcome: Result<Result<T, QueryError>, tokio::time::error::Elapsed>,
    deadline: Duration,
) -> Result<T, QueryError> {
    match outcome {
        Err(_) => Err(QueryError::DeadlineExceeded { deadline }),
        Ok(Err(QueryError::Eval(ravel_promql::Error::DeadlineExceeded))) => {
            Err(QueryError::DeadlineExceeded { deadline })
        }
        Ok(result) => result,
    }
}

/// The last evaluation-grid instant `<= end_ns`, starting at `start_ns` and
/// stepping by `step_ns`. Mirrors `ravel_promql::eval`'s own per-step range
/// grid (`while t <= end_ns { ...; t += step_ns }`) exactly, so the fetch
/// window computed from it lines up with what evaluation will request.
fn last_grid_ns(start_ns: i64, end_ns: i64, step_ns: i64) -> Result<i64, QueryError> {
    let span = end_ns
        .checked_sub(start_ns)
        .ok_or(QueryError::TimeOverflow)?;
    let num_steps = span / step_ns;
    let step_span = num_steps
        .checked_mul(step_ns)
        .ok_or(QueryError::TimeOverflow)?;
    start_ns
        .checked_add(step_span)
        .ok_or(QueryError::TimeOverflow)
}

/// Translates one selector's plan (`range_ns`/`offset_ns`/`anchor`) into the
/// concrete window to fetch for it, for either an instant or a range query.
/// `PlanAnchor::Pinned` is already offset-adjusted and constant regardless
/// of the grid; `PlanAnchor::Window` shifts by `offset_ns` off the ambient
/// evaluation instant(s) (docs/query-engine.md "padded_range").
fn selector_fetch_window(
    plan: &SelectorPlan,
    eval_window: &EvalWindow,
) -> Result<TimeRange, QueryError> {
    let (sel_start_ns, sel_end_ns) = match (&plan.anchor, eval_window) {
        (PlanAnchor::Pinned(ts), _) => (*ts, *ts),
        (PlanAnchor::Window, EvalWindow::Instant { t_ns }) => {
            let sel_ts = t_ns
                .checked_sub(plan.offset_ns)
                .ok_or(QueryError::TimeOverflow)?;
            (sel_ts, sel_ts)
        }
        (
            PlanAnchor::Window,
            EvalWindow::Range {
                start_ns,
                end_ns,
                step_ns,
            },
        ) => {
            let last_ns = last_grid_ns(*start_ns, *end_ns, *step_ns)?;
            let sel_start = start_ns
                .checked_sub(plan.offset_ns)
                .ok_or(QueryError::TimeOverflow)?;
            let sel_end = last_ns
                .checked_sub(plan.offset_ns)
                .ok_or(QueryError::TimeOverflow)?;
            (sel_start, sel_end)
        }
    };
    Ok(TimeRange {
        start_ns: sel_start_ns
            .checked_sub(plan.range_ns)
            .ok_or(QueryError::TimeOverflow)?,
        end_ns: sel_end_ns,
    })
}

/// Leading sentinel marking a `name_filter` as a literal-prefix range key
/// rather than an exact `__name__` value (ADR-0061 decision 3, EF-4/#724).
///
/// This MUST equal `ravel_catalog`'s
/// `snapshot_resolve::PREFIX_FILTER_SENTINEL`, the byte the catalog strips to
/// decide the prefix-vs-exact postings lookup. The value is duplicated inline
/// here (matching this codebase's language-specific-enforcement precedent for
/// name filters, #278) rather than shared across the crate boundary; the
/// catalog pins the value with a test and the postings-pruning oracles in this
/// file round-trip it end to end, so a silent drift cannot pass.
const PREFIX_FILTER_SENTINEL: char = '\u{1}';

/// The literal prefix of a fully-anchored `__name__` regex of the exact shape
/// `^literal.*$`, or `None` for every other shape (ADR-0061 decision 3).
///
/// PromQL/SQL regex matchers store the raw pattern text in `LabelMatcher.value`
/// and are always evaluated fully anchored (Prometheus wraps every pattern as
/// `^(?:value)$`). A pattern whose value is `literal.*` therefore matches
/// exactly the names beginning with `literal`, which a sorted-name range scan
/// resolves precisely. This detector accepts ONLY that shape and rejects
/// everything else so a misclassification can never prune a segment a query
/// could match:
///
/// - one optional explicit leading `^` and trailing `$` (redundant with the
///   implicit anchoring) are tolerated;
/// - the remainder MUST end with an unanchored `.*` wildcard tail;
/// - the literal before that tail MUST be non-empty and consist solely of
///   plain metric-name bytes (`[A-Za-z0-9_:]`).
///
/// Anything else -- an infix wildcard (`foo.*bar`), an alternation
/// (`foo|bar`), a character class, `.+`/`foo*`/`.` in the prefix, a
/// non-`.*` tail, or ANY backslash escape (`^a\.b.*$`) -- is rejected and the
/// caller falls back to the pre-existing unpruned resolve. Escapes are
/// deliberately rejected wholesale rather than interpreted: parsing them is
/// where a mis-read prefix would hide, so the conservative choice is to decline
/// (ADR-0061 rejects general regex pruning outright; this stays strictly inside
/// the provably-cheap prefix case).
fn literal_prefix_from_anchored_regex(pattern: &str) -> Option<String> {
    let mut p = pattern;
    p = p.strip_prefix('^').unwrap_or(p);
    p = p.strip_suffix('$').unwrap_or(p);
    let literal = p.strip_suffix(".*")?;
    if literal.is_empty() {
        return None;
    }
    if literal.bytes().all(is_literal_prefix_byte) {
        Some(literal.to_string())
    } else {
        None
    }
}

/// A byte that is unambiguously a literal in a Prometheus-anchored regex and a
/// valid metric-name character. Conservative on purpose: any byte outside this
/// set (including every regex metacharacter and every escape) forces a bypass.
fn is_literal_prefix_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b':'
}

/// The postings pruning key a single `__name__` matcher yields, or `None` if
/// it cannot be soundly used to prune. An equality matcher yields its literal
/// value verbatim (the pre-existing exact case); a prefix-anchored regex
/// matcher (`^foo.*$`) yields the sentinel-encoded literal prefix (ADR-0061
/// decision 3); a negation, a non-prefix regex, or a non-prefix-shaped regex
/// yields `None` (the conservative bypass).
fn name_pruning_key(m: &LabelMatcher) -> Option<Cow<'_, str>> {
    match &m.op {
        MatchOp::Eq => Some(Cow::Borrowed(m.value.as_str())),
        MatchOp::Re(_) => literal_prefix_from_anchored_regex(&m.value)
            .map(|prefix| Cow::Owned(format!("{PREFIX_FILTER_SENTINEL}{prefix}"))),
        MatchOp::Ne | MatchOp::Nre(_) => None,
    }
}

/// The postings pruning key a single `__name__` matcher pins, or `None` if
/// postings pruning must bypass entirely (docs/metric-index-plan.md P5b,
/// ADR-0061 decision 3): no `__name__` matcher at all, more than one
/// `__name__` matcher on the same selector, or a lone `__name__` matcher whose
/// shape is neither an exact `=` nor a literal-prefix-anchored regex (`^foo.*$`)
/// all take the conservative bypass path. The returned key is an exact value
/// as written, or a sentinel-encoded literal prefix ([`name_pruning_key`]).
fn equality_name_filter(matchers: &[LabelMatcher]) -> Option<Cow<'_, str>> {
    let mut found: Option<Cow<'_, str>> = None;
    for m in matchers {
        if m.name != METRIC_NAME_LABEL {
            continue;
        }
        // A second `__name__` matcher of any kind: the pruning key is no longer
        // well defined, so bypass (unchanged from the equality-only behaviour).
        if found.is_some() {
            return None;
        }
        found = Some(name_pruning_key(m)?);
    }
    found
}

/// The `__name__` pruning key shared by every selector in a multi-selector
/// query (docs/metric-index-plan.md P5b, ADR-0061 decision 3). `prefetch`
/// resolves one snapshot shared across all of a query's selectors (e.g.
/// `foo + bar`), so pruning only applies when every selector agrees on the
/// same key; otherwise a filter narrower than some other selector's own
/// matchers would silently drop segments that selector still needs, so this
/// bypasses (`None`) on any disagreement or on any selector with no usable key
/// of its own. Keys are compared post-encoding, so an exact `=foo` and a
/// prefix `^foo.*$` (different encoded strings) never masquerade as a match.
fn shared_equality_name_filter<'a>(plans: &'a [SelectorPlan]) -> Option<Cow<'a, str>> {
    let mut shared: Option<Cow<'a, str>> = None;
    for plan in plans {
        let key = equality_name_filter(&plan.matchers)?;
        match &shared {
            None => shared = Some(key),
            Some(s) if *s == key => {}
            Some(_) => return None,
        }
    }
    shared
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod name_filter_tests {
    use super::*;

    fn encoded(prefix: &str) -> String {
        format!("{PREFIX_FILTER_SENTINEL}{prefix}")
    }

    fn re(pattern: &str) -> LabelMatcher {
        LabelMatcher::regex(METRIC_NAME_LABEL, pattern).expect("compilable regex")
    }

    /// Every pattern the detector must ACCEPT as a literal-prefix-anchored
    /// regex, with the exact literal prefix it must extract.
    #[test]
    fn detector_accepts_literal_prefix_shapes() {
        let accept = [
            ("foo.*", "foo"),
            ("^foo.*$", "foo"),
            ("^foo.*", "foo"),
            ("foo.*$", "foo"),
            ("a1_b:c.*", "a1_b:c"),
            ("^A.*$", "A"),
            ("http_requests_total.*", "http_requests_total"),
        ];
        for (pattern, prefix) in accept {
            assert_eq!(
                literal_prefix_from_anchored_regex(pattern).as_deref(),
                Some(prefix),
                "pattern {pattern:?} must yield prefix {prefix:?}"
            );
        }
    }

    /// Every pattern the detector must REJECT (falling back to the unpruned
    /// path). Grouped by why each is unsafe to treat as a bare literal prefix.
    #[test]
    fn detector_rejects_every_non_prefix_shape() {
        let reject = [
            // Exact-equivalent (no wildcard tail): out of scope, bypass.
            "foo",
            "^foo$",
            // Empty prefix: `.*` matches everything, nothing to prune.
            ".*",
            "^.*$",
            "^$",
            // Infix / trailing-non-`.*` wildcards.
            ".*foo.*",
            "foo.*bar.*",
            "foo.*bar",
            "fo.o.*",
            // Non-`.*` quantifier tails.
            "foo*",
            "foo.+",
            "foo?.*", // '?' lands in the literal portion
            // Alternation and grouping.
            "foo|bar",
            "(foo).*",
            "foo(.*)",
            // Character classes.
            "[a-z].*",
            "foo[0-9].*",
            // Escapes are rejected wholesale rather than interpreted: a
            // detector that does not decode escapes must not mis-parse them
            // (ADR-0061 decision 3). `^a\.b.*$` means literal "a.b" as a
            // prefix, but we conservatively decline it.
            r"a\.b.*",
            r"^a\.b.*$",
            r"foo\d.*",
            // Redundant double anchor leaves a stray metacharacter.
            "^^foo.*$",
            // A metacharacter-only body.
            "^.*.*$",
        ];
        for pattern in reject {
            assert_eq!(
                literal_prefix_from_anchored_regex(pattern),
                None,
                "pattern {pattern:?} must be rejected (unpruned bypass)"
            );
        }
    }

    #[test]
    fn name_pruning_key_encodes_by_matcher_shape() {
        // Exact equality: verbatim value, borrowed.
        assert_eq!(
            name_pruning_key(&LabelMatcher::equal(METRIC_NAME_LABEL, "foo")).as_deref(),
            Some("foo")
        );
        // Prefix regex: sentinel-encoded literal prefix.
        assert_eq!(
            name_pruning_key(&re("^foo.*$")).map(|c| c.into_owned()),
            Some(encoded("foo"))
        );
        // Non-prefix regex, negation, negated regex: all bypass.
        assert!(name_pruning_key(&re("foo|bar")).is_none());
        assert!(name_pruning_key(&re(".*foo.*")).is_none());
        assert!(name_pruning_key(&LabelMatcher::not_equal(METRIC_NAME_LABEL, "foo")).is_none());
        assert!(
            name_pruning_key(&LabelMatcher::not_regex(METRIC_NAME_LABEL, "^foo.*$").unwrap())
                .is_none()
        );
    }

    #[test]
    fn equality_name_filter_extends_to_prefix_and_preserves_bypass() {
        // Exact case unchanged.
        assert_eq!(
            equality_name_filter(&[LabelMatcher::equal(METRIC_NAME_LABEL, "foo")]).as_deref(),
            Some("foo")
        );
        // Prefix regex now prunes.
        assert_eq!(
            equality_name_filter(&[re("^foo.*$")]).map(|c| c.into_owned()),
            Some(encoded("foo"))
        );
        // A non-`__name__` matcher alongside is ignored.
        let mixed = vec![LabelMatcher::equal("job", "api"), re("^foo.*$")];
        assert_eq!(
            equality_name_filter(&mixed).map(|c| c.into_owned()),
            Some(encoded("foo"))
        );
        // Two `__name__` matchers: bypass, even when each alone would prune.
        let two = vec![LabelMatcher::equal(METRIC_NAME_LABEL, "foo"), re("^foo.*$")];
        assert!(equality_name_filter(&two).is_none());
        // No `__name__` matcher: bypass.
        assert!(equality_name_filter(&[LabelMatcher::equal("job", "api")]).is_none());
        // Non-prefix `__name__` regex: bypass.
        assert!(equality_name_filter(&[re(".*foo.*")]).is_none());
    }

    #[test]
    fn shared_filter_agrees_only_on_identical_keys() {
        let plan = |m: LabelMatcher| SelectorPlan {
            matchers: vec![m],
            range_ns: 0,
            offset_ns: 0,
            anchor: PlanAnchor::Window,
        };

        // Same prefix across selectors: shared prefix key.
        let same = vec![plan(re("^foo.*$")), plan(re("^foo.*$"))];
        assert_eq!(
            shared_equality_name_filter(&same).map(|c| c.into_owned()),
            Some(encoded("foo"))
        );

        // Exact `foo` vs prefix `^foo.*$`: different encoded keys, so bypass
        // (the prefix set is a superset of the exact one; pruning to the
        // narrower exact key would drop segments the prefix selector needs).
        let exact_vs_prefix = vec![
            plan(LabelMatcher::equal(METRIC_NAME_LABEL, "foo")),
            plan(re("^foo.*$")),
        ];
        assert!(shared_equality_name_filter(&exact_vs_prefix).is_none());

        // Two different prefixes: bypass.
        let diff = vec![plan(re("^foo.*$")), plan(re("^bar.*$"))];
        assert!(shared_equality_name_filter(&diff).is_none());

        // One selector with no usable key: bypass the whole query.
        let one_bypasses = vec![plan(re("^foo.*$")), plan(re("foo|bar"))];
        assert!(shared_equality_name_filter(&one_bypasses).is_none());
    }
}

/// Parses `query` as a bare vector selector (Phase 1 scope) and returns its
/// matchers plus signed offset in nanoseconds. Mirrors the rejection rules
/// of `ravel_promql::eval`'s private pre-parse (that logic isn't exported),
/// so the pre-fetch window this computes lines up with what the evaluator
/// itself will later select.
fn parse_selector(query: &str) -> Result<(Vec<LabelMatcher>, i64), QueryError> {
    let expr = promql_parser::parser::parse(query).map_err(QueryError::Parse)?;
    let vs = match expr {
        Expr::VectorSelector(vs) => vs,
        other => {
            return Err(QueryError::Unsupported {
                construct: describe_expr(&other),
            });
        }
    };
    if vs.at.is_some() {
        return Err(QueryError::Unsupported {
            construct: "@ modifier".to_string(),
        });
    }
    if has_or_group(&vs.matchers) {
        return Err(QueryError::Unsupported {
            construct: "or-grouped label matchers".to_string(),
        });
    }
    let mut matchers = from_ast_matchers(&vs.matchers);
    if let Some(name) = &vs.name {
        matchers.push(LabelMatcher::equal(METRIC_NAME_LABEL, name.clone()));
    }
    let offset_ns = signed_offset_ns(vs.offset.as_ref())?;
    Ok((matchers, offset_ns))
}

/// Parses a `match[]` selector for the labels/label-values/series HTTP
/// endpoints: same grammar as [`parse_selector`], but the offset (if any)
/// is meaningless for a point-in-time label lookup and is discarded.
pub(crate) fn parse_match_selector(selector: &str) -> Result<Vec<LabelMatcher>, QueryError> {
    let (matchers, _offset_ns) = parse_selector(selector)?;
    Ok(matchers)
}

fn signed_offset_ns(offset: Option<&Offset>) -> Result<i64, QueryError> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    let (duration, sign) = match offset {
        Offset::Pos(d) => (d, 1i64),
        Offset::Neg(d) => (d, -1i64),
    };
    let ns = i64::try_from(duration.as_nanos()).map_err(|_| QueryError::TimeOverflow)?;
    ns.checked_mul(sign).ok_or(QueryError::TimeOverflow)
}

fn describe_expr(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate(_) => "aggregation".to_string(),
        Expr::Unary(_) => "unary expression".to_string(),
        Expr::Binary(_) => "binary expression".to_string(),
        Expr::Paren(_) => "parenthesized expression".to_string(),
        Expr::Subquery(_) => "subquery".to_string(),
        Expr::NumberLiteral(_) => "number literal".to_string(),
        Expr::StringLiteral(_) => "string literal".to_string(),
        Expr::VectorSelector(_) => "vector selector".to_string(),
        Expr::MatrixSelector(_) => "matrix selector (range vector)".to_string(),
        Expr::Call(_) => "function call".to_string(),
        Expr::Extension(_) => "promql-parser extension".to_string(),
    }
}

/// One candidate sample for a (series, timestamp) slot, carrying the
/// ordering tuple from ADR-0010 §5 plus the raw value, for the
/// greatest-wins comparison in [`is_greater`]. The timestamp is the merge
/// key and lives outside the candidate; only the fields that break a
/// duplicate-timestamp tie are held here.
#[derive(Debug, Clone, Copy)]
struct Candidate {
    value: f64,
    priority: (i64, u64, u64, u32),
}

/// Cross-segment (and within-segment) duplicate-sample total order
/// (docs/catalog-and-mvcc.md): `(created_unix_ns, writer_epoch, writer_seq,
/// in-page index)`, greatest wins; ties broken by raw value bit pattern for
/// full determinism.
fn is_greater(a: &Candidate, b: &Candidate) -> bool {
    (a.priority, a.value.to_bits()) > (b.priority, b.value.to_bits())
}

/// One decoded per-segment run of a single series, kept in on-disk order
/// (ascending ts, duplicate timestamps preserved; docs/segment-format.md
/// "Sample order within a page"). The run-wide prefix of the ADR-0010 §5
/// order is shared by every sample in the run; the per-sample in-page index
/// (position in `timestamps`) completes the tuple.
struct SeriesRun {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    prefix: (i64, u64, u64),
}

/// Counts samples as the k-way merge *yields* them (post-dedup), enforcing
/// the max-samples budget on the yielded stream rather than on a fully
/// materialized window. The budget trips at exactly `max + 1`, so peak work
/// is bounded by the budget itself, not by the query's full result size
/// (docs/query-engine.md "Budgets", count-yielded semantics).
struct YieldBudget {
    yielded: usize,
    max: usize,
}

impl YieldBudget {
    fn count_one(&mut self) -> Result<(), QueryError> {
        self.yielded += 1;
        if self.yielded > self.max {
            return Err(QueryError::TooManySamples {
                count: self.yielded,
                max: self.max,
            });
        }
        Ok(())
    }
}

/// Groups the per-segment SoA runs by series id, then lazily k-way merges
/// each series' runs into ascending, per-timestamp-deduplicated samples.
/// Replaces the old materialized per-timestamp map ("merged window"): no
/// `HashMap<ts, _>` is built and no final sort runs, because each run is
/// already sorted ascending by ts and the merge emits in ts order. The
/// max-samples budget is enforced by counting yielded samples
/// ([`YieldBudget`]); duplicate timestamps resolve under the full total
/// order in [`is_greater`]. `max_series` is enforced incrementally, the
/// moment a new series id would push the map past the cap (issue #470,
/// ADR-0051 S7 / S4-07), so peak map size is bounded by the cap rather than
/// by however many distinct series the fetch actually returned.
// `pub(crate)` (not private) so the ADR-0071 distributed acceptance test
// (`distrib::tests`) merges a distributed fetch with the exact same total-order
// k-way merge the local path uses, proving byte-identical results rather than
// re-implementing the merge in the test.
pub(crate) fn merge_soa_runs(
    fetched: Vec<Vec<FetchedSeriesSoa>>,
    max_series: usize,
    max_samples: usize,
) -> Result<Vec<SeriesData>, QueryError> {
    let mut by_series: HashMap<SeriesId, (LabelSet, Vec<SeriesRun>)> = HashMap::new();
    for segment_series in fetched {
        for fs in segment_series {
            if !by_series.contains_key(&fs.series_id) && by_series.len() >= max_series {
                return Err(QueryError::TooManySeries {
                    count: by_series.len() + 1,
                    max: max_series,
                });
            }
            let entry = by_series
                .entry(fs.series_id)
                .or_insert_with(|| (fs.labels.clone(), Vec::new()));
            entry.1.push(SeriesRun {
                timestamps: fs.timestamps,
                values: fs.values,
                prefix: (fs.created_unix_ns, fs.writer_epoch, fs.writer_seq),
            });
        }
    }

    let mut budget = YieldBudget {
        yielded: 0,
        max: max_samples,
    };
    let mut out = Vec::with_capacity(by_series.len());
    for (labels, runs) in by_series.into_values() {
        let mut samples = Vec::new();
        merge_series_runs(&runs, &mut budget, &mut samples)?;
        out.push(SeriesData { labels, samples });
    }
    Ok(out)
}

/// Lazily k-way merges one series' per-segment `runs` into `out`, ascending
/// by timestamp with one sample per timestamp. Each run is individually
/// sorted ascending by ts (RSEG page order), so a min-heap keyed by ts
/// suffices; there is no full sort. At each distinct timestamp every
/// candidate across all runs (including duplicate timestamps within a run)
/// is considered and the greatest under [`is_greater`] wins, never arrival
/// order. Every emitted sample is counted through `budget`.
fn merge_series_runs(
    runs: &[SeriesRun],
    budget: &mut YieldBudget,
    out: &mut Vec<Sample>,
) -> Result<(), QueryError> {
    // Min-heap of each run's current head as (ts, run_idx); `Reverse` turns
    // the max-heap into a min-heap so the smallest pending ts pops first.
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();
    let mut cursors = vec![0usize; runs.len()];
    for (idx, run) in runs.iter().enumerate() {
        if let Some(&ts) = run.timestamps.first() {
            heap.push(Reverse((ts, idx)));
        }
    }

    while let Some(&Reverse((ts, _))) = heap.peek() {
        // Gather the winner across every run head equal to `ts`, draining
        // each run's consecutive equal-ts streak before advancing it.
        let mut best: Option<Candidate> = None;
        while let Some(Reverse((head_ts, idx))) = heap.pop() {
            if head_ts != ts {
                heap.push(Reverse((head_ts, idx)));
                break;
            }
            let run = &runs[idx];
            let mut pos = cursors[idx];
            while pos < run.timestamps.len() && run.timestamps[pos] == ts {
                let in_page_index = u32::try_from(pos).unwrap_or(u32::MAX);
                let candidate = Candidate {
                    value: run.values[pos],
                    priority: (run.prefix.0, run.prefix.1, run.prefix.2, in_page_index),
                };
                best = match best {
                    Some(current) if is_greater(&current, &candidate) => Some(current),
                    _ => Some(candidate),
                };
                pos += 1;
            }
            cursors[idx] = pos;
            if let Some(&next_ts) = run.timestamps.get(pos) {
                // A run is a decoded RSEG page and must be ascending
                // (docs/segment-format.md "Sample order within a page"). The
                // drain above stops only once `next_ts != ts`; if it fell
                // below `ts` the run itself is out of order, which would
                // otherwise re-enter the heap as a fresh, already-visited
                // minimum and double-emit or misorder output.
                if next_ts < ts {
                    return Err(QueryError::NonMonotonicSamples {
                        prev: ts,
                        next: next_ts,
                    });
                }
                heap.push(Reverse((next_ts, idx)));
            }
        }
        if let Some(candidate) = best {
            budget.count_one()?;
            out.push(Sample {
                ts_ns: ts,
                value: candidate.value,
            });
        }
    }
    Ok(())
}

// --- Native-histogram dedup total order (docs/rseg-v3-plan.md section 7) ---
//
// Generalizes `is_greater`/`merge_series_runs` to histogram structural
// comparison by bit pattern. Wired into the live query path (#218):
// `merge_histogram_soa_runs` groups the per-segment histogram runs
// `SegmentFetcher::fetch_histograms` decodes and drains them through
// `merge_histogram_series_runs` below, and `MergedSource::query_histograms`
// serves the converted result to the evaluator's native-histogram path. The
// tests below additionally prove the merge order directly, constructing
// `HistogramValue`s by hand rather than via OTLP/RW decode, mirroring how
// phase C7's ingest plumbing is proven.

/// Bit-pattern total order over one [`HistogramValue`]'s full structure:
/// every float field compared by its raw bit pattern (never `==`/`PartialOrd`
/// on `f64` itself), so NaN payloads and -0.0 are significant, matching
/// `f64::to_bits()`'s role in the scalar tiebreak ([`is_greater`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HistogramSortKey {
    scale: i32,
    zero_threshold_bits: u64,
    sum_bits: Option<u64>,
    custom_values_bits: Option<Vec<u64>>,
    positive_spans: Vec<(i32, u32)>,
    negative_spans: Vec<(i32, u32)>,
    counts: CountsSortKey,
    reset_hint_rank: u8,
}

/// Bit-pattern projection of [`ravel_segment::HistogramCounts`]. Variant
/// order (`Int` before `Float`) gives the two kinds a stable, deterministic
/// relative rank; nothing in the format requires one kind to structurally
/// outrank the other, so any fixed order is correct as long as it never
/// changes (it decides duplicate-sample winners).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CountsSortKey {
    Int {
        zero_count: u64,
        count: u64,
        positive: Vec<u64>,
        negative: Vec<u64>,
    },
    Float {
        zero_count_bits: u64,
        count_bits: u64,
        positive_bits: Vec<u64>,
        negative_bits: Vec<u64>,
    },
}

/// Stable, deterministic rank for [`ravel_segment::ResetHint`] (which has no
/// `Ord` of its own): only used to make [`HistogramSortKey`] total, not tied
/// to the format's wire encoding.
fn reset_hint_rank(hint: ravel_segment::ResetHint) -> u8 {
    use ravel_segment::ResetHint;
    match hint {
        ResetHint::Unknown => 0,
        ResetHint::Yes => 1,
        ResetHint::No => 2,
        ResetHint::Gauge => 3,
    }
}

fn spans_sort_key(spans: &[ravel_segment::HistogramSpan]) -> Vec<(i32, u32)> {
    spans.iter().map(|s| (s.offset, s.length)).collect()
}

fn histogram_sort_key(v: &ravel_segment::HistogramValue) -> HistogramSortKey {
    use ravel_segment::HistogramCounts;

    let counts = match &v.counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => CountsSortKey::Int {
            zero_count: *zero_count,
            count: *count,
            positive: positive.clone(),
            negative: negative.clone(),
        },
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => CountsSortKey::Float {
            zero_count_bits: zero_count.to_bits(),
            count_bits: count.to_bits(),
            positive_bits: positive.iter().map(|f| f.to_bits()).collect(),
            negative_bits: negative.iter().map(|f| f.to_bits()).collect(),
        },
    };
    HistogramSortKey {
        scale: v.scale,
        zero_threshold_bits: v.zero_threshold.to_bits(),
        sum_bits: v.sum.map(f64::to_bits),
        custom_values_bits: v
            .custom_values
            .as_ref()
            .map(|cv| cv.iter().map(|f| f.to_bits()).collect()),
        positive_spans: spans_sort_key(&v.positive_spans),
        negative_spans: spans_sort_key(&v.negative_spans),
        counts,
        reset_hint_rank: reset_hint_rank(v.reset_hint),
    }
}

/// Histogram counterpart to [`Candidate`]: same provenance priority, paired
/// with a full [`HistogramValue`] instead of a plain `f64`.
struct HistogramCandidate {
    value: ravel_segment::HistogramValue,
    priority: (i64, u64, u64, u32),
}

/// Histogram counterpart to [`is_greater`]: same priority-prefix-first
/// total order, tie-broken by [`histogram_sort_key`] instead of a plain
/// `value.to_bits()`.
fn histogram_is_greater(a: &HistogramCandidate, b: &HistogramCandidate) -> bool {
    (a.priority, histogram_sort_key(&a.value)) > (b.priority, histogram_sort_key(&b.value))
}

/// Histogram counterpart to [`SeriesRun`]: one decoded per-segment run of a
/// single histogram-kind series, kept in on-disk order.
#[derive(Debug, Clone)]
struct HistogramSeriesRun {
    timestamps: Vec<i64>,
    values: Vec<ravel_segment::HistogramValue>,
    prefix: (i64, u64, u64),
}

/// Histogram counterpart to [`merge_series_runs`]: identical k-way merge
/// shape (min-heap by ts, drain same-ts heads, [`histogram_is_greater`]
/// picks the winner), substituting [`HistogramCandidate`] for [`Candidate`]
/// and yielding [`ravel_segment::HistogramSample`]s.
fn merge_histogram_series_runs(
    runs: &[HistogramSeriesRun],
    budget: &mut YieldBudget,
    out: &mut Vec<ravel_segment::HistogramSample>,
) -> Result<(), QueryError> {
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();
    let mut cursors = vec![0usize; runs.len()];
    for (idx, run) in runs.iter().enumerate() {
        if let Some(&ts) = run.timestamps.first() {
            heap.push(Reverse((ts, idx)));
        }
    }

    while let Some(&Reverse((ts, _))) = heap.peek() {
        let mut best: Option<HistogramCandidate> = None;
        while let Some(Reverse((head_ts, idx))) = heap.pop() {
            if head_ts != ts {
                heap.push(Reverse((head_ts, idx)));
                break;
            }
            let run = &runs[idx];
            let mut pos = cursors[idx];
            while pos < run.timestamps.len() && run.timestamps[pos] == ts {
                let in_page_index = u32::try_from(pos).unwrap_or(u32::MAX);
                let candidate = HistogramCandidate {
                    value: run.values[pos].clone(),
                    priority: (run.prefix.0, run.prefix.1, run.prefix.2, in_page_index),
                };
                best = match best {
                    Some(current) if histogram_is_greater(&current, &candidate) => Some(current),
                    _ => Some(candidate),
                };
                pos += 1;
            }
            cursors[idx] = pos;
            if let Some(&next_ts) = run.timestamps.get(pos) {
                if next_ts < ts {
                    return Err(QueryError::NonMonotonicSamples {
                        prev: ts,
                        next: next_ts,
                    });
                }
                heap.push(Reverse((next_ts, idx)));
            }
        }
        if let Some(candidate) = best {
            budget.count_one()?;
            out.push(ravel_segment::HistogramSample {
                ts_ns: ts,
                value: candidate.value,
            });
        }
    }
    Ok(())
}

/// Converts one storage-model [`ravel_segment::HistogramValue`] to the
/// evaluator's float working model [`FloatHistogram`] (#218). Integer counts
/// are widened to `f64` exactly as Prometheus converts an integer histogram to
/// its `FloatHistogram` before evaluation (`ravel_promql::source::HistogramSample`
/// doc); span layouts and float fields carry over verbatim. A sample with no
/// stored `sum` maps to `NaN` (the honest "sum unknown" float value); every
/// native histogram Ravel ingests today carries a sum, so this is the
/// unreachable-in-practice edge, not the norm.
fn histogram_value_to_float(v: &ravel_segment::HistogramValue) -> FloatHistogram {
    use ravel_segment::HistogramCounts;

    let spans = |src: &[ravel_segment::HistogramSpan]| -> Vec<Span> {
        src.iter()
            .map(|s| Span {
                offset: s.offset,
                length: s.length,
            })
            .collect()
    };

    let (zero_count, count, positive_buckets, negative_buckets) = match &v.counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => (
            *zero_count as f64,
            *count as f64,
            positive.iter().map(|&c| c as f64).collect(),
            negative.iter().map(|&c| c as f64).collect(),
        ),
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => (*zero_count, *count, positive.clone(), negative.clone()),
    };

    FloatHistogram {
        counter_reset_hint: reset_hint_to_float(v.reset_hint),
        scale: v.scale,
        zero_threshold: v.zero_threshold,
        zero_count,
        count,
        sum: v.sum.unwrap_or(f64::NAN),
        positive_spans: spans(&v.positive_spans),
        negative_spans: spans(&v.negative_spans),
        positive_buckets,
        negative_buckets,
        custom_values: v.custom_values.clone().unwrap_or_default(),
    }
}

/// Maps [`ravel_segment::ResetHint`] to the evaluator's own
/// [`ravel_promql::ResetHint`] (identical four states, distinct types across
/// the crate boundary).
fn reset_hint_to_float(hint: ravel_segment::ResetHint) -> ravel_promql::ResetHint {
    use ravel_promql::ResetHint as F;
    use ravel_segment::ResetHint as S;
    match hint {
        S::Unknown => F::Unknown,
        S::Yes => F::Yes,
        S::No => F::No,
        S::Gauge => F::Gauge,
    }
}

/// Histogram counterpart to [`merge_soa_runs`] (#218): groups the per-segment
/// histogram runs by series id, lazily k-way merges each series' runs through
/// [`merge_histogram_series_runs`] (same ascending-ts, per-timestamp-dedup,
/// full-total-order winner selection as the scalar merge), then converts each
/// merged storage-model sample to the evaluator's float model for the built
/// [`HistogramSeriesData`]. `max_series`/`max_samples` are enforced exactly as
/// on the scalar path.
fn merge_histogram_soa_runs(
    fetched: Vec<Vec<FetchedHistogramSeries>>,
    max_series: usize,
    max_samples: usize,
) -> Result<Vec<HistogramSeriesData>, QueryError> {
    let mut by_series: HashMap<SeriesId, (LabelSet, Vec<HistogramSeriesRun>)> = HashMap::new();
    for segment_series in fetched {
        for fs in segment_series {
            if !by_series.contains_key(&fs.series_id) && by_series.len() >= max_series {
                return Err(QueryError::TooManySeries {
                    count: by_series.len() + 1,
                    max: max_series,
                });
            }
            let entry = by_series
                .entry(fs.series_id)
                .or_insert_with(|| (fs.labels.clone(), Vec::new()));
            entry.1.push(HistogramSeriesRun {
                timestamps: fs.timestamps,
                values: fs.values,
                prefix: (fs.created_unix_ns, fs.writer_epoch, fs.writer_seq),
            });
        }
    }

    let mut budget = YieldBudget {
        yielded: 0,
        max: max_samples,
    };
    let mut out = Vec::with_capacity(by_series.len());
    for (labels, runs) in by_series.into_values() {
        let mut merged = Vec::new();
        merge_histogram_series_runs(&runs, &mut budget, &mut merged)?;
        let samples = merged
            .into_iter()
            .map(|s| ravel_promql::HistogramSample {
                ts_ns: s.ts_ns,
                value: histogram_value_to_float(&s.value),
            })
            .collect();
        out.push(HistogramSeriesData { labels, samples });
    }
    Ok(out)
}

/// A `SeriesSource` backed by the per-series output of the lazy k-way merge
/// ([`merge_soa_runs`] for scalar series, [`merge_histogram_soa_runs`] for
/// native-histogram series), not a materialized per-timestamp window
/// (docs/query-engine.md / `ravel_promql::source` module doc: by the time
/// the evaluator runs, everything is plain, synchronous, in-memory data).
struct MergedSource {
    series: Vec<SeriesData>,
    histogram_series: Vec<HistogramSeriesData>,
}

impl SeriesSource for MergedSource {
    fn query(
        &self,
        matchers: &[LabelMatcher],
        window: TimeRange,
    ) -> Result<Vec<SeriesData>, SourceError> {
        Ok(self
            .series
            .iter()
            .filter(|s| matches_series(matchers, &s.labels))
            .map(|s| SeriesData {
                labels: s.labels.clone(),
                samples: s
                    .samples
                    .iter()
                    .filter(|sm| sm.ts_ns >= window.start_ns && sm.ts_ns <= window.end_ns)
                    .copied()
                    .collect(),
            })
            .collect())
    }

    /// Native-histogram counterpart to [`query`](Self::query) (#218): the same
    /// matcher-filter-then-window-clip over the already-merged histogram
    /// series. A series is scalar or histogram in storage, never both, so this
    /// and `query` partition the merged source with no overlap.
    fn query_histograms(
        &self,
        matchers: &[LabelMatcher],
        window: TimeRange,
    ) -> Result<Vec<HistogramSeriesData>, SourceError> {
        Ok(self
            .histogram_series
            .iter()
            .filter(|s| matches_series(matchers, &s.labels))
            .map(|s| HistogramSeriesData {
                labels: s.labels.clone(),
                samples: s
                    .samples
                    .iter()
                    .filter(|sm| sm.ts_ns >= window.start_ns && sm.ts_ns <= window.end_ns)
                    .cloned()
                    .collect(),
            })
            .collect())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod merge_tests {
    use ravel_types::{Label, LabelSet, SeriesId};

    use super::*;
    use crate::fetcher::FetchedSeriesSoa;

    fn labels(series: u8) -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: format!("metric_{series}"),
        }])
        .expect("valid labels")
    }

    /// One decoded per-segment run of `series` at the given provenance.
    #[allow(clippy::too_many_arguments)]
    fn run(
        series: u8,
        ts: &[i64],
        vals: &[f64],
        created: i64,
        epoch: u64,
        seq: u64,
    ) -> FetchedSeriesSoa {
        assert_eq!(ts.len(), vals.len());
        FetchedSeriesSoa {
            series_id: SeriesId([series; 16]),
            labels: labels(series),
            timestamps: ts.to_vec(),
            values: vals.to_vec(),
            created_unix_ns: created,
            writer_epoch: epoch,
            writer_seq: seq,
        }
    }

    /// Merges with generous budgets and returns the per-series output.
    fn merge(segments: Vec<Vec<FetchedSeriesSoa>>) -> Vec<SeriesData> {
        merge_soa_runs(segments, 10_000, usize::MAX).expect("merge")
    }

    /// Merges a single-series, single-timestamp scenario and returns the raw
    /// bits of the one winning sample's value, so bit-exact ties are checked
    /// without float `==`.
    fn winner_bits(segments: Vec<Vec<FetchedSeriesSoa>>) -> u64 {
        let mut out = merge(segments);
        assert_eq!(out.len(), 1, "exactly one series expected");
        let series = out.pop().expect("one series");
        assert_eq!(series.samples.len(), 1, "exactly one timestamp expected");
        series.samples[0].value.to_bits()
    }

    #[test]
    fn created_unix_ns_decides_winner() {
        // Two segments, same ts, differ only in created_unix_ns: greatest wins.
        let older = run(1, &[5], &[1.0], 10, 0, 0);
        let newer = run(1, &[5], &[2.0], 20, 0, 0);
        assert_eq!(
            winner_bits(vec![vec![older.clone()], vec![newer.clone()]]),
            2.0f64.to_bits()
        );
        // Order-independent: swap the two segments, same winner.
        assert_eq!(
            winner_bits(vec![vec![newer], vec![older]]),
            2.0f64.to_bits()
        );
    }

    #[test]
    fn writer_epoch_decides_when_created_ties() {
        let lo = run(1, &[5], &[1.0], 100, 1, 9);
        let hi = run(1, &[5], &[2.0], 100, 2, 0);
        assert_eq!(
            winner_bits(vec![vec![lo.clone()], vec![hi.clone()]]),
            2.0f64.to_bits()
        );
        assert_eq!(winner_bits(vec![vec![hi], vec![lo]]), 2.0f64.to_bits());
    }

    #[test]
    fn writer_seq_decides_when_created_and_epoch_tie() {
        let lo = run(1, &[5], &[1.0], 100, 3, 7);
        let hi = run(1, &[5], &[2.0], 100, 3, 8);
        assert_eq!(
            winner_bits(vec![vec![lo.clone()], vec![hi.clone()]]),
            2.0f64.to_bits()
        );
        assert_eq!(winner_bits(vec![vec![hi], vec![lo]]), 2.0f64.to_bits());
    }

    #[test]
    fn in_page_index_decides_within_one_segment() {
        // One run, duplicate ts: the later position (greater in-page index)
        // wins even though the whole provenance prefix is identical.
        let dup = run(1, &[5, 5], &[1.0, 2.0], 100, 3, 8);
        assert_eq!(winner_bits(vec![vec![dup]]), 2.0f64.to_bits());
    }

    #[test]
    fn in_page_index_decides_across_segments() {
        // Same provenance prefix in two segments, but one run carries the
        // sample at index 1 (a leading same-ts sample), the other at index 0.
        // Greater index wins regardless of value magnitude.
        let idx0 = run(1, &[5], &[100.0], 100, 3, 8);
        let idx1 = run(1, &[5, 5], &[1.0, 2.0], 100, 3, 8);
        // idx1's winning candidate is at position 1 (value 2.0); across the
        // two runs it beats idx0's position-0 candidate (value 100.0).
        assert_eq!(winner_bits(vec![vec![idx0], vec![idx1]]), 2.0f64.to_bits());
    }

    #[test]
    fn value_bits_break_full_priority_tie() {
        // Two segments, identical provenance, each a single index-0 sample:
        // the full priority tuple ties, so value.to_bits() (greatest) decides.
        // -1.0 has the sign bit set, so its bit pattern exceeds 1.0's despite
        // being numerically smaller: proves the tiebreak is on bits, not value.
        let pos = run(1, &[5], &[1.0], 100, 3, 8);
        let neg = run(1, &[5], &[-1.0], 100, 3, 8);
        assert_eq!(winner_bits(vec![vec![pos], vec![neg]]), (-1.0f64).to_bits());
    }

    #[test]
    fn k_way_merge_yields_sorted_deduped_stream() {
        // Interleaved timestamps across three runs, plus a cross-segment
        // duplicate at ts=3 (newer created wins) and a within-run duplicate
        // at ts=5 in run b.
        let a = run(1, &[1, 4], &[10.0, 40.0], 5, 0, 0);
        let b = run(1, &[3, 5, 5], &[3.1, 5.0, 5.9], 5, 0, 1);
        let c = run(1, &[2, 3], &[2.0, 3.2], 9, 0, 0); // ts=3 newer created
        let out = merge(vec![vec![a], vec![b], vec![c]]);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        let got: Vec<(i64, u64)> = s
            .samples
            .iter()
            .map(|x| (x.ts_ns, x.value.to_bits()))
            .collect();
        let want: Vec<(i64, u64)> = vec![
            (1, 10.0f64.to_bits()),
            (2, 2.0f64.to_bits()),
            (3, 3.2f64.to_bits()), // c's created=9 beats b's created=5
            (4, 40.0f64.to_bits()),
            (5, 5.9f64.to_bits()), // within-run: index 2 beats index 1
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn distinct_series_produce_distinct_output() {
        let s1 = run(1, &[1, 2], &[1.0, 2.0], 0, 0, 0);
        let s2 = run(2, &[3], &[3.0], 0, 0, 0);
        let out = merge(vec![vec![s1, s2]]);
        assert_eq!(out.len(), 2);
        let total: usize = out.iter().map(|s| s.samples.len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn budget_trips_at_max_plus_one_on_yield() {
        // Three distinct samples, budget of 2: the merge must error on the
        // third yielded sample, reporting count = max + 1 exactly.
        let seg = vec![vec![run(1, &[1, 2, 3], &[1.0, 2.0, 3.0], 0, 0, 0)]];
        let err = merge_soa_runs(seg, 10_000, 2).expect_err("over budget");
        match err {
            QueryError::TooManySamples { count, max } => {
                assert_eq!(count, 3);
                assert_eq!(max, 2);
            }
            other => panic!("expected TooManySamples, got {other:?}"),
        }
    }

    #[test]
    fn budget_counts_yielded_not_materialized() {
        // Duplicates collapse before counting: two segments each with the
        // same three timestamps yield only three samples, within a budget of
        // three. A count-materialized budget would have seen six.
        let a = run(1, &[1, 2, 3], &[1.0, 2.0, 3.0], 1, 0, 0);
        let b = run(1, &[1, 2, 3], &[9.0, 9.0, 9.0], 2, 0, 0);
        let out = merge_soa_runs(vec![vec![a], vec![b]], 10_000, 3).expect("within budget");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].samples.len(), 3);
    }

    #[test]
    fn budget_error_is_order_independent() {
        // Same sample set, two segment orderings: the tripped count is
        // identical because it counts yielded, not iteration order.
        let a = run(1, &[1, 3], &[1.0, 3.0], 0, 0, 0);
        let b = run(1, &[2, 4], &[2.0, 4.0], 0, 0, 0);
        let e1 = merge_soa_runs(vec![vec![a.clone()], vec![b.clone()]], 10_000, 3)
            .expect_err("over budget");
        let e2 = merge_soa_runs(vec![vec![b], vec![a]], 10_000, 3).expect_err("over budget");
        let count = |e: &QueryError| match e {
            QueryError::TooManySamples { count, .. } => *count,
            other => panic!("expected TooManySamples, got {other:?}"),
        };
        assert_eq!(count(&e1), 4);
        assert_eq!(count(&e2), 4);
    }

    #[test]
    fn max_series_enforced() {
        let seg = vec![vec![
            run(1, &[1], &[1.0], 0, 0, 0),
            run(2, &[1], &[1.0], 0, 0, 0),
        ]];
        let err = merge_soa_runs(seg, 1, usize::MAX).expect_err("too many series");
        match err {
            QueryError::TooManySeries { count, max } => {
                assert_eq!(count, 2);
                assert_eq!(max, 1);
            }
            other => panic!("expected TooManySeries, got {other:?}"),
        }
    }

    // The bytes-scanned budget (ADR-0061 decision 1, issue #721) is no longer
    // enforced inside `merge_soa_runs`; it moved into the two fetch fan-outs so
    // it can cancel a query mid-scan rather than after every byte is already
    // fetched. The end-to-end proof (real cancellation, GET-count short-circuit,
    // and the empty-input case) lives in tests/bytes_scanned_budget.rs; the
    // former synchronous merge-level unit tests could not prove cancellation
    // and were removed with the check they exercised.

    #[test]
    fn non_ascending_run_is_a_typed_error_not_wrong_data() {
        // Run A itself is not ascending (ts=[5,3]), violating the RSEG page
        // order precondition. Without a guard this reintroduces ts=5's
        // remainder as a fresh heap minimum after ts=3 has already been
        // processed, silently double-emitting and misordering output
        // instead of surfacing the corruption.
        let a = run(1, &[5, 3], &[50.0, 30.0], 100, 0, 0);
        let b = run(1, &[3], &[99.0], 1, 0, 0);
        let err = merge_soa_runs(vec![vec![a], vec![b]], 10_000, usize::MAX)
            .expect_err("non-ascending run must error, not emit reordered data");
        match err {
            QueryError::NonMonotonicSamples { prev, next } => {
                assert_eq!(prev, 5);
                assert_eq!(next, 3);
            }
            other => panic!("expected NonMonotonicSamples, got {other:?}"),
        }
    }

    #[test]
    fn empty_run_yields_empty_series() {
        let seg = vec![vec![run(1, &[], &[], 0, 0, 0)]];
        let out = merge(seg);
        assert_eq!(out.len(), 1);
        assert!(out[0].samples.is_empty());
    }

    // --- Randomized independent-oracle coverage (issue #56, a6-F01) ---
    //
    // The dedup total order (ADR-0010 §5; docs/catalog-and-mvcc.md
    // "Cross-segment duplicate samples") is the crown-jewel invariant: a
    // wrong per-(series, ts) winner is silent wrong data. The hand-picked
    // point tests above pin one tiebreak level each; the property tests
    // below drive the *production* merge on randomized multi-segment inputs
    // and compare it against a reference oracle written fresh here. The
    // oracle is an independent implementation of the order itself — not the
    // heap merge under test, not the standalone copies in
    // benches/merge_kway_vs_materialized.rs — so agreement is evidence, not
    // a tautology.

    use proptest::prelude::*;

    /// Full ADR-0010 §5 ordering prefix plus per-sample in-page index:
    /// `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`.
    type PriorityKey = (i64, u64, u64, u32);
    /// Complete winner-comparison key: the priority tuple, tie-broken by
    /// `value.to_bits()`.
    type OrderKey = (PriorityKey, u64);

    /// Independent reference for the ADR-0010 §5 order. For every
    /// (series_id, ts) it gathers *every* candidate sample across all
    /// segments/runs, tags each with the full ordering key
    /// `(created_unix_ns, writer_epoch, writer_seq, in_page_index)` and the
    /// raw `value.to_bits()`, then keeps the single greatest key by plain
    /// tuple comparison and emits per series ascending by ts. No heap, no
    /// cursor drain, no `is_greater`, no `Candidate`: a straight
    /// sort-by-full-key-then-take-greatest. `in_page_index` is the sample's
    /// position within its run's `timestamps` (the field's definition), the
    /// same way production reads it; that is the meaning of the field, not a
    /// copy of the merge algorithm. Grouping is by `series_id` to mirror the
    /// production merge's real grouping key; output is re-keyed by labels
    /// because `merge_soa_runs` yields `SeriesData` carrying labels, not id.
    fn oracle(segments: &[Vec<FetchedSeriesSoa>]) -> HashMap<LabelSet, Vec<(i64, u64)>> {
        let mut labels_of: HashMap<SeriesId, LabelSet> = HashMap::new();
        let mut best: HashMap<(SeriesId, i64), OrderKey> = HashMap::new();
        for segment in segments {
            for r in segment {
                labels_of
                    .entry(r.series_id)
                    .or_insert_with(|| r.labels.clone());
                for (pos, (&ts, &val)) in r.timestamps.iter().zip(&r.values).enumerate() {
                    let in_page_index = u32::try_from(pos).unwrap_or(u32::MAX);
                    let key = (
                        (
                            r.created_unix_ns,
                            r.writer_epoch,
                            r.writer_seq,
                            in_page_index,
                        ),
                        val.to_bits(),
                    );
                    best.entry((r.series_id, ts))
                        .and_modify(|cur| {
                            if key > *cur {
                                *cur = key;
                            }
                        })
                        .or_insert(key);
                }
            }
        }
        // Seed every series that appeared in the input, including runs with
        // zero samples: production groups by series id before draining, so an
        // empty run still yields a `SeriesData` with an empty sample vec.
        let mut per_series: HashMap<SeriesId, Vec<(i64, u64)>> =
            labels_of.keys().map(|&id| (id, Vec::new())).collect();
        for ((series_id, ts), (_prio, bits)) in best {
            per_series.entry(series_id).or_default().push((ts, bits));
        }
        let mut out = HashMap::new();
        for (series_id, mut samples) in per_series {
            samples.sort_by_key(|&(ts, _)| ts);
            out.insert(labels_of[&series_id].clone(), samples);
        }
        out
    }

    /// Runs the production merge with unbounded budgets and projects the
    /// result to the same label-keyed `(ts, value.to_bits())` shape as
    /// [`oracle`], for a bit-exact, order-independent comparison.
    fn production_map(segments: &[Vec<FetchedSeriesSoa>]) -> HashMap<LabelSet, Vec<(i64, u64)>> {
        let out = merge_soa_runs(segments.to_vec(), usize::MAX, usize::MAX).expect("merge");
        out.into_iter()
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .map(|x| (x.ts_ns, x.value.to_bits()))
                    .collect();
                (s.labels, samples)
            })
            .collect()
    }

    /// Deterministic in-place Fisher-Yates over whole runs, driven by a
    /// proptest-supplied seed through a self-contained LCG. Shuffles runs
    /// only, never samples *within* a run: a run is one decoded RSEG page in
    /// ascending on-disk order and its in-page index is load-bearing, so its
    /// internal order is not a free permutation.
    fn shuffle_runs(mut v: Vec<FetchedSeriesSoa>, seed: u64) -> Vec<FetchedSeriesSoa> {
        let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
        for i in (1..v.len()).rev() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = ((state >> 33) as usize) % (i + 1);
            v.swap(i, j);
        }
        v
    }

    /// A random value with frequent ties and every significant bit pattern
    /// the storage path treats as distinct: NaN payloads, -0.0, infinities,
    /// and a small integer band so duplicate `to_bits()` are common.
    fn value_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            8 => (-3i8..=3).prop_map(f64::from),
            1 => Just(0.0f64),
            1 => Just(-0.0f64),
            1 => Just(f64::NAN),
            1 => Just(-f64::NAN),
            1 => Just(f64::INFINITY),
            1 => Just(f64::NEG_INFINITY),
        ]
    }

    /// One decoded per-segment run. Small domains for series id, provenance
    /// prefix, and timestamps force cross-run and within-run duplicate
    /// timestamps, shared provenance prefixes, and provenance ties — so the
    /// value tiebreak is exercised, not merely reachable. Timestamps are
    /// sorted ascending (RSEG page order; duplicates preserved) so the merge
    /// never trips its `NonMonotonicSamples` guard, which is not under test
    /// here.
    fn run_strategy() -> impl Strategy<Value = FetchedSeriesSoa> {
        (
            0u8..3,
            0i64..3,
            0u64..3,
            0u64..3,
            prop::collection::vec((0i64..4, value_strategy()), 0..6),
        )
            .prop_map(|(series, created, epoch, seq, mut pairs)| {
                pairs.sort_by_key(|&(ts, _)| ts);
                let ts: Vec<i64> = pairs.iter().map(|&(t, _)| t).collect();
                let vals: Vec<f64> = pairs.iter().map(|&(_, v)| v).collect();
                run(series, &ts, &vals, created, epoch, seq)
            })
    }

    fn segments_strategy() -> impl Strategy<Value = Vec<Vec<FetchedSeriesSoa>>> {
        prop::collection::vec(prop::collection::vec(run_strategy(), 0..4), 0..4)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The production merge equals the independent oracle on random
        /// multi-segment, multi-series inputs with duplicate (series, ts)
        /// samples of varied provenance and value bit patterns. Mutating any
        /// comparison in `is_greater` or the drain loop breaks this.
        #[test]
        fn merge_matches_independent_oracle(segments in segments_strategy()) {
            let want = oracle(&segments);
            let got = production_map(&segments);
            prop_assert_eq!(got, want);
        }

        /// The merge output (as an order-independent map) is identical
        /// whether the runs arrive in generated order or an arbitrary
        /// shuffle. Winner selection must depend on the total order alone,
        /// never on arrival/heap-insertion order.
        #[test]
        fn merge_is_order_independent(
            segments in segments_strategy(),
            seed in any::<u64>(),
        ) {
            let base = production_map(&segments);
            let runs: Vec<FetchedSeriesSoa> = segments.into_iter().flatten().collect();
            let shuffled = shuffle_runs(runs, seed);
            // Re-group into a different segment boundary as well, so both the
            // run order and the segment partition differ from the original.
            let mid = shuffled.len() / 2;
            let (a, b) = shuffled.split_at(mid);
            let regrouped = vec![a.to_vec(), b.to_vec()];
            prop_assert_eq!(production_map(&regrouped), base);
        }

        /// Each provenance field, in isolation, decides the winner. The
        /// candidate that should win by the field carries the *minimum*
        /// possible value bits (0.0) and the other the *maximum*
        /// (`from_bits(u64::MAX)`), so if the merge ignored the deciding
        /// field and fell through to the value tiebreak, the loser would
        /// win. A winning value of bits 0 proves the field alone decided.
        #[test]
        fn each_provenance_field_independently_decides(
            created in 0i64..5,
            epoch in 0u64..5,
            seq in 0u64..5,
            ts in 0i64..10,
        ) {
            let win = 0.0f64; // to_bits() == 0, the minimum
            let lose = f64::from_bits(u64::MAX); // to_bits() == u64::MAX, the maximum

            // created_unix_ns decides (epoch, seq, index all tie).
            let w = run(1, &[ts], &[win], created + 1, epoch, seq);
            let l = run(1, &[ts], &[lose], created, epoch, seq);
            prop_assert_eq!(winner_bits(vec![vec![l], vec![w]]), 0u64);

            // writer_epoch decides (created ties).
            let w = run(1, &[ts], &[win], created, epoch + 1, seq);
            let l = run(1, &[ts], &[lose], created, epoch, seq);
            prop_assert_eq!(winner_bits(vec![vec![l], vec![w]]), 0u64);

            // writer_seq decides (created + epoch tie).
            let w = run(1, &[ts], &[win], created, epoch, seq + 1);
            let l = run(1, &[ts], &[lose], created, epoch, seq);
            prop_assert_eq!(winner_bits(vec![vec![l], vec![w]]), 0u64);

            // in_page_index decides (full provenance prefix ties): one run,
            // two same-ts samples; the loser at index 0, the winner at
            // index 1. Greater index wins regardless of value magnitude.
            let r = run(1, &[ts, ts], &[lose, win], created, epoch, seq);
            prop_assert_eq!(winner_bits(vec![vec![r]]), 0u64);
        }

        /// When the full provenance tuple ties, `value.to_bits()` (greatest)
        /// breaks it. Every candidate shares one series, one ts, one
        /// provenance prefix, and in_page_index 0 (each in its own
        /// single-sample run), so the priority tuple is identical and only
        /// the value bits distinguish them. Expected winner is the plain max
        /// of the bit patterns.
        #[test]
        fn value_bits_break_full_provenance_tie(
            created in 0i64..5,
            epoch in 0u64..5,
            seq in 0u64..5,
            ts in 0i64..10,
            values in prop::collection::vec(value_strategy(), 1..6),
        ) {
            let segments: Vec<Vec<FetchedSeriesSoa>> = values
                .iter()
                .map(|&v| vec![run(1, &[ts], &[v], created, epoch, seq)])
                .collect();
            let want = values.iter().map(|v| v.to_bits()).max().expect("non-empty");
            prop_assert_eq!(winner_bits(segments), want);
        }
    }
}

/// Covers `build_series_by_id` directly (issue #470, ADR-0051 S7 / S4-07):
/// the `/series`, `/labels`, and `/label/{name}/values` endpoints' map
/// construction, distinct from the PromQL evaluation path's own series maps
/// exercised in `merge_tests`/`histogram_merge_tests`.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_types::{Label, LabelSet, SeriesId};

    use super::*;

    fn label_set() -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "metric".to_string(),
        }])
        .expect("valid labels")
    }

    /// Distinct series ids `0..n`, each carrying an otherwise-irrelevant
    /// label set, for probing `build_series_by_id` at scale without
    /// constructing full `ravel_segment::SeriesEntry` values.
    fn series_ids(n: u32) -> Vec<(SeriesId, LabelSet)> {
        (0..n)
            .map(|i| {
                let mut id = [0u8; 16];
                id[..4].copy_from_slice(&i.to_le_bytes());
                (SeriesId(id), label_set())
            })
            .collect()
    }

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    fn soa(lbls: LabelSet, ts: &[i64]) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([0; 16]),
            labels: lbls,
            timestamps: ts.to_vec(),
            values: ts.iter().map(|_| 1.0).collect(),
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
        }
    }

    /// A snapshot with no `pending_erasure` yields no predicates, so the scan
    /// filter stays a no-op (the pre-EJ-T2 baseline behavior).
    #[test]
    fn snapshot_erasure_predicates_empty_when_no_pending() {
        let snapshot = Snapshot::default();
        assert!(snapshot.pending_erasure.is_empty());
        assert!(snapshot_erasure_predicates(&snapshot).is_empty());
    }

    /// The real `snapshot_erasure_predicates` maps each `ErasureRequest` in a
    /// populated `Snapshot::pending_erasure` into an `ErasurePredicate` whose
    /// matchers and window bounds are carried through, and the resulting
    /// predicate excludes a matching series from a decoded SoA result via the
    /// same `retain_series_soa` pass the fetch call sites run. This proves the
    /// EJ-T2 to EJ-T3 wire-up is live end to end, not merely compiling.
    #[test]
    fn snapshot_erasure_predicates_excludes_matching_series() {
        use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};

        // A windowless request: the whole matching series must drop. Other
        // request fields (tenant_hash, signal, request_id, ...) are irrelevant
        // to filtering and left at their defaults.
        let request = ErasureRequest {
            predicate: vec![
                ErasurePredicateMatcher {
                    key: "user_id".to_string(),
                    value: "u1".to_string(),
                },
                ErasurePredicateMatcher {
                    key: "region".to_string(),
                    value: "eu".to_string(),
                },
            ],
            window_start_ns: 0,
            window_end_ns: 0,
            ..Default::default()
        };
        let snapshot = Snapshot {
            pending_erasure: vec![request],
            ..Default::default()
        };

        let predicates = snapshot_erasure_predicates(&snapshot);
        assert_eq!(predicates.len(), 1, "one predicate per pending request");

        // The mapped predicate excludes the matching series and leaves a
        // non-matching one untouched.
        let matching = labels(&[("__name__", "m"), ("user_id", "u1"), ("region", "eu")]);
        let other = labels(&[("__name__", "m"), ("user_id", "u2"), ("region", "eu")]);
        let mut series = vec![
            soa(matching, &[10, 20, 30]),
            soa(other.clone(), &[10, 20, 30]),
        ];
        crate::erasure::retain_series_soa(&mut series, &predicates);
        assert_eq!(series.len(), 1, "matching series excluded");
        assert_eq!(series[0].labels, other, "non-matching series survives");
    }

    /// A windowed request maps its `[window_start_ns, window_end_ns)` bounds
    /// through, so only in-window samples of a matching series drop.
    #[test]
    fn snapshot_erasure_predicates_maps_window_bounds() {
        use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};

        let request = ErasureRequest {
            predicate: vec![ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u1".to_string(),
            }],
            window_start_ns: 15,
            window_end_ns: 25,
            ..Default::default()
        };
        let snapshot = Snapshot {
            pending_erasure: vec![request],
            ..Default::default()
        };

        let predicates = snapshot_erasure_predicates(&snapshot);
        assert_eq!(predicates.len(), 1);

        let mut series = vec![soa(labels(&[("user_id", "u1")]), &[10, 20, 30])];
        crate::erasure::retain_series_soa(&mut series, &predicates);
        // Only the in-window sample (ts=20) is dropped; 10 and 30 survive.
        assert_eq!(series.len(), 1, "windowed drop keeps out-of-window samples");
        assert_eq!(series[0].timestamps, vec![10, 30]);
    }

    #[test]
    fn max_series_bounds_peak_map_size() {
        // Far more distinct series than the cap: a build-then-check
        // implementation would grow the map to 10_000 entries before
        // rejecting. Asserting on the partial map itself (not just the
        // error's `count` field) proves construction actually stopped at
        // the cap instead of merely reporting a small number afterward.
        let err = build_series_by_id(series_ids(10_000), 3).expect_err("cap exceeded");
        let SeriesBuildError::TooManySeries(cap) = err;
        assert_eq!(
            cap.by_id.len(),
            3,
            "map must never grow past max_series before rejecting"
        );
        assert_eq!(cap.max, 3);

        let query_err: QueryError = SeriesBuildError::TooManySeries(cap).into();
        match query_err {
            QueryError::TooManySeries { count, max } => {
                assert_eq!(count, 4);
                assert_eq!(max, 3);
            }
            other => panic!("expected TooManySeries, got {other:?}"),
        }
    }

    #[test]
    fn max_series_exactly_at_cap_succeeds() {
        let by_id = build_series_by_id(series_ids(3), 3).expect("exactly at cap must succeed");
        assert_eq!(by_id.len(), 3);
    }

    // The bytes-scanned budget no longer lives in `build_series_by_id`
    // (ADR-0061 amendment, issue #721): it moved into `fetch_all_series`, so a
    // labels/series query that matches zero series still evaluates the budget
    // against every segment fetched. That end-to-end behavior, which a
    // `build_series_by_id` unit test could not reach, is proven in
    // tests/bytes_scanned_budget.rs.
}

/// Histogram counterpart to `merge_tests`: exercises `merge_histogram_series_runs`
/// / `histogram_is_greater` (docs/rseg-v3-plan.md section 7) with the same
/// hand-picked-cases-plus-independent-oracle-proptest structure, substituting
/// directly-constructed `HistogramValue`s for plain `f64`s.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod histogram_merge_tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use ravel_segment::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};

    use super::*;

    /// A fixed, simple single-bucket int-counts histogram shape, varied only
    /// by `sum`/`zero_threshold` (the two float fields exercised by the
    /// bit-pattern tests below).
    fn histogram_value(sum: Option<f64>, zero_threshold: f64) -> HistogramValue {
        HistogramValue {
            scale: 0,
            zero_threshold,
            sum,
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 1,
                positive: vec![1],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    fn run(
        ts: &[i64],
        values: Vec<HistogramValue>,
        created: i64,
        epoch: u64,
        seq: u64,
    ) -> HistogramSeriesRun {
        HistogramSeriesRun {
            timestamps: ts.to_vec(),
            values,
            prefix: (created, epoch, seq),
        }
    }

    fn winner(runs: Vec<HistogramSeriesRun>) -> HistogramValue {
        let mut budget = YieldBudget {
            yielded: 0,
            max: 10_000,
        };
        let mut out = Vec::new();
        merge_histogram_series_runs(&runs, &mut budget, &mut out).expect("merge succeeds");
        assert_eq!(out.len(), 1, "expected exactly one merged sample");
        out.into_iter().next().expect("checked len == 1").value
    }

    #[test]
    fn provenance_decides_winner() {
        let ts = 1_000;
        let older = histogram_value(Some(1.0), 0.0);
        let newer = histogram_value(Some(2.0), 0.0);
        let runs = vec![
            run(&[ts], vec![older], 100, 0, 0),
            run(&[ts], vec![newer.clone()], 200, 0, 0),
        ];
        let got = winner(runs);
        assert_eq!(got.sum, newer.sum);
    }

    #[test]
    fn bit_pattern_breaks_full_provenance_tie_with_nan_payloads() {
        let ts = 1_000;
        let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
        assert_ne!(nan_a.to_bits(), nan_b.to_bits());
        let want_bits = nan_a.to_bits().max(nan_b.to_bits());

        let a = histogram_value(Some(nan_a), 0.0);
        let b = histogram_value(Some(nan_b), 0.0);
        let runs = vec![
            run(&[ts], vec![a], 100, 0, 0),
            run(&[ts], vec![b], 100, 0, 0),
        ];
        let got = winner(runs);
        assert_eq!(got.sum.expect("has sum").to_bits(), want_bits);
    }

    #[test]
    fn bit_pattern_breaks_tie_with_negative_zero() {
        let ts = 1_000;
        let pos_zero = histogram_value(Some(1.0), 0.0);
        let neg_zero = histogram_value(Some(1.0), -0.0);
        assert_ne!(
            pos_zero.zero_threshold.to_bits(),
            neg_zero.zero_threshold.to_bits()
        );
        let want_bits = pos_zero
            .zero_threshold
            .to_bits()
            .max(neg_zero.zero_threshold.to_bits());

        let runs = vec![
            run(&[ts], vec![pos_zero], 100, 0, 0),
            run(&[ts], vec![neg_zero], 100, 0, 0),
        ];
        let got = winner(runs);
        assert_eq!(got.zero_threshold.to_bits(), want_bits);
    }

    #[test]
    fn provenance_wins_over_structural_bits() {
        let ts = 1_000;
        // Older/lower-priority side has the numerically "louder" bit
        // pattern; the newer/higher-priority side must still win, proving
        // the priority prefix dominates the structural tiebreak.
        let loud_but_old = histogram_value(Some(f64::from_bits(u64::MAX)), 0.0);
        let quiet_but_new = histogram_value(Some(0.0), 0.0);
        let runs = vec![
            run(&[ts], vec![loud_but_old], 100, 0, 0),
            run(&[ts], vec![quiet_but_new.clone()], 200, 0, 0),
        ];
        let got = winner(runs);
        assert_eq!(
            got.sum.expect("has sum").to_bits(),
            quiet_but_new.sum.expect("has sum").to_bits()
        );
    }

    #[test]
    fn non_monotonic_timestamps_within_a_run_error() {
        let a = histogram_value(Some(1.0), 0.0);
        let b = histogram_value(Some(2.0), 0.0);
        let runs = vec![run(&[1_000, 500], vec![a, b], 100, 0, 0)];
        let mut budget = YieldBudget {
            yielded: 0,
            max: 10_000,
        };
        let mut out = Vec::new();
        let err = merge_histogram_series_runs(&runs, &mut budget, &mut out).unwrap_err();
        assert!(matches!(err, QueryError::NonMonotonicSamples { .. }));
    }

    fn float_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            Just(0.0_f64),
            Just(-0.0_f64),
            Just(f64::NAN),
            Just(-f64::NAN),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
            any::<i16>().prop_map(f64::from),
        ]
    }

    fn histogram_strategy() -> impl Strategy<Value = HistogramValue> {
        (proptest::option::of(float_strategy()), float_strategy())
            .prop_map(|(sum, zero_threshold)| histogram_value(sum, zero_threshold))
    }

    fn run_strategy() -> impl Strategy<Value = HistogramSeriesRun> {
        (
            proptest::collection::vec(1_i64..20, 1..5),
            0_i64..5,
            0_u64..3,
            0_u64..3,
        )
            .prop_flat_map(|(mut raw_ts, created, epoch, seq)| {
                raw_ts.sort_unstable();
                raw_ts.dedup();
                let n = raw_ts.len();
                proptest::collection::vec(histogram_strategy(), n).prop_map(move |values| {
                    HistogramSeriesRun {
                        timestamps: raw_ts.clone(),
                        values,
                        prefix: (created, epoch, seq),
                    }
                })
            })
    }

    fn runs_strategy() -> impl Strategy<Value = Vec<HistogramSeriesRun>> {
        proptest::collection::vec(run_strategy(), 1..4)
    }

    /// Full ADR-0010 §5 ordering prefix plus per-sample in-page index,
    /// mirroring `merge_tests::PriorityKey`.
    type PriorityKey = (i64, u64, u64, u32);
    /// Complete winner-comparison key: the priority tuple, tie-broken by
    /// [`HistogramSortKey`].
    type OrderKey = (PriorityKey, HistogramSortKey);

    /// Linear-scan reference: for each timestamp, keeps the greatest
    /// `(priority, HistogramSortKey)` tuple across every run, mirroring
    /// `merge_tests::oracle`'s differential-testing pattern.
    fn oracle(runs: &[HistogramSeriesRun]) -> HashMap<i64, OrderKey> {
        let mut best: HashMap<i64, OrderKey> = HashMap::new();
        for run in runs {
            for (pos, (&ts, value)) in run.timestamps.iter().zip(run.values.iter()).enumerate() {
                let in_page_index = u32::try_from(pos).unwrap_or(u32::MAX);
                let priority = (run.prefix.0, run.prefix.1, run.prefix.2, in_page_index);
                let key = histogram_sort_key(value);
                match best.get(&ts) {
                    Some((cur_priority, cur_key))
                        if (*cur_priority, cur_key.clone()) >= (priority, key.clone()) => {}
                    _ => {
                        best.insert(ts, (priority, key));
                    }
                }
            }
        }
        best
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn merge_matches_independent_oracle(runs in runs_strategy()) {
            let want = oracle(&runs);
            let mut budget = YieldBudget { yielded: 0, max: 100_000 };
            let mut out = Vec::new();
            merge_histogram_series_runs(&runs, &mut budget, &mut out).expect("merge succeeds");

            let mut got: HashMap<i64, HistogramSortKey> = HashMap::new();
            for sample in &out {
                got.insert(sample.ts_ns, histogram_sort_key(&sample.value));
            }
            let want_keys: HashMap<i64, HistogramSortKey> =
                want.into_iter().map(|(ts, (_, key))| (ts, key)).collect();
            prop_assert_eq!(got, want_keys);
        }
    }
}

/// Exercises `merge_histogram_soa_runs` + `MergedSource::query_histograms`
/// (#218): the fetch-side merge-and-convert path that feeds the evaluator's
/// native-histogram source, the histogram counterpart of the scalar
/// `prefetch`/`MergedSource::query` path. Builds `FetchedHistogramSeries` by
/// hand (as `merge_tests` builds `FetchedSeriesSoa`), so it is independent of
/// the object-store fetch proven in `fetcher::tests`.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod histogram_source_tests {
    use ravel_segment::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
    use ravel_types::{Label, LabelSet};

    use super::*;
    use crate::fetcher::FetchedHistogramSeries;

    fn labels(series: u8) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: format!("metric_{series}"),
        }])
        .expect("valid labels")
    }

    /// A single-bucket int histogram distinguished only by `sum`.
    fn hv(sum: f64) -> HistogramValue {
        HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(sum),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 1,
                positive: vec![1],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    fn fhs(
        series: u8,
        ts: &[i64],
        vals: Vec<HistogramValue>,
        created: i64,
        epoch: u64,
        seq: u64,
    ) -> FetchedHistogramSeries {
        assert_eq!(ts.len(), vals.len());
        FetchedHistogramSeries {
            series_id: SeriesId([series; 16]),
            labels: labels(series),
            timestamps: ts.to_vec(),
            values: vals,
            created_unix_ns: created,
            writer_epoch: epoch,
            writer_seq: seq,
        }
    }

    fn source(segments: Vec<Vec<FetchedHistogramSeries>>) -> MergedSource {
        let histogram_series =
            merge_histogram_soa_runs(segments, 10_000, usize::MAX).expect("merge");
        MergedSource {
            series: Vec::new(),
            histogram_series,
        }
    }

    #[test]
    fn duplicate_sample_across_segments_resolves_by_provenance() {
        // Two segments, same series, same ts: the greater created_unix_ns wins,
        // exactly as the scalar merge resolves a duplicate.
        let older = fhs(1, &[5], vec![hv(1.0)], 10, 0, 0);
        let newer = fhs(1, &[5], vec![hv(2.0)], 20, 0, 0);

        let src = source(vec![vec![older.clone()], vec![newer.clone()]]);
        let window = TimeRange {
            start_ns: 0,
            end_ns: 100,
        };
        let got = src.query_histograms(&[], window).expect("query_histograms");
        assert_eq!(got.len(), 1, "one merged series");
        assert_eq!(got[0].samples.len(), 1, "one deduped sample");
        assert_eq!(got[0].samples[0].value.sum, 2.0, "newer created wins");

        // Order-independent: swapping the two segments yields the same winner.
        let src = source(vec![vec![newer], vec![older]]);
        let got = src.query_histograms(&[], window).expect("query_histograms");
        assert_eq!(got[0].samples[0].value.sum, 2.0);
    }

    #[test]
    fn k_way_merge_and_window_clip_and_matcher_filter() {
        // Series 1 interleaved across two segments; series 2 disjoint. The
        // merge yields ascending deduped ts per series; query_histograms then
        // clips to the window and filters by matcher.
        let a = fhs(1, &[1, 5], vec![hv(1.0), hv(5.0)], 0, 0, 0);
        let b = fhs(1, &[3], vec![hv(3.0)], 0, 0, 0);
        let c = fhs(2, &[2, 4], vec![hv(20.0), hv(40.0)], 0, 0, 0);
        let src = source(vec![vec![a, c], vec![b]]);

        // Full window, series 1 only: ts 1,3,5 in order.
        let full = TimeRange {
            start_ns: 0,
            end_ns: 100,
        };
        let s1 = src
            .query_histograms(&[LabelMatcher::equal(METRIC_NAME_LABEL, "metric_1")], full)
            .expect("query");
        assert_eq!(s1.len(), 1, "matcher isolates series 1");
        let ts1: Vec<i64> = s1[0].samples.iter().map(|s| s.ts_ns).collect();
        assert_eq!(ts1, vec![1, 3, 5]);

        // Clipped window drops out-of-range samples (inclusive both ends).
        let clipped = TimeRange {
            start_ns: 2,
            end_ns: 4,
        };
        let s2 = src
            .query_histograms(
                &[LabelMatcher::equal(METRIC_NAME_LABEL, "metric_2")],
                clipped,
            )
            .expect("query");
        let ts2: Vec<i64> = s2[0].samples.iter().map(|s| s.ts_ns).collect();
        assert_eq!(ts2, vec![2, 4]);
    }

    #[test]
    fn int_counts_convert_to_float_model() {
        // The converted sample carries the float working model: int counts
        // widened to f64, spans preserved, sum carried through.
        let src = source(vec![vec![fhs(1, &[7], vec![hv(9.5)], 0, 0, 0)]]);
        let window = TimeRange {
            start_ns: 0,
            end_ns: 100,
        };
        let got = src.query_histograms(&[], window).expect("query");
        let h = &got[0].samples[0].value;
        assert_eq!(h.count, 1.0);
        assert_eq!(h.sum, 9.5);
        assert_eq!(h.positive_buckets, vec![1.0]);
        assert_eq!(h.positive_spans.len(), 1);
    }
}

/// Exercises `QueryEngine::prefetch` directly with a hand-built, multi-entry
/// `SelectorPlan` list (ADR-0021 P2): the current evaluator grammar can only
/// ever produce 0 or 1 plans from real query text (`plan_selectors`'s doc
/// comment: it deliberately walks constructs the evaluator still rejects,
/// e.g. `Expr::Binary`, so a query like `a + b` already yields two plans
/// today even though evaluating it fails with `Unsupported`). These tests
/// go around `plan_selectors` and call `prefetch` with plans built by hand,
/// so the multi-selector fetch/merge path is verified independent of which
/// future phase grows the evaluator to actually emit such plans.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod prefetch_tests {
    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::memory::MemoryStore;
    use ravel_promql::MatchOp;
    use ravel_segment::{
        HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
        SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3, SeriesValues, WrittenSegment,
    };
    use ravel_types::{Label, TenantId};
    use uuid::Uuid;

    use super::*;

    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const BASE_NS: i64 = 1_700_000_000_000_000_000;
    // Single source of truth for the lookback delta lives in ravel-promql
    // (the evaluator owns the semantic). A hand-built plan reuses it so the
    // test window can never drift from what the evaluator selects (a6-F02).
    const DEFAULT_LOOKBACK_NS: i64 = ravel_promql::DEFAULT_LOOKBACK_NS;

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn name_matcher(metric: &str) -> LabelMatcher {
        LabelMatcher {
            name: METRIC_NAME_LABEL.to_string(),
            op: MatchOp::Eq,
            value: metric.to_string(),
        }
    }

    fn window_plan(metric: &str) -> SelectorPlan {
        SelectorPlan {
            matchers: vec![name_matcher(metric)],
            range_ns: DEFAULT_LOOKBACK_NS,
            offset_ns: 0,
            anchor: PlanAnchor::Window,
        }
    }

    /// Writes one real RSEG segment (one series per metric) and publishes
    /// its commit record, mirroring `tests/e2e.rs`'s own helper.
    async fn publish_metric(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        writer_seq: u64,
        metric: &str,
        ts_ns: i64,
        value: f64,
    ) {
        let series_id =
            SeriesId::compute(&TenantId::new("acme"), metric, &labels(metric)).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels: labels(metric),
            samples: vec![Sample { ts_ns, value }],
        }];
        let writer_id = Uuid::new_v4();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written: WrittenSegment =
            SegmentWriter::write(series, identity, bounds).expect("write segment");
        let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");
        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: BASE_NS,
            ingest_hour_bucket: hour_bucket,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// Writes one real RSEG segment containing a single native-histogram
    /// series with `bucket_count` positive buckets, mirroring
    /// `publish_metric` but through `SegmentWriter::write_histograms`
    /// (ADR-0044 finding 2 test fixture: proves `estimate_cost`'s
    /// per-sample bound against a real wide-histogram footprint, not just
    /// scalar samples).
    async fn publish_histogram_metric(
        store: &MemoryStore,
        tenant_hash: TenantHash,
        writer_seq: u64,
        metric: &str,
        ts_ns: i64,
        bucket_count: usize,
    ) {
        let series_id =
            SeriesId::compute(&TenantId::new("acme"), metric, &labels(metric)).expect("series id");
        let value = HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(1.0),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: bucket_count as u32,
            }],
            negative_spans: Vec::new(),
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: bucket_count as u64,
                positive: vec![1u64; bucket_count],
                negative: Vec::new(),
            },
            reset_hint: ResetHint::Unknown,
        };
        let series = vec![SeriesInputV3 {
            series_id,
            labels: labels(metric),
            values: SeriesValues::Histogram(vec![HistogramSample { ts_ns, value }]),
        }];
        let writer_id = Uuid::new_v4();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written: WrittenSegment = SegmentWriter::write_histograms(series, identity, bounds)
            .expect("write histogram segment");
        let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");
        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: BASE_NS,
            ingest_hour_bucket: hour_bucket,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    fn engine(store: Arc<MemoryStore>) -> QueryEngine {
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let catalog =
            Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
        QueryEngine::new(Arc::new(catalog), backend, EngineConfig::default())
    }

    /// A slice fetcher double that reports `SnapshotInvalidated` for every
    /// slice and counts its dispatches, so the seam test can assert exactly
    /// one whole-query re-resolve (two dispatches for a one-slice snapshot).
    struct CountingInvalidated {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::distrib::client::SliceFetcher for CountingInvalidated {
        async fn fetch(
            &self,
            _request: ravel_proto::queryfrag::v1::FetchRequest,
        ) -> Result<crate::distrib::client::SliceResponse, crate::distrib::client::DistribError>
        {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::distrib::client::SliceResponse {
                scalar: Vec::new(),
                accounting: ravel_types::accounting::QueryAccountingSnapshot::default(),
                stats: crate::fetcher::FetchStats::default(),
                series_returned: 0,
                samples_returned: 0,
                status: ravel_proto::queryfrag::v1::status::Code::SnapshotInvalidated,
                status_message: "vanished".to_string(),
            })
        }
    }

    /// ADR-0071 failure semantics: a slice reporting `SNAPSHOT_INVALIDATED`
    /// makes the coordinator re-resolve and re-dispatch the *whole* query
    /// exactly once, then give up with `SnapshotInvalidated` -- it does not
    /// retry per slice or loop. The dispatch counter proves "exactly once":
    /// one segment resolves to one slice, so two dispatches total means the
    /// initial attempt plus a single re-resolve.
    #[tokio::test]
    async fn distributed_snapshot_invalidation_reresolves_once() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        publish_metric(
            &store,
            tenant_hash,
            1,
            "metric_a",
            BASE_NS - NS_PER_MIN,
            1.0,
        )
        .await;

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let distributed = Arc::new(crate::distrib::Distributed::new(
            Arc::new(CountingInvalidated {
                calls: Arc::clone(&calls),
            }),
            // Thresholds of 0 force the cost gate on for even this one-segment
            // snapshot, so the seam takes the distributed path.
            crate::distrib::partition::DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        ));
        let eng = engine(Arc::clone(&store)).with_distributed(distributed);

        let plans = vec![window_plan("metric_a")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let err = match eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
        {
            Ok(_) => panic!("an always-invalidated worker must fail the query"),
            Err(e) => e,
        };
        assert!(
            matches!(err, QueryError::SnapshotInvalidated),
            "expected SnapshotInvalidated after the single retry, got {err:?}"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one slice dispatched on the initial attempt and once more after \
             exactly one re-resolve"
        );
    }

    /// Two selectors for two disjoint metrics: the merged source must
    /// contain both series with their own correct samples, proving
    /// `prefetch` resolves one shared snapshot and fetches+merges each
    /// selector's own matchers into a single combined result rather than
    /// only covering the first plan.
    #[tokio::test]
    async fn multi_selector_prefetch_combines_both_selectors_series() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();

        let ts_a = BASE_NS - NS_PER_MIN;
        let ts_b = BASE_NS - 2 * NS_PER_MIN;
        publish_metric(&store, tenant_hash, 1, "metric_a", ts_a, 10.0).await;
        publish_metric(&store, tenant_hash, 2, "metric_b", ts_b, 20.0).await;

        let eng = engine(Arc::clone(&store));
        let plans = vec![window_plan("metric_a"), window_plan("metric_b")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (source, _stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch must succeed across both selectors");

        let window = TimeRange {
            start_ns: BASE_NS - NS_PER_MIN * 10,
            end_ns: BASE_NS,
        };
        let a = source
            .query(&[name_matcher("metric_a")], window)
            .expect("query metric_a");
        assert_eq!(a.len(), 1, "metric_a's series must have been prefetched");
        assert_eq!(
            a[0].samples,
            vec![Sample {
                ts_ns: ts_a,
                value: 10.0
            }]
        );

        let b = source
            .query(&[name_matcher("metric_b")], window)
            .expect("query metric_b");
        assert_eq!(b.len(), 1, "metric_b's series must have been prefetched");
        assert_eq!(
            b[0].samples,
            vec![Sample {
                ts_ns: ts_b,
                value: 20.0
            }]
        );
    }

    /// A selector's own matcher isolates its fetch: querying the merged
    /// source with only `metric_a`'s matcher must never return `metric_b`'s
    /// series, even though `prefetch` resolved one shared snapshot covering
    /// both.
    #[tokio::test]
    async fn multi_selector_prefetch_keeps_each_selectors_series_isolated_by_matcher() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let ts = BASE_NS - NS_PER_MIN;
        publish_metric(&store, tenant_hash, 1, "metric_a", ts, 1.0).await;
        publish_metric(&store, tenant_hash, 2, "metric_b", ts, 2.0).await;

        let eng = engine(Arc::clone(&store));
        let plans = vec![window_plan("metric_a"), window_plan("metric_b")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (source, _stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch");

        let window = TimeRange {
            start_ns: BASE_NS - NS_PER_MIN * 10,
            end_ns: BASE_NS,
        };
        let only_a = source
            .query(&[name_matcher("metric_a")], window)
            .expect("query metric_a");
        assert_eq!(only_a.len(), 1);
        assert_eq!(only_a[0].labels, labels("metric_a"));
    }

    /// Issue #801: the combine stage's output order must not depend on
    /// `HashMap` `RandomState` iteration order or on which selector's fetch
    /// future happened to finish first (`buffer_unordered`). 40 distinct
    /// series makes an accidental match between two independently-seeded
    /// `HashMap`s astronomically unlikely, so two `prefetch` calls landing on
    /// the same order, run after run, is real evidence the stage sorts its
    /// output rather than leaving it at arbitrary hash order.
    #[tokio::test]
    async fn multi_selector_prefetch_output_order_is_deterministic_across_runs() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let ts = BASE_NS - NS_PER_MIN;

        let metrics: Vec<String> = (0..40).map(|i| format!("metric_{i:02}")).collect();
        for (i, metric) in metrics.iter().enumerate() {
            publish_metric(&store, tenant_hash, i as u64 + 1, metric, ts, i as f64).await;
        }

        let eng = engine(Arc::clone(&store));
        let plans: Vec<SelectorPlan> = metrics.iter().map(|m| window_plan(m)).collect();
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let window = TimeRange {
            start_ns: BASE_NS - NS_PER_MIN * 10,
            end_ns: BASE_NS,
        };

        let (source_a, _) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch 1");
        let (source_b, _) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch 2");

        let order_a: Vec<LabelSet> = source_a
            .query(&[], window)
            .expect("query all")
            .into_iter()
            .map(|s| s.labels)
            .collect();
        let order_b: Vec<LabelSet> = source_b
            .query(&[], window)
            .expect("query all")
            .into_iter()
            .map(|s| s.labels)
            .collect();
        assert_eq!(order_a.len(), metrics.len());

        assert_eq!(
            order_a, order_b,
            "combine stage output order must not depend on HashMap iteration order \
             or fetch completion order"
        );

        let mut sorted = order_a.clone();
        sorted.sort_by(|a, b| a.iter().cmp(b.iter()));
        assert_eq!(
            order_a, sorted,
            "combine stage output must be sorted by label set, not left at \
             arbitrary HashMap order"
        );
    }

    /// An empty plan list (a query with no selectors, e.g. a bare scalar
    /// literal) must skip storage entirely rather than resolve a snapshot
    /// or fetch anything.
    #[tokio::test]
    async fn empty_plan_list_skips_storage_and_returns_an_empty_source() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let eng = engine(store);
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (source, _stats) = eng
            .prefetch(tenant_hash, &[], &eval_window, &[], BASE_NS)
            .await
            .expect("empty plan list must not error");
        let window = TimeRange {
            start_ns: BASE_NS - NS_PER_MIN,
            end_ns: BASE_NS,
        };
        assert!(
            source
                .query(&[], window)
                .expect("query empty source")
                .is_empty(),
            "no plans means no series, regardless of matchers"
        );
    }

    /// a6-F02: the pre-fetch padding (`padded_range`) and the evaluator's
    /// lookback window must be one and the same value. Both now derive from
    /// the single `ravel_promql::DEFAULT_LOOKBACK_NS` constant; this pins the
    /// link so a future change to that constant moves the engine's fetch
    /// window with it instead of silently under- or over-fetching the left
    /// edge of the lookback window.
    #[test]
    fn prefetch_padding_matches_evaluator_lookback_delta() {
        // `plan_selectors` builds the same plan the engine feeds to
        // `selector_fetch_window` in production; a bare selector's own range
        // is exactly the evaluator's lookback delta.
        let plans = plan_selectors("http_requests_total", 0, 0).expect("plan bare selector");
        assert_eq!(plans.len(), 1, "one selector, one plan");
        assert_eq!(
            plans[0].range_ns,
            ravel_promql::DEFAULT_LOOKBACK_NS,
            "the selector's fetch range must be the evaluator's lookback delta"
        );

        // The padded fetch window an instant query resolves extends left by
        // exactly that delta off the evaluation instant, so the evaluator can
        // never select a sample the source did not fetch.
        let sel_ts = BASE_NS;
        let window = selector_fetch_window(&plans[0], &EvalWindow::Instant { t_ns: sel_ts })
            .expect("instant fetch window");
        assert_eq!(
            window.end_ns, sel_ts,
            "instant window ends at the eval instant"
        );
        assert_eq!(
            window.start_ns,
            sel_ts - ravel_promql::DEFAULT_LOOKBACK_NS,
            "the fetch window's left extent is the shared lookback delta"
        );
    }

    /// ADR-0044: `estimate_cost`'s output must be a genuine upper envelope,
    /// never a prediction -- the actual accounting snapshot recorded by the
    /// same query attempt must never exceed it in any dimension. `divergence`
    /// is actual/estimated, so a value above 1.0 would mean the estimate
    /// under-shot.
    fn assert_estimate_covers_actual(shape: &str, stats: &QueryStats) {
        let divergence = stats.estimate.divergence(&stats.accounting);
        assert!(
            divergence.requests <= 1.0,
            "{shape}: requests diverged {divergence:?}, estimate {:?}, actual {:?}",
            stats.estimate,
            stats.accounting
        );
        assert!(
            divergence.store_bytes <= 1.0,
            "{shape}: store_bytes diverged {divergence:?}, estimate {:?}, actual {:?}",
            stats.estimate,
            stats.accounting
        );
        assert!(
            divergence.decompressed_bytes <= 1.0,
            "{shape}: decompressed_bytes diverged {divergence:?}, estimate {:?}, actual {:?}",
            stats.estimate,
            stats.accounting
        );
    }

    /// Point lookup: one segment, one selector, one series.
    #[tokio::test]
    async fn cost_estimate_is_upper_bound_for_point_lookup() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let ts = BASE_NS - NS_PER_MIN;
        publish_metric(&store, tenant_hash, 1, "point_metric", ts, 1.0).await;

        let eng = engine(store);
        let plans = vec![window_plan("point_metric")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (_source, stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch point lookup");

        assert_estimate_covers_actual("point lookup", &stats);
    }

    /// Wide scan: several selectors fanned out against one shared snapshot
    /// (`fetch_multiplier > 1`), so the estimate's per-selector scaling must
    /// hold, not just its per-segment terms.
    #[tokio::test]
    async fn cost_estimate_is_upper_bound_for_wide_multi_selector_scan() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let ts = BASE_NS - NS_PER_MIN;
        for (i, metric) in ["wide_a", "wide_b", "wide_c", "wide_d"]
            .into_iter()
            .enumerate()
        {
            publish_metric(&store, tenant_hash, i as u64 + 1, metric, ts, i as f64).await;
        }

        let eng = engine(store);
        let plans = vec![
            window_plan("wide_a"),
            window_plan("wide_b"),
            window_plan("wide_c"),
            window_plan("wide_d"),
        ];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (_source, stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch wide scan");

        assert_estimate_covers_actual("wide multi-selector scan", &stats);
    }

    /// Multi-segment: one selector whose matcher spans several independently
    /// published segments for the same metric, so `estimate_cost`'s
    /// per-segment sum (not just its per-selector multiplier) must cover the
    /// real per-segment fetch cost.
    #[tokio::test]
    async fn cost_estimate_is_upper_bound_for_multi_segment_query() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        for seq in 1..=5u64 {
            let ts = BASE_NS - (6 - seq as i64) * NS_PER_MIN;
            publish_metric(&store, tenant_hash, seq, "multi_seg_metric", ts, seq as f64).await;
        }

        let eng = engine(store);
        let plans = vec![window_plan("multi_seg_metric")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (_source, stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch multi-segment");

        assert!(
            stats.estimate.segments >= 5,
            "expected the snapshot to span at least the 5 published segments, got {}",
            stats.estimate.segments
        );
        assert_estimate_covers_actual("multi-segment query", &stats);
    }

    /// Native-histogram fixture (ADR-0044 finding 2): a 200-bucket sample's
    /// real decompressed footprint (`histogram_value_footprint`, fetcher.rs:
    /// `16 + 200*8` = 1616 bytes of counts alone, before spans/scale/sum)
    /// is already more than 3x the old flat `BYTES_PER_SAMPLE_UPPER_BOUND =
    /// 512` constant. This test fails against that constant and passes only
    /// once the bound is derived from the segment's own value kinds
    /// (`segment_decompressed_bytes_upper_bound`).
    #[tokio::test]
    async fn cost_estimate_is_upper_bound_for_wide_histogram_segment() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let ts = BASE_NS - NS_PER_MIN;
        publish_histogram_metric(&store, tenant_hash, 1, "wide_hist_metric", ts, 200).await;

        let eng = engine(store);
        let plans = vec![window_plan("wide_hist_metric")];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (_source, stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch wide histogram");

        assert_estimate_covers_actual("wide histogram segment", &stats);
    }

    /// Catalog-term fixture (ADR-0044 finding 3): a wide window with no
    /// segment published for the queried metric still makes `resolve`
    /// LIST every `(shard, hour)` pair the padded window spans, plus attempt
    /// one snapshot-window HEAD GET before any LIST runs (the window is
    /// non-empty, so `resolve_impl` always tries it first). Pre-fix,
    /// `estimate_cost` had no catalog term at all, so a zero-segment
    /// snapshot always estimated zero requests against this nonzero request
    /// count -- `CostEstimate::divergence`'s `f64::INFINITY` case. A second
    /// gap surfaced once the LIST-only catalog term was added: it missed the
    /// snapshot-window HEAD GET, under-estimating by exactly one request.
    /// This test fails against either gap and passes once
    /// `Catalog::estimated_catalog_requests` (LISTs plus
    /// `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`) is folded into the estimate.
    #[tokio::test]
    async fn cost_estimate_is_upper_bound_for_wide_window_multi_shard_query() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("acme").hash();
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let catalog = Catalog::new(
            Arc::clone(&backend),
            CatalogConfig {
                shard_count: 16,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog");
        let eng = QueryEngine::new(Arc::new(catalog), backend, EngineConfig::default());

        let plans = vec![SelectorPlan {
            matchers: vec![name_matcher("nonexistent_metric")],
            range_ns: 24 * 60 * NS_PER_MIN,
            offset_ns: 0,
            anchor: PlanAnchor::Window,
        }];
        let eval_window = EvalWindow::Instant { t_ns: BASE_NS };
        let (_source, stats) = eng
            .prefetch(tenant_hash, &plans, &eval_window, &[], BASE_NS)
            .await
            .expect("prefetch wide window");

        assert_eq!(
            stats.estimate.segments, 0,
            "no segment published for this metric"
        );
        assert_estimate_covers_actual("wide window multi-shard query", &stats);
    }
}
