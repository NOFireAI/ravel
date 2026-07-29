//! QueryEngine: snapshot resolution, bounded-concurrency segment fetch,
//! cross-segment duplicate-sample resolution, and PromQL evaluation
//! (docs/query-engine.md "Flow", docs/catalog-and-mvcc.md).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use promql_parser::parser::{Expr, Offset};
use ravel_catalog::{Catalog, Snapshot};
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_promql::{
    Annotations, Evaluator, LabelMatcher, MatchOp, PlanAnchor, SelectorPlan, SeriesData,
    SeriesSource, SourceError, Value, from_ast_matchers, has_or_group, matches_series, ms_to_ns,
    plan_selectors,
};
use ravel_types::{
    CommitToken, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TimeRange,
};

use crate::config::EngineConfig;
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchedSeriesSoa, SegmentFetcher};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryStats {
    /// Segments actually fetched (the post-pruning snapshot size).
    pub segments_fetched: u64,
    /// Snapshot-sourced segments postings pruning excluded.
    pub segments_pruned: u64,
}

impl QueryStats {
    fn from_snapshot(snapshot: &Snapshot) -> Self {
        QueryStats {
            segments_fetched: snapshot.segments.len() as u64,
            segments_pruned: snapshot.segments_pruned,
        }
    }
}

/// Resolves snapshots, fetches segments, merges cross-segment duplicates,
/// and evaluates PromQL over the result (docs/query-engine.md).
pub struct QueryEngine {
    catalog: Arc<Catalog>,
    fetcher: SegmentFetcher,
    config: EngineConfig,
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
        }
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
        let (value, annotations) = evaluator.eval_instant_annotated(&source, query, t_ms)?;
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
            evaluator.eval_range_annotated(&source, query, start_ms, end_ms, step_ms)?;
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
        let attempt = |snapshot: Snapshot| async move {
            let fetched = self
                .fetch_all_series(tenant_hash, &snapshot, matchers)
                .await?;
            let mut by_id: HashMap<SeriesId, LabelSet> = HashMap::new();
            for segment_entries in fetched {
                for entry in segment_entries {
                    by_id.entry(entry.series_id).or_insert(entry.labels);
                }
            }
            if by_id.len() > self.config.max_series {
                return Err(QueryError::TooManySeries {
                    count: by_id.len(),
                    max: self.config.max_series,
                });
            }
            Ok(by_id.into_iter().collect())
        };

        self.resolve_snapshot_with_retry(
            tenant_hash,
            window,
            min_tokens,
            now_ns,
            name_filter,
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
            return Ok((MergedSource { series: Vec::new() }, QueryStats::default()));
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
        let concurrency = self.config.fetch_concurrency.max(1);
        let attempt = |snapshot: Snapshot| async move {
            // Owned clones, not borrowed slice items: a closure capturing a
            // reference into `plans` through this combinator chain makes
            // rustc infer a fixed (non-higher-ranked) lifetime for the
            // closure, which later fails to unify with axum's `Handler`
            // blanket impl ("implementation of FnOnce is not general
            // enough") at the router call site in `http/mod.rs`. Cloning
            // each `SelectorPlan` into the future sidesteps that entirely.
            let results: Vec<Result<Vec<SeriesData>, QueryError>> = stream::iter(plans.to_vec())
                .map(|plan| {
                    let snapshot = &snapshot;
                    async move {
                        let fetched = self
                            .fetch_all_samples_soa(tenant_hash, snapshot, &plan.matchers)
                            .await?;
                        merge_soa_runs(fetched, max_series, max_samples)
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            let mut combined: HashMap<LabelSet, SeriesData> = HashMap::new();
            for r in results {
                for series in r? {
                    combined.entry(series.labels.clone()).or_insert(series);
                }
            }
            Ok(MergedSource {
                series: combined.into_values().collect(),
            })
        };

        self.resolve_snapshot_with_retry(
            tenant_hash,
            padded,
            min_tokens,
            now_ns,
            name_filter,
            attempt,
        )
        .await
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
        mut attempt: F,
    ) -> Result<(T, QueryStats), QueryError>
    where
        F: FnMut(Snapshot) -> Fut,
        Fut: std::future::Future<Output = Result<T, QueryError>>,
    {
        let first = self
            .resolve_bounded(tenant_hash, window, min_tokens, now_ns, name_filter)
            .await?;
        let first_stats = QueryStats::from_snapshot(&first);
        match attempt(first).await {
            Err(QueryError::Fetch(FetchError::Store {
                source: StoreError::NotFound,
                ..
            })) => {
                let second = self
                    .resolve_bounded(tenant_hash, window, min_tokens, now_ns, name_filter)
                    .await?;
                let second_stats = QueryStats::from_snapshot(&second);
                match attempt(second).await {
                    Err(QueryError::Fetch(FetchError::Store {
                        source: StoreError::NotFound,
                        ..
                    })) => Err(QueryError::SnapshotInvalidated),
                    Ok(t) => Ok((t, second_stats)),
                    Err(other) => Err(other),
                }
            }
            Ok(t) => Ok((t, first_stats)),
            Err(other) => Err(other),
        }
    }

    async fn resolve_bounded(
        &self,
        tenant_hash: TenantHash,
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
    ) -> Result<Snapshot, QueryError> {
        let snapshot = self
            .catalog
            .resolve_pruned(
                &tenant_hash,
                Signal::Metrics,
                window,
                min_tokens,
                now_ns,
                name_filter,
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

    /// Fetches every matched series' samples from each snapshot segment as
    /// per-segment SoA runs (`fetch_soa`), one `Vec<FetchedSeriesSoa>` per
    /// segment. The runs are handed to [`merge_soa_runs`] for the lazy
    /// k-way merge; the per-segment `FetchStats` are not consumed on this
    /// path (issue #25, X1).
    async fn fetch_all_samples_soa(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Vec<FetchedSeriesSoa>>, QueryError> {
        let concurrency = self.config.fetch_concurrency.max(1);
        let matchers: Arc<Vec<LabelMatcher>> = Arc::new(matchers.to_vec());
        let results: Vec<Result<Vec<FetchedSeriesSoa>, FetchError>> =
            stream::iter(snapshot.segments.iter().cloned())
                .map(|seg_ref| {
                    let fetcher = self.fetcher.clone();
                    let matchers = Arc::clone(&matchers);
                    async move {
                        fetcher
                            .fetch_soa(tenant_hash, &seg_ref, matchers.as_slice())
                            .await
                            .map(|(runs, _stats)| runs)
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    }

    async fn fetch_all_series(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Vec<ravel_segment::SeriesEntry>>, QueryError> {
        let concurrency = self.config.fetch_concurrency.max(1);
        let matchers: Arc<Vec<LabelMatcher>> = Arc::new(matchers.to_vec());
        let results: Vec<Result<Vec<ravel_segment::SeriesEntry>, FetchError>> =
            stream::iter(snapshot.segments.iter().cloned())
                .map(|seg_ref| {
                    let fetcher = self.fetcher.clone();
                    let matchers = Arc::clone(&matchers);
                    async move {
                        fetcher
                            .fetch_series(tenant_hash, &seg_ref, matchers.as_slice())
                            .await
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
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

/// The literal metric name a single equality `__name__` matcher pins, or
/// `None` if postings pruning must bypass entirely (docs/metric-index-plan.md
/// P5b): no `__name__` matcher at all, or any `__name__` matcher that is not
/// a lone `=` (a regex, a negation, or more than one `__name__` matcher on
/// the same selector all take the conservative bypass path).
fn equality_name_filter(matchers: &[LabelMatcher]) -> Option<&str> {
    let mut found: Option<&str> = None;
    for m in matchers {
        if m.name == METRIC_NAME_LABEL {
            match &m.op {
                MatchOp::Eq if found.is_none() => found = Some(m.value.as_str()),
                _ => return None,
            }
        }
    }
    found
}

/// The equality `__name__` filter shared by every selector in a
/// multi-selector query (docs/metric-index-plan.md P5b). `prefetch` resolves
/// one snapshot shared across all of a query's selectors (e.g. `foo + bar`),
/// so pruning only applies when every selector agrees on one literal name;
/// otherwise a filter narrower than some other selector's own matchers would
/// silently drop segments that selector still needs, so this bypasses (`None`)
/// on any disagreement or on any selector with no equality name of its own.
fn shared_equality_name_filter<'a>(plans: &'a [SelectorPlan]) -> Option<&'a str> {
    let mut shared: Option<&'a str> = None;
    for plan in plans {
        let name = equality_name_filter(&plan.matchers)?;
        match shared {
            None => shared = Some(name),
            Some(s) if s == name => {}
            Some(_) => return None,
        }
    }
    shared
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
/// order in [`is_greater`].
fn merge_soa_runs(
    fetched: Vec<Vec<FetchedSeriesSoa>>,
    max_series: usize,
    max_samples: usize,
) -> Result<Vec<SeriesData>, QueryError> {
    let mut by_series: HashMap<SeriesId, (LabelSet, Vec<SeriesRun>)> = HashMap::new();
    for segment_series in fetched {
        for fs in segment_series {
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

    if by_series.len() > max_series {
        return Err(QueryError::TooManySeries {
            count: by_series.len(),
            max: max_series,
        });
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
// comparison by bit pattern. `ravel-promql`'s `SeriesData`/`SeriesSource`
// remain scalar-only, and wiring this into a live query path additionally
// needs `fetcher.rs` to decode RSEG v3/HIST_PAGES, both out of this
// ticket's "Work in" scope (crates/ravel-segment, crates/ravel-ingest,
// crates/ravel-query only) -- so nothing here is called by
// `merge_soa_runs`/`MergedSource` yet. The extension is proven directly by
// the tests below, constructing `HistogramValue`s by hand rather than via
// OTLP/RW decode, mirroring how phase C7's ingest plumbing is proven.

/// Bit-pattern total order over one [`HistogramValue`]'s full structure:
/// every float field compared by its raw bit pattern (never `==`/`PartialOrd`
/// on `f64` itself), so NaN payloads and -0.0 are significant, matching
/// `f64::to_bits()`'s role in the scalar tiebreak ([`is_greater`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
fn reset_hint_rank(hint: ravel_segment::ResetHint) -> u8 {
    use ravel_segment::ResetHint;
    match hint {
        ResetHint::Unknown => 0,
        ResetHint::Yes => 1,
        ResetHint::No => 2,
        ResetHint::Gauge => 3,
    }
}

#[allow(dead_code)]
fn spans_sort_key(spans: &[ravel_segment::HistogramSpan]) -> Vec<(i32, u32)> {
    spans.iter().map(|s| (s.offset, s.length)).collect()
}

#[allow(dead_code)]
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
#[allow(dead_code)]
struct HistogramCandidate {
    value: ravel_segment::HistogramValue,
    priority: (i64, u64, u64, u32),
}

/// Histogram counterpart to [`is_greater`]: same priority-prefix-first
/// total order, tie-broken by [`histogram_sort_key`] instead of a plain
/// `value.to_bits()`.
#[allow(dead_code)]
fn histogram_is_greater(a: &HistogramCandidate, b: &HistogramCandidate) -> bool {
    (a.priority, histogram_sort_key(&a.value)) > (b.priority, histogram_sort_key(&b.value))
}

/// Histogram counterpart to [`SeriesRun`]: one decoded per-segment run of a
/// single histogram-kind series, kept in on-disk order.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HistogramSeriesRun {
    timestamps: Vec<i64>,
    values: Vec<ravel_segment::HistogramValue>,
    prefix: (i64, u64, u64),
}

/// Histogram counterpart to [`merge_series_runs`]: identical k-way merge
/// shape (min-heap by ts, drain same-ts heads, [`histogram_is_greater`]
/// picks the winner), substituting [`HistogramCandidate`] for [`Candidate`]
/// and yielding [`ravel_segment::HistogramSample`]s.
#[allow(dead_code)]
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

/// A `SeriesSource` backed by the per-series output of the lazy k-way merge
/// ([`merge_soa_runs`]), not a materialized per-timestamp window
/// (docs/query-engine.md / `ravel_promql::source` module doc: by the time
/// the evaluator runs, everything is plain, synchronous, in-memory data).
struct MergedSource {
    series: Vec<SeriesData>,
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
        IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment,
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

    fn engine(store: Arc<MemoryStore>) -> QueryEngine {
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let catalog =
            Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
        QueryEngine::new(Arc::new(catalog), backend, EngineConfig::default())
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
}
