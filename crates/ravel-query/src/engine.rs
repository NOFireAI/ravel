//! QueryEngine: snapshot resolution, bounded-concurrency segment fetch,
//! cross-segment duplicate-sample resolution, and PromQL evaluation
//! (docs/query-engine.md "Flow", docs/catalog-and-mvcc.md).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use promql_parser::parser::{Expr, Offset, VectorSelector};
use ravel_catalog::{Catalog, Snapshot};
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_promql::{
    Evaluator, InstantVector, LabelMatcher, RangeMatrix, SeriesData, SeriesSource, SourceError,
    from_ast_matchers, has_or_group, matches_series, ms_to_ns,
};
use ravel_types::{CommitToken, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TimeRange};

use crate::config::EngineConfig;
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchedSeries, SegmentFetcher};

/// Must match `ravel_promql::Evaluator`'s default lookback exactly (that
/// constant is private): the engine needs the lookback duration to compute
/// its pre-fetch window *before* the evaluator runs. Phase 1 does not
/// support per-query lookback overrides.
const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

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
    ) -> Result<InstantVector, QueryError> {
        tokio::time::timeout(
            deadline,
            self.instant_inner(tenant_hash, query, t_ms, min_tokens, now_ns),
        )
        .await
        .map_err(|_| QueryError::DeadlineExceeded { deadline })?
    }

    async fn instant_inner(
        &self,
        tenant_hash: TenantHash,
        query: &str,
        t_ms: i64,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<InstantVector, QueryError> {
        let (matchers, offset_ns) = parse_selector(query)?;
        let t_ns = ms_to_ns(t_ms)?;
        let sel_ts_ns = t_ns.checked_sub(offset_ns).ok_or(QueryError::TimeOverflow)?;
        let window = TimeRange {
            start_ns: sel_ts_ns
                .checked_sub(DEFAULT_LOOKBACK_NS)
                .ok_or(QueryError::TimeOverflow)?,
            end_ns: sel_ts_ns,
        };
        let source = self
            .resolve_and_materialize(tenant_hash, &matchers, window, min_tokens, now_ns)
            .await?;
        Ok(Evaluator::new().instant(&source, query, t_ms)?)
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
    ) -> Result<RangeMatrix, QueryError> {
        tokio::time::timeout(
            deadline,
            self.range_inner(tenant_hash, query, start_ms, end_ms, step_ms, min_tokens, now_ns),
        )
        .await
        .map_err(|_| QueryError::DeadlineExceeded { deadline })?
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
    ) -> Result<RangeMatrix, QueryError> {
        if step_ms <= 0 {
            return Err(QueryError::NonPositiveStep { step_ms });
        }
        if start_ms > end_ms {
            return Err(QueryError::InvalidRange { start_ms, end_ms });
        }
        let (matchers, offset_ns) = parse_selector(query)?;
        let start_ns = ms_to_ns(start_ms)?;
        let end_ns = ms_to_ns(end_ms)?;
        let step_ns = ms_to_ns(step_ms)?;
        let window = range_fetch_window(start_ns, end_ns, step_ns, offset_ns)?;
        let source = self
            .resolve_and_materialize(tenant_hash, &matchers, window, min_tokens, now_ns)
            .await?;
        Ok(Evaluator::new().range(&source, query, start_ms, end_ms, step_ms)?)
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
    ) -> Result<Vec<(SeriesId, LabelSet)>, QueryError> {
        let attempt = |snapshot: Snapshot| async move {
            let fetched = self.fetch_all_series(tenant_hash, &snapshot, matchers).await?;
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

        self.resolve_snapshot_with_retry(tenant_hash, window, min_tokens, now_ns, attempt)
            .await
    }

    async fn resolve_and_materialize(
        &self,
        tenant_hash: TenantHash,
        matchers: &[LabelMatcher],
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<MaterializedSource, QueryError> {
        let max_series = self.config.max_series;
        let max_samples = self.config.max_samples;
        let attempt = |snapshot: Snapshot| async move {
            let fetched = self.fetch_all_samples(tenant_hash, &snapshot, matchers).await?;
            let series = merge_segments(fetched, max_series, max_samples)?;
            Ok(MaterializedSource { series })
        };

        self.resolve_snapshot_with_retry(tenant_hash, window, min_tokens, now_ns, attempt)
            .await
    }

    /// Resolves a snapshot, enforces `max_segments`, runs `attempt` once,
    /// and on a store `NotFound` (a pinned segment vanished under a
    /// concurrent GC/compaction) re-resolves and retries the whole query
    /// exactly once before giving up with `SnapshotInvalidated`
    /// (docs/catalog-and-mvcc.md).
    async fn resolve_snapshot_with_retry<T, F, Fut>(
        &self,
        tenant_hash: TenantHash,
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        mut attempt: F,
    ) -> Result<T, QueryError>
    where
        F: FnMut(Snapshot) -> Fut,
        Fut: std::future::Future<Output = Result<T, QueryError>>,
    {
        let first = self.resolve_bounded(tenant_hash, window, min_tokens, now_ns).await?;
        match attempt(first).await {
            Err(QueryError::Fetch(FetchError::Store {
                source: StoreError::NotFound,
                ..
            })) => {
                let second = self
                    .resolve_bounded(tenant_hash, window, min_tokens, now_ns)
                    .await?;
                match attempt(second).await {
                    Err(QueryError::Fetch(FetchError::Store {
                        source: StoreError::NotFound,
                        ..
                    })) => Err(QueryError::SnapshotInvalidated),
                    other => other,
                }
            }
            other => other,
        }
    }

    async fn resolve_bounded(
        &self,
        tenant_hash: TenantHash,
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<Snapshot, QueryError> {
        let snapshot = self
            .catalog
            .resolve(&tenant_hash, Signal::Metrics, window, min_tokens, now_ns)
            .await?;
        if snapshot.segments.len() > self.config.max_segments {
            return Err(QueryError::TooManySegments {
                count: snapshot.segments.len(),
                max: self.config.max_segments,
            });
        }
        Ok(snapshot)
    }

    async fn fetch_all_samples(
        &self,
        tenant_hash: TenantHash,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Vec<FetchedSeries>>, QueryError> {
        let concurrency = self.config.fetch_concurrency.max(1);
        let results: Vec<Result<Vec<FetchedSeries>, FetchError>> =
            stream::iter(snapshot.segments.iter())
                .map(|seg_ref| async move { self.fetcher.fetch(tenant_hash, seg_ref, matchers).await })
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
        let results: Vec<Result<Vec<ravel_segment::SeriesEntry>, FetchError>> =
            stream::iter(snapshot.segments.iter())
                .map(|seg_ref| async move {
                    self.fetcher.fetch_series(tenant_hash, seg_ref, matchers).await
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

fn range_fetch_window(
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    offset_ns: i64,
) -> Result<TimeRange, QueryError> {
    let span = end_ns.checked_sub(start_ns).ok_or(QueryError::TimeOverflow)?;
    let num_steps = span / step_ns;
    let step_span = num_steps.checked_mul(step_ns).ok_or(QueryError::TimeOverflow)?;
    let last_grid_ns = start_ns.checked_add(step_span).ok_or(QueryError::TimeOverflow)?;
    let sel_start = start_ns.checked_sub(offset_ns).ok_or(QueryError::TimeOverflow)?;
    let sel_end = last_grid_ns.checked_sub(offset_ns).ok_or(QueryError::TimeOverflow)?;
    Ok(TimeRange {
        start_ns: sel_start
            .checked_sub(DEFAULT_LOOKBACK_NS)
            .ok_or(QueryError::TimeOverflow)?,
        end_ns: sel_end,
    })
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
/// greatest-wins comparison in [`is_greater`].
#[derive(Debug, Clone, Copy)]
struct Candidate {
    ts_ns: i64,
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

fn merge_segments(
    fetched: Vec<Vec<FetchedSeries>>,
    max_series: usize,
    max_samples: usize,
) -> Result<Vec<SeriesData>, QueryError> {
    let mut by_series: HashMap<SeriesId, (LabelSet, HashMap<i64, Candidate>)> = HashMap::new();
    for segment_series in fetched {
        for fs in segment_series {
            let entry = by_series
                .entry(fs.series_id)
                .or_insert_with(|| (fs.labels.clone(), HashMap::new()));
            for (idx, sample) in fs.samples.iter().enumerate() {
                let in_page_index = u32::try_from(idx).unwrap_or(u32::MAX);
                let candidate = Candidate {
                    ts_ns: sample.ts_ns,
                    value: sample.value,
                    priority: (fs.created_unix_ns, fs.writer_epoch, fs.writer_seq, in_page_index),
                };
                entry
                    .1
                    .entry(sample.ts_ns)
                    .and_modify(|existing| {
                        if is_greater(&candidate, existing) {
                            *existing = candidate;
                        }
                    })
                    .or_insert(candidate);
            }
        }
    }

    if by_series.len() > max_series {
        return Err(QueryError::TooManySeries {
            count: by_series.len(),
            max: max_series,
        });
    }

    let mut total_samples: usize = 0;
    let mut out = Vec::with_capacity(by_series.len());
    for (labels, ts_map) in by_series.into_values() {
        total_samples = total_samples.saturating_add(ts_map.len());
        if total_samples > max_samples {
            return Err(QueryError::TooManySamples {
                count: total_samples,
                max: max_samples,
            });
        }
        let mut samples: Vec<Sample> = ts_map
            .into_values()
            .map(|c| Sample {
                ts_ns: c.ts_ns,
                value: c.value,
            })
            .collect();
        samples.sort_by_key(|s| s.ts_ns);
        out.push(SeriesData { labels, samples });
    }
    Ok(out)
}

/// A `SeriesSource` over a query's already-fetched, already-merged samples
/// (docs/query-engine.md / `ravel_promql::source` module doc: by the time
/// the evaluator runs, everything is plain, synchronous, in-memory data).
struct MaterializedSource {
    series: Vec<SeriesData>,
}

impl SeriesSource for MaterializedSource {
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
