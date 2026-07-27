//! QueryEngine: snapshot resolution, bounded-concurrency segment fetch,
//! cross-segment duplicate-sample resolution, and PromQL evaluation
//! (docs/query-engine.md "Flow", docs/catalog-and-mvcc.md).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use promql_parser::parser::{Expr, Offset};
use ravel_catalog::{Catalog, Snapshot};
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_promql::{
    Evaluator, InstantVector, LabelMatcher, RangeMatrix, SeriesData, SeriesSource, SourceError,
    from_ast_matchers, has_or_group, matches_series, ms_to_ns,
};
use ravel_types::{
    CommitToken, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TimeRange,
};

use crate::config::EngineConfig;
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchedSeriesSoa, SegmentFetcher};

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
        let sel_ts_ns = t_ns
            .checked_sub(offset_ns)
            .ok_or(QueryError::TimeOverflow)?;
        let window = TimeRange {
            start_ns: sel_ts_ns
                .checked_sub(DEFAULT_LOOKBACK_NS)
                .ok_or(QueryError::TimeOverflow)?,
            end_ns: sel_ts_ns,
        };
        let source = self
            .resolve_and_merge(tenant_hash, &matchers, window, min_tokens, now_ns)
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
            self.range_inner(
                tenant_hash,
                query,
                start_ms,
                end_ms,
                step_ms,
                min_tokens,
                now_ns,
            ),
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
            .resolve_and_merge(tenant_hash, &matchers, window, min_tokens, now_ns)
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

        self.resolve_snapshot_with_retry(tenant_hash, window, min_tokens, now_ns, attempt)
            .await
    }

    async fn resolve_and_merge(
        &self,
        tenant_hash: TenantHash,
        matchers: &[LabelMatcher],
        window: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<MergedSource, QueryError> {
        let max_series = self.config.max_series;
        let max_samples = self.config.max_samples;
        let attempt = |snapshot: Snapshot| async move {
            let fetched = self
                .fetch_all_samples_soa(tenant_hash, &snapshot, matchers)
                .await?;
            let series = merge_soa_runs(fetched, max_series, max_samples)?;
            Ok(MergedSource { series })
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
        let first = self
            .resolve_bounded(tenant_hash, window, min_tokens, now_ns)
            .await?;
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

fn range_fetch_window(
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    offset_ns: i64,
) -> Result<TimeRange, QueryError> {
    let span = end_ns
        .checked_sub(start_ns)
        .ok_or(QueryError::TimeOverflow)?;
    let num_steps = span / step_ns;
    let step_span = num_steps
        .checked_mul(step_ns)
        .ok_or(QueryError::TimeOverflow)?;
    let last_grid_ns = start_ns
        .checked_add(step_span)
        .ok_or(QueryError::TimeOverflow)?;
    let sel_start = start_ns
        .checked_sub(offset_ns)
        .ok_or(QueryError::TimeOverflow)?;
    let sel_end = last_grid_ns
        .checked_sub(offset_ns)
        .ok_or(QueryError::TimeOverflow)?;
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
    fn empty_run_yields_empty_series() {
        let seg = vec![vec![run(1, &[], &[], 0, 0, 0)]];
        let out = merge(seg);
        assert_eq!(out.len(), 1);
        assert!(out[0].samples.is_empty());
    }
}
