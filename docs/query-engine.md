# Query Engine Design (Phase 1)

Implementer contract for `ravel-query` and the query side of `ravel-server`.
ADR-0006/0007 give the reasoning; docs/consistency-model.md the semantics.

## Flow

```
HTTP /api/v1/query, /query_range, /labels, /label/{name}/values, /series
  -> auth + tenant resolve
  -> ravel_promql::plan_selectors(query, eval_start, eval_end): a static AST
     walk, independent of evaluation, reporting every selector's matchers,
     range, offset, and @-anchor (ADR-0021 SS1)
  -> Catalog::resolve(tenant, Metrics, padded_range, min_commit_token), one
     shared snapshot resolved against the union of every selector's own
     fetch window
  -> per selector (concurrency-bounded, default 8), against that one
     snapshot:
       for each snapshot segment: suffix GET (64 KiB) -> Reader::parse
         -> prune series by that selector's own matchers (SERIES_TABLE +
            LABEL_DICT)
         -> plan page ranges, coalesce adjacent (gap <= 64 KiB)
         -> ranged GETs -> decode pages -> per-series samples
       -> lazy k-way merge of that selector's per-segment SoA runs per
          series id (each run already ascending by ts; the merge emits
          ascending, one sample per ts, no final sort). Duplicate
          timestamps (across segments and within one run) resolve under
          the total order in docs/catalog-and-mvcc.md, not by arrival
          order; max_series/max_samples are enforced independently per
          selector (each selector gets the full budget, not a shared
          split; max_segments stays a per-query cap on the shared
          snapshot)
  -> every selector's already-merged series combined into one flat
     SeriesSource (later selectors sharing a series id keep the first
     merge seen; a later per-selector SeriesSource::query call still clips
     to that selector's own window, so which selector "wins" a shared
     series id in the combine step does not affect any single selector's
     result)
  -> ravel-promql Evaluator -> Value (scalar / string / instant vector /
     range matrix)
  -> Prometheus JSON envelope {status, data:{resultType, result}, warnings}
```

`padded_range`: the union, over every selector `plan_selectors` reports, of
that selector's own fetch window (its lookback or matrix range, plus its
own offset, anchored per `PlanAnchor::Window`/`Pinned`), so lookback never
misses samples stored in an earlier-only segment for any selector, however
widely the selectors' own windows differ.

Staleness: the evaluator recognizes the Prometheus staleness marker (the
exact NaN bit pattern `0x7ff0_0000_0000_0002`, compared via
`f64::to_bits()`, never `is_nan()`). A selector whose newest in-window
sample is the marker treats the series as absent at that instant; range
windows exclude marker samples. Every other NaN payload is a live value
and passes through bit-exactly (issue #75).

## Time-range and matcher pruning order

1. Segment level: commit-record event-time bounds vs padded range (already
   done by Catalog::resolve).
2. Series level: SERIES_TABLE entry ts bounds vs padded range, then matcher
   evaluation against the decoded LabelSet. Equality matchers use dictionary
   ordinal lookups (resolve value -> ordinal once, compare ordinals);
   regex/negative matchers evaluate on materialized label sets.
3. Page level: v1 has one page pair per series; nothing further to prune.

## Endpoints (Prometheus compatibility subset)

- `POST/GET /api/v1/query` (params: query, time, timeout) instant.
- `POST/GET /api/v1/query_range` (query, start, end, step, timeout).
- `GET /api/v1/labels`, `/api/v1/label/{name}/values` (match[] optional,
  start/end optional): from snapshot SERIES_TABLE label dictionaries.
- `GET/POST /api/v1/series` (match[] required, start/end).
- All accept `min_commit_token`. Errors use the Prometheus JSON error
  envelope (`status:"error"`, `errorType`, `error`) with correct HTTP codes
  (400 bad_data, 422 unprocessable for unsupported constructs, 503
  unavailable for store failures).
- Timestamps: Prometheus float seconds in, RFC3339 or float accepted like
  Prometheus; responses use float seconds with ms precision.
- `resultType`: `vector`, `matrix`, `scalar`, `string`. A top-level scalar
  or string result from `/api/v1/query_range` is rendered Prometheus-style
  as a `matrix` with one synthetic empty-labeled series repeating that
  value at every evaluated grid step (`ravel-promql`'s `eval_range`
  resolves it once; materializing the repetition is this HTTP layer's
  job, not the evaluator's).

## Budgets (Phase 1: static config)

Per query: max segments touched (1024, a cap on the one shared snapshot,
not per selector), max concurrent GETs (8, applied both across selectors
and across one selector's own segments), max matched series (10k) and max
samples (10M) enforced independently per selector (a query with several
selectors, e.g. a future binary/aggregate expression, grants each its own
full budget rather than splitting one budget across them), max
range-evaluation points per query (11,000, matching the Prometheus
resolution limit; issue #77), wall deadline (server maximum, default
30 s). The `timeout` param can only lower the deadline: values above the
server maximum are clamped to it (issue #58). Exceeding a budget returns a
Prometheus-style error, never a partial silent result.

The max-samples budget is **count-yielded**: samples are counted as the
lazy k-way merge emits them (post-dedup), and the budget trips at exactly
`max + 1`. It does not count a fully materialized per-timestamp window
before checking, so it does not silently pass an over-budget query that
happens to dedup down to size. The count is independent of segment or
series iteration order, so the error is deterministic. It bounds only the
output size: every matched series in every matched segment is still fully
fetched and SoA-decoded before the merge runs, so peak fetch/decode memory
scales with the query's matched input, not with `max_samples`.

## Caching note

Phase 1 caches only decoded commit records (immutable) and parsed footers
keyed by (object key, etag), both in-memory LRU. Anything else waits for
measurements.
