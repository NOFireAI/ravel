# Query Engine Design (Phase 1)

Implementer contract for `ravel-query` and the query side of `ravel-server`.
ADR-0006/0007 give the reasoning; docs/consistency-model.md the semantics.

## Flow

```
HTTP /api/v1/query, /query_range, /labels, /label/{name}/values, /series
  -> auth + tenant resolve
  -> parse PromQL (ravel-promql), extract selector matchers + range
  -> Catalog::resolve(tenant, Metrics, padded_range, min_commit_token)
  -> for each snapshot segment (concurrency-bounded, default 8):
       suffix GET (64 KiB) -> Reader::parse
       -> prune series by matchers (SERIES_TABLE + LABEL_DICT)
       -> plan page ranges, coalesce adjacent (gap <= 64 KiB)
       -> ranged GETs -> decode pages -> per-series samples
  -> lazy k-way merge of the per-segment SoA runs per series id (each run
     already ascending by ts; the merge emits ascending, one sample per ts,
     no final sort). Duplicate timestamps (across segments and within one
     run) resolve under the total order in docs/catalog-and-mvcc.md, not by
     arrival order; the max-samples budget is counted on yield (below).
  -> SeriesSource impl backed by that merged per-series output
  -> ravel-promql Evaluator -> instant vector / range matrix
  -> Prometheus JSON envelope {status, data:{resultType, result}, warnings}
```

`padded_range`: query range extended left by lookback delta (5 m) plus the
largest selector offset, so lookback never misses samples stored in an
earlier-only segment.

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

## Budgets (Phase 1: static config)

Per query: max segments touched (1024), max concurrent GETs (8), max
matched series (10k), max samples (10M), wall deadline (default 30 s,
`timeout` param can lower it). Exceeding a budget returns a Prometheus-style
error, never a partial silent result.

The max-samples budget is **count-yielded**: samples are counted as the
lazy k-way merge emits them (post-dedup), and the budget trips at exactly
`max + 1`. It does not count a fully materialized window before checking,
so peak merge work is bounded by the budget itself rather than by the
query's full deduplicated result size. The count is independent of segment
or series iteration order, so the error is deterministic.

## Caching note

Phase 1 caches only decoded commit records (immutable) and parsed footers
keyed by (object key, etag), both in-memory LRU. Anything else waits for
measurements.
