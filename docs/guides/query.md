# Query

![query path](../diagrams/query-path.svg)

All five endpoints live under `/api/v1` on `--listen-http`, require the same
tenant authentication as ingest (`Authorization: Bearer <token>`, or the
dev header if `--dev-insecure-tenant-header` is set), and return the same
Prometheus-compatible JSON envelope:

```json
{"status": "success", "data": {...}}
{"status": "error", "errorType": "bad_data", "error": "..."}
```

## Endpoints

### `GET/POST /api/v1/query`

Instant query. Params: `query` (required), `time` (optional, Prometheus
timestamp format, defaults to now), `min_commit_token` (repeatable),
`timeout` (optional).

```sh
curl -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode 'query=demo_requests_total{job="checkout"}' \
  --data-urlencode 'time=1732400000'
```

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {"metric": {"__name__": "demo_requests_total", "job": "checkout"}, "value": [1732400000, "42"]}
    ]
  }
}
```

### `GET/POST /api/v1/query_range`

Range query. Params: `query`, `start`, `end`, `step` (all required),
`min_commit_token` (repeatable), `timeout` (optional).

```sh
curl -G http://127.0.0.1:4318/api/v1/query_range \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode 'query=demo_requests_total' \
  --data-urlencode 'start=1732400000' \
  --data-urlencode 'end=1732400300' \
  --data-urlencode 'step=30s'
```

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {"metric": {"__name__": "demo_requests_total"}, "values": [[1732400000, "40"], [1732400030, "41"]]}
    ]
  }
}
```

### `GET /api/v1/labels`

Label names across matched series. Params: `match[]` (optional, repeatable;
omit it to match every series in the window), `start`, `end`,
`min_commit_token` (repeatable).

```sh
curl -G http://127.0.0.1:4318/api/v1/labels \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode 'match[]=demo_requests_total'
```

```json
{"status": "success", "data": ["__name__", "instance", "job"]}
```

### `GET /api/v1/label/{name}/values`

Values seen for one label name, same params as `/labels`.

```sh
curl -G http://127.0.0.1:4318/api/v1/label/job/values \
  -H "Authorization: Bearer devtoken"
```

```json
{"status": "success", "data": ["checkout", "payments"]}
```

### `GET/POST /api/v1/series`

Series (as label sets, no values) matching one or more selectors. Params:
`match[]` (required, repeatable, at least one), `start`, `end`,
`min_commit_token` (repeatable).

```sh
curl -G http://127.0.0.1:4318/api/v1/series \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode 'match[]=demo_requests_total{job="checkout"}'
```

```json
{"status": "success", "data": [{"__name__": "demo_requests_total", "job": "checkout"}]}
```

Omitting `match[]` on `/series` is a `400 bad_data` error
(`missing required parameter "match[]"`); `/labels` and `/label/{name}/values`
allow it and match every series in the window instead.

`/labels`, `/label/{name}/values`, and `/series` default their window to the
hour before now (`start`/`end` unset). If more than one `match[]` selector
is given, each one resolves its own catalog snapshot independently and the
results are unioned by series identity, rather than all selectors sharing
one snapshot for the request.

## PromQL subset

Ravel's evaluator (`ravel-promql`) supports exactly one AST shape: a bare
vector selector, optionally with `offset`. Everything else is rejected with
`422 unprocessable_entity` and an error naming the construct:

| Rejected | Error names it as |
|---|---|
| `rate(x[5m])`, any function call | `function call: rate` |
| `sum(x)`, any aggregation | `aggregation: sum` |
| `x + y`, any binary operator | `binary expression: +` |
| `-x`, unary expressions | `unary expression` |
| `(x)`, parens | `paren expression` |
| `x[5m]`, a bare matrix selector | `matrix selector` |
| `x[5m:1m]`, a subquery | `subquery` |
| `x @ 100`, the `@` modifier | `@` |
| `x{job="a" or job="b"}`, an or-grouped matcher | `label matcher or-group` |
| a number or string literal alone | `number literal` / `string literal` |

What is supported, precisely:

- All four matcher operators: `=`, `!=`, `=~`, `!~`.
- Absent-label semantics match Prometheus: an absent label reads as an
  empty string for every operator. `{foo=""}` matches series without
  `foo`; `{foo=~".*"}` matches everything, including series without `foo`;
  `{foo!=""}` matches only series where `foo` is present and non-empty;
  `{foo=~""}` matches only series where `foo` is absent (the regex is
  anchored, so this is `^(?:)$`, which only an empty string satisfies).
  Regex matchers are always fully anchored (`^(?:pattern)$`), matching
  Prometheus, so `job=~"api"` does not match `job="api-server"`.
- `offset`, both the standard positive form (look backward) and the
  negative form (look forward, experimental in upstream PromQL too).
- A fixed 5-minute lookback: at evaluation instant `T` (shifted by
  `offset` if present), a series' value is its most recent sample with
  timestamp in `(T - 5m, T]`. The window's start is exclusive: a sample
  exactly 5 minutes old is not used; a series with no sample in that window
  is omitted from the result entirely, not reported as absent or zero.

## `min_commit_token`

Pass a commit token from an ingest response's `x-ravel-commit-token`
header as `min_commit_token` (repeatable if you have more than one, e.g.
from a request that flushed to multiple shards) to guarantee the query
sees that write. The catalog resolves each token to its exact commit
record directly rather than depending on a listing that might race the
write. If a token can't be resolved, the query fails outright
(`503 unavailable`) instead of silently returning a snapshot older than
what you asked for. See [docs/guides/ingest.md](ingest.md#commit-tokens-and-read-your-write).

## Query budgets

Every query is bounded, and every bound is a typed error, never a silent
truncation ([crates/ravel-query/src/config.rs](../../crates/ravel-query/src/config.rs)):

| Budget | Default | Error when exceeded |
|---|---|---|
| Segments touched | 1,024 | `query matched {count} segments, exceeding the limit of {max}` |
| Distinct series | 10,000 | `query matched {count} series, exceeding the limit of {max}` |
| Samples materialized | 10,000,000 | `query matched {count} samples, exceeding the limit of {max}` |
| Concurrent segment fetches | 8 | (not user-visible; throughput knob only) |
| Wall-clock deadline | 30s | `query exceeded its deadline of {deadline}` |

`timeout` (Prometheus duration syntax like `30s`/`5m`, or bare float
seconds) lowers the deadline per request; it cannot raise it above the
server's configured default.

## SQL over the `logs` table

`POST /api/v1/sql` (see README "SQL") serves two tables from one endpoint:
`samples` (metrics) and `logs`. DataFusion picks the table from the query's
`FROM` clause; a single query may reference one or the other, never both (a
query naming both is rejected with HTTP 400). The request body, auth, window
(`start`/`end`), and `min_commit_token` handling are identical to the `samples`
case.

The `logs` table columns are `ts`, `observed_ts` (both `Timestamp(ns)`),
`severity_num`, `severity_text`, `body`, `trace_id`, `span_id`, `flags`, and an
`attrs` `Map(Utf8, Utf8)` merging each record's resource, scope, and per-record
attributes (see docs/query-engine.md for the full schema and semantics).

A `ts` range scan. `ts` is a timestamp, so the bounds are `TIMESTAMP` literals,
not bare integers:

```sh
curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "SELECT ts, severity_text, body FROM logs WHERE ts >= TIMESTAMP '\''2026-07-30 00:00:00'\'' ORDER BY ts LIMIT 100",
        "start": 1785369600.0,
        "end": 1785373200.0
      }'
```

A word/phrase content search with `has_word(body, 'literal')`, which pushes
down to the RLOG bloom-accelerated scan and matches whole tokens (so `timeout`
matches `connection timeout` but not `timed out`):

```sh
curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT ts, body FROM logs WHERE has_word(body, '\''timeout'\'') ORDER BY ts"}'
```

Filtering by an attribute value (the `attrs['service.name'] = 'api'` shape) is
**not yet available over SQL**: this build registers no nested-expression
planner, so the `attrs['k']` subscript fails query planning with a loud error
rather than returning a wrong answer (a documented ADR-0033 gap). Until it is
wired, use `has_word` over `body` for content search; only the `attrs['k']`
subscript form is affected, and other predicates (`ts`, `has_word`) are
unaffected.

## HTTP status codes

| Status | `errorType` | When |
|---|---|---|
| 200 | n/a | Success. |
| 400 | `bad_data` | Bad or missing parameter, PromQL parse error, invalid time range, step <= 0. |
| 401 | `unauthorized` | Tenant authentication failed or was not provided. |
| 422 | `execution` | PromQL construct outside the Phase 1 subset, or a query budget (segments/series/samples) exceeded. |
| 503 | `unavailable` | Catalog or segment fetch failed, an unresolvable `min_commit_token`, or a snapshot invalidated by concurrent GC/compaction. |
| 504 | `timeout` | Query exceeded its deadline. |
