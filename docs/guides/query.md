# Query

![query path](../diagrams/query-path.svg)

All five endpoints live under `/api/v1` on `--listen-http`. They require the
same tenant authentication as ingest (`Authorization: Bearer <token>`, or the
dev header if `--dev-insecure-tenant-header` is set). They return the same
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

If you omit `match[]` on `/series`, you get a `400 bad_data` error
(`missing required parameter "match[]"`). `/labels` and `/label/{name}/values`
allow it and match every series in the window instead.

`/labels`, `/label/{name}/values`, and `/series` default their window to the
hour before now (`start`/`end` unset). If you give more than one `match[]`
selector, each one resolves its own catalog snapshot independently, and Ravel
unions the results by series identity. The selectors do not share one snapshot
for the request.

### `GET /api/v1/status/buildinfo` and `GET /api/v1/metadata`

Two routes Grafana's built-in Prometheus datasource probes on every datasource
save. They take no parameters.

```json
{"status": "success", "data": {"version": "0.1.0", "revision": "", "branch": "", "buildUser": "", "buildDate": "", "goVersion": ""}}
```

`version` is Ravel's own version, not a Prometheus one. `revision` is the
build's git SHA when the build exported `RAVEL_GIT_SHA`, empty otherwise.

`/api/v1/metadata` returns real per-metric type, help, and unit for any metric
ingested after ADR-0085 shipped, in Prometheus' documented shape (`data` maps
each family name to a length-1 array of `{type, help, unit}`). It resolves the
requesting tenant from the same bearer credential the other query routes use and
serves that tenant's metadata from a per-process, per-tenant cache (one object
read per tenant per refresh horizon, never a read per request). The optional
`metric` and `limit` query parameters filter to one family and cap the number of
names, matching Prometheus. Metadata is best-effort: a metric ingested before
ADR-0085, or over a path that sent no type/help/unit, has no entry, and a request
that carries no resolvable tenant still gets `{"status": "success", "data": {}}`
(this endpoint never returns `401`). See [ADR-0085](../adrs/0085-metric-metadata-and-otlp-suffixing.md)
for the record format and the OTLP name suffixing that decides the family names.

## PromQL support

Ravel's evaluator (`ravel-promql`) is a full PromQL evaluator, differentially
tested against real Prometheus (ADR-0021, ADR-0035). Function calls,
aggregations, binary operators, subqueries, unary and paren expressions, the
`@` modifier, and vector matching are all supported. The generated conformance
table in [docs/query-engine.md](../query-engine.md#promql-conformance-adr-0035)
is authoritative: at last regeneration it scored 124 supported constructs
(including the 72 non-experimental functions and 12 aggregation operators
promql-parser marks stable, all 16 binary operators, and the AST node and
modifier categories) against 5 intentionally rejected and 2 accepted
divergences.

A handful of constructs are still intentionally rejected. Each answers with a
typed `422 unprocessable_entity` error naming the construct, never a panic and
never silently wrong data. The current set (from
[crates/ravel-promql-difftest/src/scoring.rs](../../crates/ravel-promql-difftest/src/scoring.rs)'s
`REJECTION_CASES`) is:

| Rejected | Error names it as |
|---|---|
| `histogram_stddev(x)` over native histograms | `histogram_stddev` |
| `histogram_stdvar(x)` over native histograms | `histogram_stdvar` |
| `x{job="a" or job="b"}`, an or-grouped matcher | `label matcher or-group` |
| `a + fill(0) b`, vector-matching fill values | `fill-in values` |
| `avg_over_time(x[5m:1m])` over native histograms, a subquery over native histograms | `subquery over native histograms` |

The experimental aggregation operators `limitk` and `limit_ratio` parse but are
also rejected with a typed error naming the operator: they are outside the
stable language and out of the scored surface, not implemented (see
docs/query-engine.md). Subqueries themselves are supported; only a subquery
whose inner expression matches native-histogram data is refused.

Selector details that hold for a bare vector selector:

- All four matcher operators: `=`, `!=`, `=~`, `!~`.
- Absent-label semantics match Prometheus: an absent label reads as an
  empty string for every operator. `{foo=""}` matches series without
  `foo`. `{foo=~".*"}` matches everything, including series without `foo`.
  `{foo!=""}` matches only series where `foo` is present and non-empty.
  `{foo=~""}` matches only series where `foo` is absent (the regex is
  anchored, so this is `^(?:)$`, which only an empty string satisfies).
  Regex matchers are always fully anchored (`^(?:pattern)$`), the same as
  Prometheus, so `job=~"api"` does not match `job="api-server"`.
- `offset`, both the standard positive form (look backward) and the
  negative form (look forward, experimental in upstream PromQL too).
- A fixed 5-minute lookback: at evaluation instant `T` (shifted by
  `offset` if present), a series' value is its most recent sample with a
  timestamp in `(T - 5m, T]`. The window's start is exclusive. A sample
  exactly 5 minutes old is not used. A series with no sample in that window
  is omitted from the result entirely, not reported as absent or zero.

## `min_commit_token`

Pass a commit token from an ingest response's `x-ravel-commit-token`
header as `min_commit_token` to guarantee that the query sees that write. It
is repeatable if you have more than one, for example from a request that
flushed to multiple shards. The catalog resolves each token to its exact commit
record directly, rather than depend on a listing that might race the write. If
the catalog cannot resolve a token, the query fails outright
(`503 unavailable`). It does not silently return a snapshot older than what you
asked for. See [docs/guides/ingest.md](ingest.md#commit-tokens-and-read-your-write).

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
| Catalog list requests | 100,000 | `query window too wide: it would issue an estimated {estimate} catalog list requests, over the limit of {limit}; narrow the query time range and retry` |

`timeout` (Prometheus duration syntax like `30s`/`5m`, or bare float
seconds) lowers the deadline per request. It cannot raise it above the
server's configured default.

SQL queries carry one more bound, a per-query byte ceiling on the DataFusion
memory pool (256 MiB by default). It bounds the memory the query *holds at one
instant* -- what a scan currently has decoded, plus the batch it is handing
downstream, plus whatever aggregate state the operators above it accumulate --
not the number of bytes the query has produced over its lifetime. A full-table
scan over `logs` therefore does not exhaust it merely by being large: the logs
scan streams one block at a time and releases each block before decoding the
next (ADR-0087), so its own contribution tracks block size and partition count.
An `ORDER BY` or a high-cardinality `GROUP BY` over a large result is what
genuinely accumulates. Exceeding the ceiling is an HTTP 422 `execution` error
naming the pool, never a truncated result.

### Operator-configurable budgets (server flags)

Four of these budgets are process-wide server flags (ADR-0088). Each default is
exactly the compiled-in value, so a server started with none of the flags
behaves byte-for-byte as before they existed. All four are process-wide, not
per-tenant.

| Flag | Reaches | Default |
|---|---|---|
| `--fetch-concurrency <N>` | `EngineConfig::fetch_concurrency` | 8 |
| `--max-segments <N>` | `EngineConfig::max_segments` | 1024 |
| `--sql-max-query-bytes <BYTES>` | `SqlConfig::max_query_bytes` (per-query SQL memory pool) | 256 MiB |
| `--sql-tenant-max-bytes <BYTES>` | per-tenant SQL memory ceiling | 1 GiB |

`--fetch-concurrency` is a single knob with three coupled effects, **not**
decoupled by this change: it governs the PromQL/analytics per-query segment
fetch fan-out, the SQL scan partition count (`target_partitions` in
`crates/ravel-sql/src/session.rs`), and object-store GET concurrency (ADR-0087).
Raising it widens all three together; size it against the host's cores and the
store's request budget.

`--max-segments` caps how many segments a single query fans out over. Only the
narrow recent set (`SegmentOrigin::Recent`, roughly the last couple of hours) is
exempt; everything older, including compacted L0/L1 objects, counts toward the
cap. A wide scan over a tenant with many sealed objects hits it directly, so
raise this flag for such a workload.

`--sql-max-query-bytes` bounds a single SQL query's DataFusion memory pool;
`--sql-tenant-max-bytes` bounds the memory one tenant may hold across its
concurrent SQL queries (the multi-tenant isolation ceiling, defaulting to four
times the per-query pool). Both apply only in a build with the `sql` feature.
Per-tenant SQL budgets are **not** configurable in the `--limits-file`: its
per-tenant query overrides have no per-tenant `EngineConfig` lookup at query
time today and are inert (ADR-0088), so these ceilings are process-wide flags
until that gap closes.

`max_bytes_scanned` is **not** a flag. It stays a `--limits-file` entry
(`query_defaults.max_bytes_scanned`, default Unlimited); see
[admission-limits.md](admission-limits.md). `--max-s3-requests` (ADR-0075)
remains a flag; omitted, it is derived from `--shards` and the flush cadence.

`--gc-max-query-duration` sets the engine's enforced wall-clock deadline. It
must be **`<=`** the tenant's durable `sys/gc.max_query_duration` (default 1h).
A value above it is **rejected at startup** (a hard error), not clamped: raise
`sys/gc.max_query_duration` first (`ravel-cli gc-config set`) if you need a
longer engine deadline.

The catalog-list budget is checked before any object-store request is made,
not after. The catalog lists one prefix per (shard, ingest hour) from the
window's start to the current hour, so a query whose `start` reaches far back
(a `start` of `0`, epoch, is the usual cause) can ask for hundreds of
thousands of LIST requests against object storage in a single call. Such a
query is refused up front, before it can run up an object-store bill or
saturate the listing path; the error reports both the estimate and the limit,
so narrow the time range by the reported factor and retry. The ceiling
permits roughly an 11-year window at one shard and about 8.5 months at
sixteen; it scales down as shard count rises (ADR-0044). Note the
limit is on the query's *start*: a narrow `start`/`end` pair costs little
however recent it is, so the fix is always to move `start` forward, never to
change `end`.

## SQL over `samples`, `logs`, and `spans`

`POST /api/v1/sql` (see README "SQL") serves three tables from one endpoint:
`samples` (metrics), `logs`, and `spans`. The server parses the query's `FROM`
clause before it plans, and registers only that one table for the query. A
single query may reference exactly one of the three; naming two or more of
them crosses signals and is rejected with an HTTP 400, before any catalog
listing. The request body, auth, window
(`start`/`end`), and `min_commit_token` handling are identical to the `samples`
case.

The `logs` table columns are `ts`, `observed_ts` (both `Timestamp(ns)`),
`severity_num`, `severity_text`, `body`, `trace_id`, `span_id`, `flags`, and an
`attrs` `Map(Utf8, Utf8)` that merges each record's resource, scope, and
per-record attributes (see docs/query-engine.md for the full schema and
semantics).

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

A word or phrase content search with `has_word(body, 'literal')`. It pushes
down to the RLOG bloom-accelerated scan and matches whole tokens (so `timeout`
matches `connection timeout` but not `timed out`):

```sh
curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT ts, body FROM logs WHERE has_word(body, '\''timeout'\'') ORDER BY ts"}'
```

A filter by an attribute value, with the `attrs['k']` subscript:

```sh
curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT ts, body FROM logs WHERE attrs['\''service.name'\''] = '\''api'\'' ORDER BY ts"}'
```

The `attrs` column merges three sources of attributes into one map. The
sources are the resource, the scope, and the log record. If more than one
source sets the same key, the value from the record wins.

A key that no record carries returns zero rows. It is not an error.

Attribute equality does not prune which objects Ravel reads. The engine
reads the objects that the `ts` range selects, then applies the attribute
filter to the decoded records. A filter on `ts`, or a `has_word` content
search, does prune the read.

Log rows are at-least-once, and a `SELECT` (or `COUNT(*)`) reflects that. A
client retry after a lost ack re-ingests the batch, and unlike metrics there
is no query-time dedup for logs, so the retried rows are returned as extra
rows. A `COUNT` over logs is therefore a lower-bounded count, not an exact
one, for any window a retry may have touched. The `x-ravel-idempotency-key`
suppresses this for keyed sequential retries; unkeyed ingest gets plain
at-least-once. See
[consistency-model.md](../consistency-model.md#duplicates-and-idempotency)
for the full contract. The same applies to spans, which are queryable through
the `spans` table on the same endpoint: span rows are at-least-once with no
query-time dedup either, so a `COUNT` over `spans` is a lower-bounded count on
the same terms.

## HTTP status codes

| Status | `errorType` | When |
|---|---|---|
| 200 | n/a | Success. |
| 400 | `bad_data` | Bad or missing parameter, PromQL parse error, invalid time range, step <= 0. |
| 401 | `unauthorized` | Tenant authentication failed or was not provided. |
| 422 | `execution` | PromQL construct outside the Phase 1 subset, or a query budget (segments/series/samples, or the catalog-list window ceiling) exceeded. |
| 503 | `unavailable` | Catalog or segment fetch failed, an unresolvable `min_commit_token`, or a snapshot invalidated by concurrent GC/compaction. |
| 504 | `timeout` | Query exceeded its deadline. |
