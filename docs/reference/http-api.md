# Ravel HTTP API reference

Every HTTP route that `ravel-server` registers. Each route lists its method,
what it accepts, what it returns, the status codes, whether a tenant
credential is required, which process modes serve it, and its cargo feature
gate where it has one.

Related pages:

- [Concepts](../concepts.md) defines the vocabulary used below (tenant,
  signal, segment, commit token, fold, snapshot).
- [The consistency model](../consistency-model.md) holds the normative
  guarantees for acknowledgement and visibility.
- [The server flag reference](ravel-server-flags.md) lists the server flags
  that change any behavior described here.

## Listeners and authentication

`ravel-server` serves HTTP on one listener (`--listen-http`, default
`127.0.0.1:4318`). OTLP ingest for metrics, logs, and traces is also served
over gRPC on a second listener (`--listen-grpc`). This page documents the
HTTP surface only.

A tenant-scoped route resolves the request to a tenant before it does any
work. The default resolver is a static bearer-token map (`--tenant-token` or
`--tenant-token-file`). A deployment can instead resolve the tenant from an
OIDC token or a proxy-forwarded mTLS identity. With every resolver, a request
to a tenant-scoped route that carries no resolvable credential is rejected
with 401 before any object-store access. A 401 therefore guarantees that
nothing was written or read.

The health and `/metrics` routes carry no tenant identity and require no
credential. They are served in every mode, including maintain mode, whose
router is otherwise empty.

Where a dedicated mutual-TLS listener is configured (`--mtls-listener`), it
serves the same ingest and query routes with the mTLS resolver. It serves
none of the four probe routes (`/healthz`, `/readyz`, `/-/healthy`,
`/-/ready`) nor `/metrics`. The one exception is
`GET /api/v1/agents/health`, which `--agents` mounts on both listeners; it
is unauthenticated there too and reports operator settings only. The other
`/api/v1/agents` routes are mounted on both listeners as well, each with
that listener's resolver (see [Agents routes](#agents-routes)).

## Ingest routes

Served in `all` and `gateway` modes.

- Every ingest route is strict-acknowledgement by default: a 2xx means that
  the data object and its commit record are durably stored.
- On a strict acknowledgement, the OTLP responses carry an
  `x-ravel-commit-token` header: a comma-separated token per shard that the
  request flushed through. A later query replays the tokens as
  `min_commit_token` for read-your-write.
- Buffered acknowledgement is opt-in per request, with the
  `x-ravel-ingest-mode: buffered` header, on the OTLP routes only.
- The same header picks a strict write's flush policy on the OTLP routes:
  `strict-arrival` opens the flush on arrival, `strict-aged` waits for the
  age threshold or the size trigger. `strict`, an absent header, and any other value use the
  server's `--strict-flush-policy` (default `aged`). Both policies
  acknowledge on the same terms.
- The request body limit is 16 MiB on the wire and 64 MiB after
  decompression, on every ingest route.
- OTLP HTTP accepts an absent, identity, or single `gzip` (`x-gzip`)
  `Content-Encoding`. A chained or unknown encoding is 415.
- Remote Write requires Snappy compression. It is strict-acknowledgement
  only, ignores a buffered-mode header, and always uses the
  `--strict-flush-policy` flush policy.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POST | `/v1/metrics` | OTLP `ExportMetricsServiceRequest` protobuf, gzip optional | OTLP `ExportMetricsServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/v1/logs` | OTLP `ExportLogsServiceRequest` protobuf, gzip optional | OTLP `ExportLogsServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/v1/traces` | OTLP `ExportTraceServiceRequest` protobuf, gzip optional | OTLP `ExportTraceServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/api/v1/write` | Prometheus Remote Write 1.0 or 2.0 protobuf, Snappy-compressed | Empty body; the `x-prometheus-remote-write-*-written` count headers | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |

- A 429 carries a `Retry-After` header and sheds the request without
  buffering it.
- Remote Write returns 415 when the content-type or version header names
  neither 1.0 nor 2.0.
- An active-series cap breach on Remote Write reduces the written count
  inside a 200. On Remote Write, 429 is reserved for rate limits.

## Query routes

Served in `all` and `query` modes. Every query route resolves one immutable
snapshot and answers from it. Commits, compactions, and deletions that land
mid-query cannot change the answer. A caller that holds commit tokens passes
them back as repeated `min_commit_token` values for read-your-write.

The Prometheus-shaped routes take form-encoded parameters: on GET, the query
string, and on POST, the body. `/api/v1/sql`, `/api/v1/esql` and
`/api/v1/analytics` take a JSON body.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET, POST | `/api/v1/query` | `query`, `time`, `timeout`, `min_commit_token` | Prometheus instant-query JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET, POST | `/api/v1/query_range` | `query`, `start`, `end`, `step`, `timeout`, `min_commit_token` | Prometheus range-query JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/labels` | `start`, `end`, `match[]` | Prometheus label-names JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/label/{name}/values` | path `name`, plus `start`, `end`, `match[]` | Prometheus label-values JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET, POST | `/api/v1/series` | `match[]`, `start`, `end` | Prometheus series JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/status/buildinfo` | none | Prometheus build-info JSON; Ravel's own version, empty `goVersion` | 200 | No | `all`, `query` | none |
| GET | `/api/v1/metadata` | `metric`, `limit` | Prometheus metric-metadata JSON; an empty object when no tenant resolves | 200 | Optional | `all`, `query` | none |
| GET, POST | `/api/v1/query_exemplars` | `query`, `start`, `end` | Prometheus exemplars JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| POST | `/api/v1/analytics` | JSON: `query`, `start`, `end`, `step`, `op`, optional `timeout`, `min_commit_token`, `allow_partial` | JSON analytics envelope, one entry per series | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| POST | `/api/v1/sql` | JSON: `query`, `start`, `end`, optional `timeout`, `min_commit_token` (each `<token>` or `<table>:<token>`; qualified is required on a statement reading two or more signal tables) | Arrow IPC stream or JSON, per `Accept` | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | `sql` |
| POST | `/api/v1/esql` | JSON: `query`, optional `start`, `end`, `timeout` | JSON: typed columns, rows, `stats`, and `warnings` when there are any | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | `esql` |
| GET | `/api/traces/{trace_id}` | path `trace_id`, plus optional `start`, `end`, `timeout`, `min_commit_token` (`<token>` or `spans:<token>`) | The `/api/v1/sql` response for every span of the trace: Arrow IPC stream or JSON, per `Accept` | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | `sql` |
| POST | `/mcp` | One MCP JSON-RPC message (revision `2026-07-28` or `2025-11-25`) | JSON or `text/event-stream`, per revision; a tool result carries the result envelope | 200, 400, 401, 403, 405, 413, 500 | Yes | `all`, `query` | `mcp` |
| GET | `/api/v1/agents/health` | none | JSON: `feature`, `mode`, `activation_concurrency`, `poll_concurrency`, `memory_budget_bytes` and the five `quotas` as `--agents-*` set them | 200 | No | `all`, `query`, and only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/create` | JSON: `agent` (`kind`, `key`) | JSON: `agent_id`, `incarnation`, `created` | 200, 401, 403, 404, 413, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/send` | JSON: `agent`, `handler`, `request_id`, `payload` | JSON: `operation`, `accept_seq`, `duplicate` | 200, 401, 403, 404, 409, 413, 422, 429, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/call` | JSON: the `send` body plus `wait_ms` | JSON: `operation`, `status`, `result` (null until terminal) | 200, 401, 403, 404, 409, 413, 422, 429, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/operation` | `operation` | JSON: `operation`, `status`, `accept_seq`, `blocked_effect` | 200, 401, 403, 404, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/result` | `operation`, optional `wait_ms` | JSON: `operation`, `result` (null until terminal) | 200, 401, 403, 404, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/resolve` | JSON: `agent`, `handler`, `request_id` | JSON: `operation` and `status`, both null when no receipt matches | 200, 401, 403, 404, 413, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/cancel` | JSON: `operation` | JSON: `operation`, `status`, `requested` | 200, 401, 403, 404, 413, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/state` | `kind`, `key` | JSON: `agent_id`, `incarnation`, `state_version`, `content_type`, `state`, `active_turn` (absent when no turn is open), `mailbox` (each `accept_seq`, `handler`, `source`, `accepted_unix_ns`, `failed_attempts`; absent when empty), `usage` (`receipts` and `max_receipts`, the sealed pages `results_pages`, `events_pages` and `timers_pages` and `max_index_pages`, `head_bytes` and `max_head_bytes`; `agents_timers_e2e::a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`) | 200, 401, 403, 404, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/events` | JSON: `agent`, `cursor` (null for the start), `limit`, optional `operation` | JSON: `events`, `next_cursor`, `terminal` (only on the page that drains a finished `operation`) | 200, 401, 403, 404, 410, 413, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/timers` | JSON: `agent`, `timer_key`, `due_unix_ns` (decimal string), `handler`, `payload` | JSON: `timer`, `generation` | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/timers/cancel` | JSON: `agent`, `timer_key` | JSON: `outcome` (`cancelled`, `already_fired`, `unknown`) | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/timers` | `kind`, `key` | JSON: `timers` (each `timer`, `timer_key`, `generation`, `due_unix_ns`, `handler`, `status`) | 200, 401, 403, 404, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/inspector` | none | `text/html; charset=utf-8`: the embedded inspector page | 200, 401, 403, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/register` | JSON: `handlers` (each `handler`, `version`, `digest`) | JSON: `worker_id` | 200, 401, 403, 409, 413, 422, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/poll` | JSON: `worker_id`, `handlers`, `wait_ms` | JSON: `assignment` (`lease`, `agent`, `operation`, `accept_seq`, `handler`, `state`, `payload`, `steps`), null when nothing is due | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/commit` | JSON: `lease` as polled, `outcome` (`completed` with `state`, `output`, `events`, `timers`, `intents`; or `failed` with `error`, `terminal`) | JSON: `operation`, `status`, `state_version`, `rebased`, `effects` | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/heartbeat` | JSON: `lease` as polled | JSON: `lease_expires_unix_ns` | 200, 401, 403, 404, 409, 413, 422, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/step` | JSON: `lease` as polled, `step_key`, `tool_name`, `tool_version`, `policy` (`provider_idempotency`, `at_least_once`, `manual`), `input_hash` (64 lowercase hex) | JSON: `step` (a `steps` entry), `effect`, `attempt`, `failure` (null unless the recorded result is a tool failure) | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/effects/complete` | JSON: `effect`, `attempt`, `result` | JSON: `effect`, `status`, `late` | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/worker/effects/fail` | JSON: `effect`, `attempt`, `error` | JSON: `effect`, `status`, `late` | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_worker` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/effects` | `kind`, `key` | JSON: `effects` (each `effect`, `turn_id`, `tool_name`, `tool_version`, `policy`, `status`, `attempt`) | 200, 401, 403, 404, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| GET | `/api/v1/agents/effects/{effect}` | none | JSON: the `effects` entry plus `step_key`, `result_attempt`, `result`, `failure` (present only when the result is a tool failure), `attempts` (each `attempt`, `status`), `evidence` (each `attempt`, `result`, and `failure` when the report was one) | 200, 401, 403, 404, 422, 503 | Yes, `agents` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/reconcile` | JSON: `effect`, `attempt`, `decision` (`record_result` with `result`, or `retry`) | JSON: `effect`, `status`, `attempt` | 200, 401, 403, 404, 409, 413, 422, 500, 503 | Yes, `agents_admin` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/sweep` | none | JSON: `wake_list_pages`, `dir_list_pages`, `head_gets`, `hints_read`, `hints_deleted`, `timers_fired`, `agents_loaded`, `dir_entries_repaired` | 200, 401, 403, 500, 503 | Yes, `agents_admin` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |
| POST | `/api/v1/agents/destroy` | JSON: `agent` (`kind`, `key`), optional `incarnation` | JSON: `agent_id`, `incarnation`, `destroyed`, `abandoned` (`inputs`, `active_turn`, `effects_in_doubt`, `effects_authorized`, `timers_scheduled`) | 200, 401, 403, 404, 413, 422, 500, 503 | Yes, `agents_admin` | `all`, `query`, only with `--agents`; also on the mTLS listener | `agents` |

### `/api/v1/status/buildinfo` and `/api/v1/metadata`

Both routes exist for Prometheus-shaped clients. Grafana's datasource test
probes both. `/api/v1/metadata` never returns 401. A request with no
resolvable tenant receives an empty object, and reads no object storage on
that path.

### `/api/v1/analytics`

The `op` type field selects one of two operations: change point detection
(`change_point`) and summary statistics (`summary`).

| Condition | Status |
| --- | --- |
| An unknown `op`, a missing field, or a malformed body | 400 |
| An analytics computation error, or the per-call series cap | 422 |
| Partial federated coverage without `allow_partial: true` | 503 |

`/api/v1/analytics` and `/api/v1/query_exemplars` take a permit from the same
query concurrency ceiling as the other query routes. A request that the
ceiling refuses gets the same 503 body that those routes return.

### `/api/v1/sql`

`/api/v1/sql` is behind the `sql` cargo feature. The published server image
builds that feature, so the route is available there.

The SQL surface registers five tables, one per signal: `samples` (metrics),
`logs`, `spans` (traces), `alerts` (alert state transitions), and `audit`
(audit records, including the query-audit trail).

A statement may name several of those five and the tenant's Parquet tables,
for example a join of `samples` and `logs`. Each signal table is read from
its own snapshot, with no order between them, and the join runs on the
receiving server under a memory ceiling that is best-effort for join
operators. In a statement reading two or more tables, a relation alias,
derived-table alias or CTE name equal to one of its other tables' names or
to any of the five signal table names is 400 before any catalog listing. An
alias equal to its own table's name (`FROM logs AS logs`) is admitted, and a
one-table statement is not checked. See
[the query guide](../guides/query.md#sql-queries) for the terms.

Request limits:

- The request body is capped at 64 KiB. A larger body is 400.
- A statement that carries more than 1,000 tokens is 400. The count covers
  tokens outside string literals and comments. The message names the measured
  count and the maximum.
- The token gate runs before the statement is parsed. It bounds the depth of
  the expression tree that a statement can build, so it constrains structure
  and not text length.
- A string literal costs one token, whatever the length of its payload. An
  identifier or a number costs one token, whatever its length. A comment
  costs nothing.
- The gate applies to Flight SQL too, where it surfaces as `InvalidArgument`.

See docs/query-engine.md, "SQL statement complexity gate".

`min_commit_token` is a list. Each entry is a commit token, or a token
qualified with the table it bounds, `<table>:<token>` (split once at the first
colon). Each qualified token bounds only that table's snapshot. Each of these
is 400 `bad_data` before any catalog or object-store request:

- An unqualified token on a statement that reads two or more of `samples`,
  `logs`, `spans`, `alerts` and `audit`. The message names those tables in
  that order: `qualify min_commit_token with one of samples, logs, ...`.
- A qualifier that is not, exactly and in lower case, the name of a signal
  table the statement reads. The message quotes the qualifier, cut to 64
  bytes.
- Any token on a statement that reads only Parquet tables.
- An empty qualifier, or a part after the colon that is not a token
  (`invalid min_commit_token`).

See [qualifying a token by table](../guides/query.md#qualifying-a-token-by-table-sql).

The JSON response envelope has one array per row under `data.rows`:

```json
{"status": "success",
 "data": {"columns": [{"name": "ts", "type": "..."}], "rows": [[...]]},
 "stats": {}}
```

`stats` holds `accounting`, `estimate`, `phases`, `signals`, `io`,
`timings` and `pruning`, and `cache` when the fetcher cache is on (see
[per-statement cache counters](../guides/caching.md#per-statement-cache-counters)).
`signals` has one entry per signal table the
statement reads, in the order `samples`, `logs`, `spans`, `alerts`,
`audit`, each with exactly `table`, `segments`, `segmentsPruned` and
`phases` (the table's own cost, in the shape of `stats.phases`). Summed
over the entries, each figure of `phases` equals the statement's
`stats.phases` figure, less what Parquet tables cost; a Parquet table has no
entry. The
[query engine reference](../query-engine.md#json-response-shape) defines
every section.

JSON encoding per column type:

- A non-finite float is a string: `NaN`, `+Inf`, and `-Inf`.
- Every integer width is a JSON number.
- `Float16` is widened to a 64-bit float and follows the float rule above,
  including the `NaN`, `+Inf` and `-Inf` strings.
- A timestamp of any unit is an integer count of nanoseconds since the
  epoch, with its time zone dropped. A value past 2^53 needs a client that
  parses JSON integers exactly, which a JavaScript `JSON.parse` does not.
- A `Time32` or `Time64` of any unit is an integer count of nanoseconds
  since midnight.
- A `Duration` of any unit is a signed integer count of nanoseconds.
- `Date32` and `Date64` are `YYYY-MM-DD` strings.
- Every decimal width (`Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`)
  is a string that holds its exact decimal text.
- Binary columns are lowercase hex strings.
- An `Interval` of any unit is an object with all three integer fields
  `months`, `days` and `nanoseconds`, each 0 where the unit has no such
  component. The milliseconds of a day-time interval become nanoseconds. The
  three fields stay apart because none converts exactly into another: a
  month has no fixed number of days. Also, an ISO 8601 duration cannot carry
  a negative component beside a positive one.
- Every list layout (`List`, `LargeList`, `FixedSizeList`, `ListView`,
  `LargeListView`) is a JSON array whose elements follow these same rules.
- A `Dictionary` with any integer key type (`Int8` to `Int64`, `UInt8` to
  `UInt64`) is encoded as the value that its key addresses.

A column whose type or value has no JSON encoding fails the query with 422
`execution`. The cases are:

- a type with no rule above, which is also logged at `warn`
- a timestamp or a duration past the i64 nanosecond range
- a time that is negative, or a whole day or more
- a date outside years 0000 to 9999
- a `Date64` that is not a whole day

The message names the column and its Arrow type and gives the reason, never
the value:
`column "<name>" of type <type> cannot be encoded as JSON: <reason>; request
the Arrow IPC format to read it exactly`.

To get an Arrow IPC stream instead, send
`Accept: application/vnd.apache.arrow.stream`. The stream is bit-exact for
every type and reads every such column.

### `/api/v1/esql`

`/api/v1/esql` runs one ES|QL statement over the `logs` table. It is behind
the `esql` cargo feature, which turns on `sql` too. The published server
image does not build `esql`, so the route is absent there, and a build
without the feature answers 404. A build with it serves the route on the
main HTTP listener and on the mTLS listener, each under its own credential.
The statement runs through the same admission ceiling, deadline, usage
record, query audit record and cost recording as `/api/v1/sql`. See
[ES|QL queries](../guides/query.md#esql-queries) for the language that runs.

Request body, JSON:

| Field | Required | Meaning |
| --- | --- | --- |
| `query` | Yes | The statement text. |
| `start` | No | Unix seconds, a float. The window starts no earlier than this, inclusive. |
| `end` | No | Unix seconds, a float. The window ends before this, exclusive. |
| `timeout` | No | Seconds. It can only lower the server deadline. |

`start` and `end` narrow the window the statement's own `@timestamp`
bounds give it; they never widen it. Any other field is 400. `params` is
refused by name: the route runs no parameterised statement, so write each
value into the statement. The route takes no `min_commit_token`. The body is
capped at 64 KiB.

```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/esql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "FROM logs | WHERE @timestamp >= NOW() - 1 hour | STATS errors = COUNT(*) WHERE log.level == \"ERROR\" BY service.name | SORT service.name"}'
```

The response is always JSON. Here, with the row values illustrative and the
`stats` sections shown empty for brevity:

```json
{"status": "success",
 "data": {"columns": [{"name": "errors", "type": "long"},
                      {"name": "service.name", "type": "keyword"}],
          "rows": [[3, "api"], [2, "web"]]},
 "stats": {"accounting": {}, "estimate": {}, "io": {}, "phases": [],
           "pruning": {}, "timings": {}}}
```

- Each column's `type` is its ES|QL type: `keyword`, `long`, `double`,
  `boolean`, `date_nanos` or `null`.
- A `date_nanos` value is an integer count of nanoseconds since the Unix
  epoch.
- `stats` has the sections `/api/v1/sql` reports for a statement that reads
  the catalog, except `signals` and `cache`: `accounting`, `estimate`, `io`, `phases`,
  `pruning` and `timings`. A `ROW` statement reads no object, and its `stats` holds only
  `accounting` and `estimate`, with no request counted.
- `warnings`, an array of strings, is present only when there is something
  to say. Today that is one case: the statement's own `LIMIT` was above
  10,000 and was lowered to it, as in
  `the statement's LIMIT 20000 was lowered to 10000, the most rows an ES|QL
  query returns`.

Errors use the shared body, `{"status": "error", "errorType": ..., "error":
...}`. The status follows the shared mapping in
[Query status codes](#query-status-codes), with these ES|QL cases:

| Condition | Status | `errorType` |
| --- | --- | --- |
| The statement does not parse, or passes a structural limit; the message starts `line L:C:` and states no byte offset | 400 | `bad_data` |
| The statement parses and does not bind: an unknown column, a type mismatch, an aggregate outside `STATS` | 400 | `bad_data` |
| A command, source, function, operator or type this build does not run; the message starts `line L:C:` and names the construct, as shown below | 400 | `bad_data` |
| `params`, an unknown field, a body that is not JSON, or `start` after `end` | 400 | `bad_data` |
| A bound statement the planner cannot plan; the message does not carry the planner's text | 422 | `execution` |
| A statement whose time window is wider than the catalog serves | 422 | `execution` |
| A failure while the statement runs | As for `/api/v1/sql` | As for `/api/v1/sql` |

The refusal of a construct this build does not run reads, for `FROM metrics`:

```text
line 1:6: source [metrics] is not supported
```

A statement that does not parse is refused before it takes a permit from the
admission ceiling and before any object-store read, and it writes no audit
or usage record.

### `/api/traces/{trace_id}`

`/api/traces/{trace_id}` is behind the `sql` cargo feature. It runs
`SELECT * FROM spans WHERE trace_id = X'<trace_id>' ORDER BY start_ts,
span_id` through the `/api/v1/sql` path, with the same envelope,
authentication, limits, deadline and query audit record.

- `trace_id` is 32 hexadecimal characters in either case, after the path
  segment is percent-decoded once. Anything else is 400 `bad_data`, before
  any object-store read.
- A trace the tenant does not hold is 200 with zero rows.
- Without `start`, the lookup reads every span the tenant retains, not the
  one hour before `end` that `/api/v1/sql` defaults to. An omitted `end` is
  the current time.
- `start` and `end` choose which objects are read and do not filter rows, so
  a span that starts outside them can still be returned.
- `min_commit_token` may repeat. Each value is a token, or a token qualified
  `spans:<token>`, the one table the lookup reads. Any other qualifier is 400
  `bad_data` before any object-store read.

See [fetching one trace by id](../guides/traces.md#fetching-one-trace-by-id).

### `/mcp`

`/mcp` is behind the `mcp` cargo feature and `--mcp`. A build that carries
the feature serves no MCP route until an operator passes the flag. `/mcp` is
the only MCP path, and it serves POST only.

Every request is authenticated with the listener's own tenant resolver
before the JSON-RPC message is parsed.

| Condition | Status |
| --- | --- |
| No resolvable credential, whatever the request asked for, including a legacy `initialize` | 401 |
| An `Origin` outside `--mcp-allowed-origins` | 403 |
| A body past `--mcp-max-body-bytes` | 413 |
| On revision `2026-07-28`, a `Mcp-Method` or `Mcp-Name` header that disagrees with the body | 400 |

### Agents routes

The `/api/v1/agents` routes are behind the
`agents` cargo feature and `--agents`. Both listeners mount them, each with
its own tenant resolver, and the tenant always comes from the credential,
never from the request (`agents_e2e::capabilities_are_enforced_on_both_listeners`).
Client routes need the principal's `agents` capability, worker routes its
`agents_worker` capability, and `reconcile`, `sweep` and `destroy` its
`agents_admin` capability. Every handler admits its capability before
anything else, and a credential without it gets 403 with a body naming the
capability: the server's route table carries each route's capability, and
`agents::tests::every_route_refuses_a_token_lacking_its_capability` sends
every route in it a token holding every capability but that one
(end to end: `agents_e2e::capabilities_are_enforced_on_both_listeners` for
`agents` and `agents_worker`,
`agents_effects_e2e::manual_blocks_until_reconciled_and_audits_the_decision_once`,
`agents_timers_e2e::sweep_route_counts_match_the_store_and_needs_agents_admin`
and
`agents_effects_e2e::destroy_fences_the_lease_holder_audits_once_and_a_recreate_starts_incarnation_two`
for `agents_admin`). The router answers 404 for any path outside the table
above (`agents_e2e::a_path_outside_the_route_table_is_404`).

The router carries its own admission counter, one
lane for the waiting routes (`call`, `result`, `worker/poll`) sized by
`--agents-poll-concurrency` and one for every other route sized by
`--agents-activation-concurrency`. It never takes a query permit, so a full
agents lane leaves query routes serving
(`agents_e2e::admission_refusal_is_503_and_takes_no_query_permit`).

Every refusal after authentication is an `ErrorBody`: `code`, `retry`,
`reason`, and `ids` holding only safe identifiers (agent id, incarnation,
operation, turn, step, effect, timer). Payloads and state never appear in
an error. A `budget_exceeded` or `mailbox_full` refusal of an agent's
lifetime cap adds `cap`, naming it: `receipts`, `index_pages`,
`head_bytes`, `mailbox` or `timers`
(`agents_timers_e2e::a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`
answers a send past the receipt cap 413 with `cap` `receipts`).

| `code` | Status | `retry` |
| --- | --- | --- |
| `agent_gone`, including an unknown operation handle, whose `ids.operation` echoes the handle (`agents_e2e::unknown_operation_handle_is_agent_gone_with_the_handle`) | 404 | `never` |
| `idempotency_conflict`, `ownership_lost`, `incompatible_handler`, `non_deterministic_replay` | 409 | `never` |
| `effect_in_doubt` | 409 | `reconcile` |
| `outcome_unknown`; resolve by `request_id` before resending (`agents_e2e::resolve_after_a_lost_send_ack_returns_the_same_operation`) | 500 | `resolve_operation` |
| `outcome_unknown` from `reconcile`, which mints no operation handle. For `record_result`, send the same decision again: a repeat of a decision that took effect answers the same body. For `retry`, do not resend: read `GET /api/v1/agents/effects/{effect}` and follow "Reconciling an effect in doubt" in the operations guide, since no process holds the attempt and a repeated `retry` is refused (`agents_effects_e2e::a_reconcile_whose_head_write_times_out_answers_the_reconcile_hint`) | 500 | `reconcile` |
| `outcome_unknown` from `destroy`, which mints no operation handle, with `ids.incarnation` the incarnation it read: send the same destroy again naming that `incarnation` (`agents_effects_e2e::a_destroy_whose_head_write_is_lost_answers_backoff_and_a_retry_answers_destroyed_false`) | 500 | `backoff` |
| `mailbox_full` | 429 | `backoff` |
| `budget_exceeded`, including a body past the router's limit (`agents_e2e::send_over_the_body_limit_is_413_with_a_typed_body`) | 413 | `never` |
| `store_unavailable`, including the admission refusal `agents admission limit reached` | 503 | `backoff` |
| `unsupported_format`, including a body or query string that does not parse | 422 | `never` |

No credential is 401 and a missing capability is 403, both in the query
routes' plain error shape.

**Long polls.** `result` and `worker/poll` hold the request until there is
something to return or `wait_ms` elapses, and `call` waits the same way
after accepting. `wait_ms` is capped at 20 s on every one of them; a larger
value waits 20 s. A long poll that ends empty answers 200 with a null
`result` or `assignment`. A waiter answers as soon as the commit lands, not
at the next poll interval
(`agents_e2e::send_then_result_through_real_server_survives_worker_replacement`).

**Event cursor.** `events` returns committed events strictly after
`cursor`, in event sequence order, at most `limit` per page. A `limit`
above 1000 reads 1000, and a `limit` of 0 is 422 `unsupported_format`
(`events limit must be at least 1`; same test as below). The cursor is the decimal committed event sequence; null starts
at the beginning. `next_cursor` is the last sequence returned, or the input
cursor when the page is empty, so resuming from it never repeats or skips
an event (`agents_e2e::events_page_after_the_durable_cursor_without_duplicates`).
A cursor that is not a decimal sequence is 422. With `operation` set (an
operation handle of `agent`), a page holds only the events that
operation's turns committed, and the page that holds its last event once
it is finished carries `terminal`, its final status; no later page holds
an event of it. A cursor below the oldest retained event is 410 with
`code` `cursor_expired`, the `cursor` sent, `oldest_cursor` and `restart`
`restart_from_oldest_cursor`: read again from `oldest_cursor`
(`agents_timers_e2e::events_resume_from_a_cursor_to_terminal_and_expire_below_the_window`,
which also pins the resumed pages' exact sequences and the 422 for a
`limit` of 0). This release never trims events, so the 410 is the
contract for a later one that retains a window.

**Timers.** `timers` arms the agent's timer `timer_key` at `due_unix_ns`,
in nanoseconds since the Unix epoch as a decimal string, and answers the
timer id and its generation. Scheduling the same key again bumps the
generation and supersedes the earlier one, so only the new generation
fires, once
(`agents_timers_e2e::a_reschedule_supersedes_the_earlier_generation`).
`timers/cancel` answers `cancelled` before the firing, `already_fired` once
the firing committed (its input is then in the mailbox, which `GET state`
lists with `source.kind` `timer`), and `unknown` for a key never scheduled
(`agents_timers_e2e::cancel_after_fire_is_already_fired_and_before_fire_is_cancelled`).
`GET timers` lists the latest generation of each key as `scheduled`,
`delivered` or `cancelled`. A due timer fires from the sweep alone; the
`_wake` hint written beside it only speeds that up, and a timer whose hint
write failed is still delivered once, by a later process over the same
store
(`agents_timers_e2e::due_timer_is_delivered_once_across_a_coordinator_restart_with_no_hint`).

**Sweep.** Each tenant's coordinator runs a sweep pass every
`agents.sweep_interval_ms` (5000 by default; 0 runs no periodic pass, and
worker polls and `sweep` still run one), for the tenants this process has
served since it started. `sweep` runs one pass over the caller's tenant on
demand under the same budgets and answers what it did: the `_wake/` and
`_dir/` LIST pages, HEAD reads, hints read and deleted, timers fired, agents
loaded and `_dir` entries repaired. The counts are the requests the store
served for that pass, and the `ravel_agents_sweep_*` families on
`/metrics` move by the same amounts
(`agents_timers_e2e::sweep_route_counts_match_the_store_and_needs_agents_admin`).
The periodic pass needs no poll and no `sweep` call
(`agents_timers_e2e::the_interval_sweep_runs_a_pass_per_interval_with_no_poll`).
An idle worker poll runs a pass at most once per `sweep_interval_ms`, or
once per 5000 ms when it is 0, so idle polls never sweep on every wake
(`agents_timers_e2e::interval_zero_runs_no_periodic_pass_and_polls_sweep_at_the_floor`).
`GET /api/v1/agents/health` reports `sweep_interval_ms`.

**Inspector.** `GET inspector` serves one embedded HTML page with no
external asset and a content security policy that admits no origin but
this server. The route needs `agents` like any other, so a plain browser
navigation, which sends no bearer token, is 401; the page itself asks for
the token and reads `state`, `timers`, `effects`, `effects/{effect}` and
`events` with it. It shows the state version, the active turn, the
mailbox, the timers, the effect-backed steps and the effects with their
status, and for a blocked agent its reason and the action that unblocks
it. Hashes and raw payloads sit under `<details>`, and every payload
preview is labelled "preview"
(`agents_timers_e2e::inspector_is_served_to_agents_tokens_with_every_section`,
`agents::tests::inspector_serves_the_embedded_page_as_html`).

**Workers.** `worker/register` mints a fresh worker id per call, so a
worker process registers once at start and a restarted process gets a new
id; no two processes can share one. A worker polls with its id and handler
pins, and `worker/commit` and `worker/heartbeat` echo the polled `lease`.
The commit is fenced by that lease alone: once another worker has taken the
agent over, the old lease's commit is 409 `ownership_lost` and changes
nothing (`agents_e2e::send_then_result_through_real_server_survives_worker_replacement`).
The polled lease also carries `recorded_unix_ns` and `random_seed`, fixed
when the turn begins, so a replacement worker re-runs the turn with the
same clock and randomness. `call`, `cancel` and `worker/heartbeat` are
covered by `agents_e2e::call_heartbeat_and_cancel_through_real_server`.
Several server processes may serve one tenant: every write is conditional
on HEAD, so the guarantees hold, but a send accepted by a process with no
waiting worker is found by another process's sweep after a delay, and a
reconcile retry is handed over only within the process that answered it
(see docs/guides/operations/agents.md).

**Steps and effects.** A worker journals each tool call with
`worker/step` before running it, under the turn's lease. Steps are
sequential: a second step while one's attempt is still open is 409
`idempotency_conflict`. A step with a recorded result answers it without a
write: `step.status` `completed` with its output, or `failure` set when the
result is a tool failure reported through `worker/effects/fail`. A failure
leaves the step `started` with a null output, in `worker/step` and in a
polled assignment's `steps` alike, so a retried turn replays the failure
rather than an output, and `worker/commit` may then fail the turn
(`agents_effects_e2e::a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail`).
A failure is typed by how `worker/effects/fail` records it, never by the
output's shape: an output of exactly `{"error": "x"}` reported through
`worker/effects/complete` replays as that output
(`agents_effects_e2e::an_output_shaped_like_a_failure_replays_as_the_output`).
Otherwise `attempt` is the attempt the caller may run and `effect` the
handle to report it under. A replay whose `input_hash` differs from the
journaled one is 409 `non_deterministic_replay`, and the operation reads
`blocked`
(`agents_effects_e2e::a_drifted_replay_is_non_deterministic_and_blocks_the_turn`).
A step whose input is being cancelled is 409 `ownership_lost` and no new
attempt is authorized
(`agents_effects_e2e::cancel_during_an_open_attempt_refuses_the_replay_and_the_result_is_late`).

When a worker dies holding an open attempt, the replacement's replay of the
step follows the step's `policy`. `provider_idempotency` hands out the same
attempt again, so whichever result arrives first is the result
(`agents_effects_e2e::provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts`).
`at_least_once` authorizes the next attempt; a result for the earlier one
then answers `late: true`, is kept as evidence and never becomes the result
(`agents_effects_e2e::completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn`).
`manual` latches the effect in doubt: the step is 409 `effect_in_doubt`,
the operation reads `blocked` with `blocked_effect` set, and the turn may
neither continue nor fail until an `agents_admin` caller reconciles it. A
repeat of the same result for an attempt is accepted, and a different one
is 409 `idempotency_conflict`
(`agents_effects_e2e::a_conflicting_final_result_for_the_same_attempt_is_refused`).

`effects` lists an agent's journaled effects and `effects/{effect}`
inspects one: each attempt as `open`, `in_doubt`, `result` or
`superseded`, the result and the attempt it came from, and the late results
kept as evidence
(`agents_effects_e2e::completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn`).
`attempts` lists at most the latest 64 attempts
(`agents_timers_e2e::effect_inspect_lists_at_most_the_latest_64_attempts`).
Evidence is held by the serving process and is not durable; a restart
forgets it, and so does a reconcile that settles the effect out of doubt.
A process names the evidence of at most 4096 effects per tenant; when that
is full, an effect whose result was recorded or whose input finished gives
its slot to a new one, and an effect still in doubt keeps it
(`agents_effects_e2e::settled_effects_give_their_evidence_slot_to_an_effect_in_doubt`).

**Reconcile.** `reconcile` settles an effect in doubt: `record_result`
attaches an operator-verified result, and `retry` authorizes one more
attempt. `attempt` must name the effect's latest attempt, else 409
`idempotency_conflict`; repeating a decision that already took effect
answers the same body and changes nothing. `retry` dispatches nothing: it
answers the authorized attempt's number in `attempt`, and the turn's owner
learns it from its next `worker/step` for the step, which answers that
attempt for the worker to run instead of refusing it as open. Until its
result is reported, a further step is 409 `idempotency_conflict` and a
`retry` naming that attempt is 409 `ownership_lost`, since it is not in
doubt
(`agents_effects_e2e::a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`).
The next `worker/step` for the step gets the attempt whichever worker
holds the turn by then, including one that took the turn over after the
owner's lease expired
(`agents_effects_e2e::a_reconcile_retry_is_handed_to_the_owner_after_a_takeover`).
The hand-over is kept only in the memory of the server process that
answered the reconcile: if that process restarts before the step, or
another server process serves the step, the attempt reads as lost and a
`manual` effect is back in doubt, so reconcile it again. For the same
reason a repeated `retry` is answered as applied only while that process
holds the hand-over unclaimed. A reconcile that answers 500
`outcome_unknown` hands its attempt to no process. The effect's `status`
may still read `authorized` in `GET /api/v1/agents/effects/{effect}`
whether or not another reconcile's process holds the attempt, and the
attempt is dispatched at most once either way
(`concurrent_reconciles_on_two_coordinators_dispatch_the_attempt_at_most_once`).
If the effect is in doubt again after the owner's next step, the attempt
may have run, so resolve it as any effect in doubt; the operations guide's
"Reconciling an effect in doubt" has the procedure
(`a_reconcile_authorization_with_a_lost_ack_is_not_handed_over_and_latches_in_doubt_again`).
A `retry` naming the attempt before one a
worker's own step authorized, such as an `at_least_once` attempt raised
after a takeover, is 409 `ownership_lost` and writes no audit record
(`agents_effects_e2e::a_reconcile_retry_of_an_attempt_a_takeover_raised_is_ownership_lost`).
Before changing an effect in
doubt the server submits one audit record with `kind` `agents_reconcile`
to the audit pipeline, which writes it into the tenant's audit shard,
naming the effect, attempt, decision and the caller's tenant and
capabilities. The record names `agents.principal.subject` and
`agents.principal.subject_source` when the caller's token or claim carries a
subject (a `subject=LABEL` entry on its `--tenant-token` pair, or the claim
`--oidc-subject-claim` names); two admins whose tokens carry none are not
told apart. No response names the subject, and no request header other than
the credential sets it
(`agents_effects_e2e::reconcile_and_destroy_records_name_each_admins_configured_subject_and_a_token_without_one_writes_no_subject_attr`).
Without `--audit-mode` no record is written. Only
`--audit-mode required` refuses on a failed audit write: the reconcile is
then 503 `store_unavailable` and nothing changes
(`agents_effects_e2e::manual_blocks_until_reconciled_and_audits_the_decision_once`).
Under `--audit-mode best-effort` the pipeline reports a failed write as
written, so the reconcile proceeds without its record. A reconcile that
repeats a decision already taken writes no second record
(`agents_effects_e2e::a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`).

**Destroy.** `destroy` tombstones the agent's current incarnation in one
HEAD write and answers what the tombstone abandoned: `inputs` is the
accepted inputs never consumed, the active turn's input among them while a
turn runs; `active_turn` whether a turn was open; `effects_in_doubt` and
`effects_authorized` the effects in each state; `timers_scheduled` the
timers still scheduled. After it, every route that names the agent or a
handle minted under that incarnation answers 404 `agent_gone`: `state`,
`operation`, `result`, `reconcile`, and the lease holder's
`worker/heartbeat`, effect result and `worker/commit`. A `result` parked on
one of the agent's operations ends inside its request with `agent_gone`,
woken by the destroy rather than by its deadline
(`agents_effects_e2e::destroy_fences_the_lease_holder_audits_once_and_a_recreate_starts_incarnation_two`).
Destroying an agent that is already tombstoned answers 200 with
`destroyed` false and the counts of the tombstoned HEAD, writes nothing and
writes no audit record (the same test). An agent that never existed, and an
`incarnation` that is not the current one, are 404 `agent_gone` with
nothing written and nothing audited; the refusal names the pinned
incarnation in `ids.incarnation`
(`agents_effects_e2e::destroy_of_an_unknown_agent_or_a_stale_incarnation_is_agent_gone_and_not_audited`).
`create` on a destroyed agent starts incarnation 2 with an empty mailbox,
receipt list and state; a handle from incarnation 1 stays `agent_gone`, and
a `request_id` used in incarnation 1 is accepted as new (the first test).
Before the tombstone write the server submits one audit record with `kind`
`agents_destroy` to the audit pipeline, naming the agent id, the
incarnation and the `abandoned` counts as it read them
(`audit::tests::the_destroy_event_names_tenant_agent_incarnation_counts_and_caller`).
The `agents_destroy` record names the caller's tenant and capabilities. The
record names `agents.principal.subject` and `agents.principal.subject_source`
when the caller's token or claim carries a subject; two admins whose tokens
carry none are not told apart, as for reconcile. The response's counts come from the HEAD the
tombstone was applied to, so a write landing between the read and the
tombstone shows in the response and not in the record. Without
`--audit-mode` no record is written. Under `--audit-mode required` a failed
audit write refuses the destroy with 503 `store_unavailable` and the agent
stays active in its incarnation
(`agents_effects_e2e::a_destroy_whose_audit_record_is_refused_leaves_the_agent_active`);
under `--audit-mode best-effort` the pipeline reports a failed write as
written, so the destroy proceeds without its record. A destroy whose
tombstone write has an unknown outcome is 500 `outcome_unknown` with retry
`backoff` and the incarnation it read in `ids.incarnation`. Send the same
destroy again with that `incarnation`: if the first write landed, the
retry answers 200 with `destroyed` false, and the one audit record is the
first attempt's
(`agents_effects_e2e::a_destroy_whose_head_write_is_lost_answers_backoff_and_a_retry_answers_destroyed_false`).
Pinning the incarnation keeps the retry from destroying an incarnation a
`create` started in between.

### Query status codes

For the query routes, the status codes come from one shared error mapping.

- 400 `bad_data`: a malformed query, a bad time range, or a non-positive step.
  On `/api/v1/sql` it also covers a `min_commit_token` the statement cannot
  take: unqualified on a statement that reads two or more signal tables,
  qualified with a table the statement does not read, or on a statement that
  reads only Parquet tables (see [`/api/v1/sql`](#apiv1sql)).
- 401 `unauthorized`: no resolvable credential.
- 422 `execution`: one of these.
  - A resource-budget refusal: too many segments, series, samples, or
    scanned bytes, or an over-wide window.
  - An unsupported construct.
  - An SQL result column that the JSON encoding cannot represent. See
    [`/api/v1/sql`](#apiv1sql). Arrow IPC reads it exactly.
- 500 `internal`: a permanent data-integrity fault in already-stored objects.
  It is not retryable. Its message is fixed, so no object key or tenant hash
  leaks. The faults are:
  - A corrupt segment.
  - An unreconstructable or mismatched commit record.
  - A catalog object (commit, compaction or rewrite record, erasure request,
    HEAD, snapshot part or postings) that does one of these:
    - its stored bytes fail to decode at a format version that this build
      covers
    - it carries a format version below the lowest that this build supports
    - it leaves an enum field unset (proto3's default 0) where a value is
      required
  - An erasure request or rewrite record that names a signal this build does
    not know.
  - A provisioning record that fails to decode, is misfiled, carries a
    corrupt shard-generation or format-floor history, or carries a format
    version below the lowest that this build supports.
  - A supersession chain of compaction or rewrite records that is cyclic, is
    deeper than the resolver's fixed bound, or names a predecessor with a
    different input set.
  - A non-monotonic run.
- 503 `unavailable`: retryable. The causes are:
  - A transient storage fault.
  - An invalidated snapshot.
  - A catalog object (commit, compaction or rewrite record, erasure request,
    HEAD, snapshot part or postings) written in a format version above the
    highest that this build reads, or one that carries an enum value above
    the highest that it reads (a snapshot part entry level). A peer on a
    newer build can read such an object during a rolling upgrade.
  - A provisioning record written in a format version above the highest that
    this build reads.
  - A Parquet table's manifest or grants record written in a format version
    above the highest that this build reads.
  - An unsatisfiable `min_commit_token`, qualified or not. The message is
    fixed and names no table.
- 504 `timeout`: the query passed its deadline.

These faults answer 500, not 503:

- A store fault that is a checksum mismatch. This applies on a catalog read,
  on a segment fetch, and on a SQL read of a Parquet table's manifest, grants
  record or data file.
- A Parquet table's manifest or grants record written in a format version
  below the lowest that this build supports.

These faults answer no error:

- A column-statistics object that this build cannot decode. The query reads
  the data instead.
- A decode job for a catalog snapshot part, postings or column-statistics
  object that panics, or that the read CPU gate cancels at shutdown. The
  query still answers exactly, by a slower path.

## Maintenance route

Served in `all` and `query` modes, alongside the query surface, because it
shares the same catalog and folder identity as the scheduled fold.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POST | `/api/v1/admin/fold` | JSON: `signal`, optional `tenant` | JSON naming the fold outcome | 200, 400, 401, 403, 503 | Yes | `all`, `query` | none |

`/api/v1/admin/fold` triggers a catalog fold for one tenant and one signal.
It is the on-demand form of the background fold. The same tenant credential
that the query routes require authorizes it. When the body names a tenant,
that name must hash to the authenticated tenant, or the request is 403. A
fold reveals no data and destroys none: it rewrites a query-cost index that
the tenant already owns.

The call returns 200 with one of four named outcomes:

- `published`: this call wrote a new snapshot and advanced HEAD.
- `nothing_eligible`: no commit was eligible. An ingest hour seals only after
  the maximum flush lifetime plus a clock-skew allowance plus a fold safety
  margin has elapsed. A fold run right after a load therefore seals nothing.
- `lost_cas`: a concurrent fold won the HEAD compare-and-swap. The catalog is
  fine and the winner's snapshot is published. This call did not publish one.
- `throttled`: the rate gate declined because HEAD was published more
  recently than the fold interval. No listing ran, so this call makes no
  eligibility claim.

A 503 on this route means that the outcome is unknown. Retry the call. A 503
never means that nothing was written.

## Alert rules and alerts routes

Served in `all` and `query` modes: the modes that build a query engine, and
therefore the modes that an alert evaluator runs in. A `gateway` or
`maintain` process serves 404 here.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | `/api/v1/rules` | none | Prometheus rules JSON envelope: `data.groups` | 200, 401 | Yes | `all`, `query` | none |
| GET | `/api/v1/alerts` | none | Prometheus alerts JSON envelope: `data.alerts` | 200, 401, 503 | Yes | `all`, `query` | none |

Both routes read a snapshot that the alert evaluator in the same process
publishes at the end of each tick. They issue no object-store request and run
no query. They serve current state, not history, and there is no
cross-process read: a process that runs no evaluator does not ask one that
does. Past transitions are in the `alerts` SQL table.

`/api/v1/rules` returns the alert rules that this process loaded, for the
authenticated tenant, in the shape that Prometheus's rules API uses.

- Its two status codes are the whole set: 200 for a resolved credential and
  401 for one that does not resolve.
- There is no request parameter. The tenant comes from the credential and
  nothing else.
- A tenant with no loaded rules gets
  `{"status": "success", "data": {"groups": []}}`, never the rules of
  another tenant, from every process, including one that runs no evaluator.

A tenant's rules render as one group named `ravel-alert-rules`. The Ravel
rules document is one flat `rules` array with no group blocks. So the name is
a constant and the group's `file` is the empty string. The group's `interval`
is the evaluation interval (`--alert-eval-interval-secs`) in seconds.

Each rule carries `type` (always `alerting`), `name` (the `rule_id`), `query`,
`duration` (the `for` delay in seconds, `0` when unset), `labels`,
`annotations`, `health`, and `state`. After a tick it also carries
`lastEvaluation` when `health` is `ok` or `err`, `lastError` when `health` is
`err`, and an `alerts` array when the tick read the tenant's alert history.

`query` is the rule's whole firing expression:

- For a PromQL rule, it is the query text with its threshold comparison
  appended (`max by (instance) (cpu_usage) > 0.9`).
- For a SQL detection rule, it is the statement alone. Its condition is that
  the statement returned a row, and there is nothing to append.

`health` and `state` come from the last tick of this process's evaluator for
the tenant:

- `health` is `ok` when the tick ran the rule, and `err` with `lastError`
  when the rule's query, condition or write failed, or when the tick could
  not read alert history or take the tenant's lease. `lastEvaluation` is the
  tick's clock reading, in RFC 3339.
- `lastError` for a failed query is the message the PromQL or SQL query
  endpoint returns for the same failure: a PromQL parse error keeps its own
  text, and a storage fault reads as a fixed message such as `upstream
  storage temporarily unavailable`. A history read failure reads `alert
  history unreadable`, a lease failure `alert lease unavailable`, and a
  failed record write `alert record write failed`. No `lastError` carries an
  object key, the tenant hash or the storage backend's own error text; the
  full error goes to the server log.
- `state` is `firing` when any of the rule's alerts is firing, else
  `pending` when any is pending, else `inactive`. The `alerts` array lists
  those alerts in the `/api/v1/alerts` shape.
- When the last tick could not read the tenant's alert history, the first
  tick included, `state` is `unknown` and there is no `alerts` array, while
  `health` is `err` with `lastError` `alert history unreadable`.
- Before this process has finished a tick for the tenant, and in a process
  that runs no evaluator, `health` and `state` are both `unknown`, with no
  `lastEvaluation` and no `alerts` array. The same holds when the tenant's
  last snapshot is stale (see the `/api/v1/alerts` status codes below).
- On a replica whose peer held the tenant's lease on the last tick, `health`
  is `unknown` with no `lastEvaluation`, and `state` and `alerts` come from
  the alert history that tick read.
- `unknown` is Prometheus's own value for `health`. For `state`, `unknown` is
  outside Prometheus's three values (`inactive`, `pending`, `firing`).
  `inactive` asserts that a rule is not firing, and nothing has checked.

`/api/v1/alerts` returns the authenticated tenant's `pending` and `firing`
alerts as of the last tick, in the shape that Prometheus's alerts API uses.
Suppressed and resolved alerts are not listed. Each alert carries `labels`
(with `alertname` set as the Alertmanager sink sets it), `annotations`,
`state`, `activeAt` and `value`.

- `activeAt` is the transition into the current state, in RFC 3339. For a
  firing alert that was pending first, it is when the alert fired, where
  Prometheus keeps the time it went pending.
- `value` is a string: the sample the alert's series matched with on the
  last tick, or for a SQL detection rule the number of rows its statement
  returned. It is present only when this process held the tenant's lease on
  that tick and ran the alert's rule without error.

Status codes:

- 200 with the tenant's alerts. A tenant with no loaded rules always gets
  `{"status": "success", "data": {"alerts": []}}`, never the alerts of
  another tenant, from every process, whatever other tenants have configured
  and whether this process runs an evaluator.
- 401 for a credential that does not resolve.
- 503 with `errorType` `unavailable`, for a tenant with loaded rules only,
  when this process runs no evaluator (evaluation disabled), before this
  process has finished a tick for the tenant, and whenever the last tick could not read
  the tenant's alert history, until a later tick reads it. An empty list
  would assert that nothing is firing, and an earlier tick's list may no
  longer be current.
- The same 503 when the tenant's last snapshot is stale: older than three
  evaluation intervals, measured from the evaluator's clock reading when it
  published the snapshot at the end of its tick, so a slow tick still
  publishes a snapshot that is served. The evaluator is the only writer of
  the snapshot, so a stopped or stuck evaluator would otherwise leave it
  served unchanged. The `error` starts with `this tenant's alert evaluator in
  this process has stopped publishing its state` and names the snapshot's age
  and the bound.
  Serving resumes when the evaluator publishes again.

## Health and metrics routes

Unauthenticated, and served in every mode, including maintain mode. The
agents health route in the query table above is also unauthenticated but is
mode- and feature-gated and needs `--agents`; it does not inherit this
section's rule.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | `/healthz` | none | `ok` | 200 | No | all modes | none |
| GET | `/readyz` | none | empty body | 200, 503 | No | all modes | none |
| GET | `/-/healthy` | none | `ok` | 200 | No | all modes | none |
| GET | `/-/ready` | none | empty body | 200, 503 | No | all modes | none |
| GET | `/metrics` | none | Prometheus text exposition | 200 | No | all modes | none |

### Liveness

`/healthz` (and its Prometheus spelling `/-/healthy`) is liveness. A request
that reaches the handler proves that the server task can route. The route is
200 whenever it answers, and a store outage never makes it fail.

### Readiness

`/readyz` (and `/-/ready`) is readiness, the AND of four conditions:

- Startup has completed: config parsed, the object-store capability gate
  passed, listeners bound.
- The process is not draining.
- The store is reachable.
- No ingest shard actor has been condemned.

Only the store condition recovers on its own. The startup latch and the
drain latch are one-way. A condemned ingest shard cannot recover in-process,
so a 503 from that cause persists until the process is rolled.

When a shard actor is condemned depends on the signal:

- The metrics pipeline respawns a dead shard actor. It condemns only on the
  death that exhausts its respawn budget.
- The logs and spans pipelines never respawn, so their first shard-actor
  death condemns.

See the
[observability guide](../guides/observability.md#ingest-pipelines-ravel_ingest_).

The readiness route issues no object-store request and takes no lock. Each
condition is an atomic load, including the ingest one, which reads the
condemned-shard counter. A background store probe with hysteresis supplies
the store atomic. Four consecutive failed probes flip readiness to 503, and a
single success recovers it.

Readiness never restarts the process. A 503 sheds traffic: Kubernetes
removes the pod from its Service endpoints. Liveness is the separate
`/healthz` route for that reason.

### The health listener

The same four probe routes, and no others, are also served on the optional
`--listen-health` listener. It runs on its own thread and answers even when
every main-runtime worker is busy. On that listener:

- `/healthz` and `/-/healthy` also return 503 once the main runtime's
  heartbeat is older than 60 s.
- `/readyz` and `/-/ready` also return 503 once it is older than 30 s.

### Metrics

`/metrics` is the Prometheus scrape endpoint. It is unauthenticated, so
per-tenant labels on the admission and query families are opt-in
(`--metrics-tenant-labels`). By default every tenant folds into a single
`tenant_hash="other"` series.

## Not on this HTTP surface

- Flight SQL is behind the `flight-sql` cargo feature and is a gRPC service on
  the gRPC listener, not an HTTP route. The published image builds that feature.
- OTAP (OpenTelemetry Arrow Protocol) metrics ingest is a gRPC service that
  needs both the `otap` cargo feature and the `--otap` runtime flag. The
  published image builds the feature, and the flag is still required.
- OTLP ingest for metrics, logs, and traces is also available over gRPC on
  the gRPC listener.
