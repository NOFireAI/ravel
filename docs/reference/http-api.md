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
neither the health routes nor `/metrics`.

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
  the catalog: `accounting`, `estimate`, `io`, `phases`, `pruning` and
  `timings`. A `ROW` statement reads no object, and its `stats` holds only
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

## Alert rules route

Served in `all` and `query` modes: the modes that build a query engine, and
therefore the modes that an alert evaluator runs in. A `gateway` or
`maintain` process serves 404 here.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | `/api/v1/rules` | none | Prometheus rules JSON envelope: `data.groups` | 200, 401 | Yes | `all`, `query` | none |

`/api/v1/rules` returns the alert rules that this process loaded, for the
authenticated tenant, in the shape that Prometheus's rules API uses.

- It is a read of configuration. It issues no object-store request and runs
  no query.
- Its two status codes are the whole set: 200 for a resolved credential and
  401 for one that does not resolve.
- There is no request parameter. The tenant comes from the credential and
  nothing else.
- A tenant with no loaded rules gets
  `{"status": "success", "data": {"groups": []}}`, never the rules of
  another tenant.

A tenant's rules render as one group named `ravel-alert-rules`. The Ravel
rules document is one flat `rules` array with no group blocks. So the name is
a constant and the group's `file` is the empty string. The group's `interval`
is the evaluation interval (`--alert-eval-interval-secs`) in seconds.

Each rule carries `type` (always `alerting`), `name` (the `rule_id`), `query`,
`duration` (the `for` delay in seconds, `0` when unset), `labels`,
`annotations`, `health`, and `state`.

`query` is the rule's whole firing expression:

- For a PromQL rule, it is the query text with its threshold comparison
  appended (`max by (instance) (cpu_usage) > 0.9`).
- For a SQL detection rule, it is the statement alone. Its condition is that
  the statement returned a row, and there is nothing to append.

`health` and `state` are both `unknown` for every rule. The endpoint serves
the loaded rule set and does not read evaluation outcomes. So it reports
neither a health nor a firing state.

- `unknown` is Prometheus's own value for `health`.
- For `state`, `unknown` is outside Prometheus's three values (`inactive`,
  `pending`, `firing`). `inactive` asserts that a rule is not firing, and
  this route never checks that.
- A rule carries no `alerts` array of currently-active alerts.

Alert state is queryable today through the `alerts` SQL table.

## Health and metrics routes

Unauthenticated, and served in every mode, including maintain mode.

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
- `/api/v1/alerts`, Prometheus's list of currently-active alerts, is not served.
  A request for it gets a 404 from the HTTP framework, in every mode. Only
  `/api/v1/rules` is served today. Query the `alerts` table for alert state.
