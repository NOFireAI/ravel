# MCP server for AI agents

This page covers the MCP server, the read surface an AI agent host uses to
query Ravel. It is distinct from Ravel Agents, the durable agent runtime,
which has its own guide: [Ravel Agents](agents.md).

> The tools ship behind the `mcp` cargo feature and the `--mcp` flag, both
> off by default. Build `ravel-server` with `--features mcp`, then run it
> with `--mcp` (only supported under `--mode all` or `--mode query`) to
> mount `POST /mcp` on the query router; `--mcp-allowed-origins` is required
> once that listener is reachable from anywhere but localhost. All nine tools
> are served: `ravel_capabilities`, `ravel_describe_data`,
> `ravel_find_labels`, `ravel_explain_query`, `ravel_query_sql`,
> `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace` and
> `ravel_analyze_timeseries`. The [MCP reference](../reference/mcp.md)
> describes each one.

The published image built from `main` compiles in the `mcp` feature, so with
that image you only pass `--mcp`. On Kubernetes, query pods the operator
manages serve `POST /mcp` when the `RavelCluster` sets
`spec.query.mcp.enabled: true` with at least one `spec.query.mcp.allowedOrigins`
entry; the operator then renders `--mcp` and `--mcp-allowed-origins` on the
query pods only. The query pods listen on `0.0.0.0`, so the API server refuses
`enabled: true` without an origin, as the server would refuse to start. A
non-browser MCP client sends no `Origin` header and is accepted, so the
allowlist governs browser clients only. A custom `spec.image` built without
the `mcp` feature ignores `--mcp` and mounts no route. Turning
`spec.query.mcp` on or off, or editing it while it is enabled, changes the
query Deployment's arguments and rolls the query pods. See
[Serving MCP from the query pods](kubernetes.md#serving-mcp-from-the-query-pods).

## What agents get

The Model Context Protocol (MCP) surface gives an agent host four things over
one connection. An agent that investigates an incident needs each of them:

- a way to find what data exists without guessing
- a way to run bounded queries with a known cost
- a way to move between metrics, logs, and traces on exact identifiers
- a way to hand back a finding that a person can check again

## Connecting

The server will expose MCP over Streamable HTTP at `POST /mcp`, on the same
listener as the rest of the query API. It will accept the same bearer
credential that the HTTP query routes use: `Authorization: Bearer <token>`.
There will be no separate agent credential and no second login step.

The bearer credential travels only over TLS:

- A deployment terminates TLS in front of the server or on the mTLS listener.
- Every proxy hop that carries the credential runs TLS.
- The client does not follow a redirect to plain HTTP.

The server will speak two protocol revisions and will answer both on the same
endpoint. Use the revision that your MCP client sends.

| Revision | Behavior |
|---|---|
| `2026-07-28` | Will carry its protocol version and method name in headers on every request. Will need no handshake. |
| `2025-11-25` | The older revision that most deployed clients still speak. It opens with an `initialize` call and keeps a session id. |

Every MCP request will authenticate separately. A cursor or an evidence
reference from an earlier call carries no authority by itself. The server
will always check it against the credential on the current request.

## The nine tools

The tools are grouped by task. Every tool is read-only: the default profile
cannot write data, delete data, or trigger maintenance. The one write the
server offers over MCP, `ravel_agent_send`, belongs to the
[Ravel Agents](#ravel-agents) tools below and is present only when they are.

### Get oriented

- `ravel_capabilities`: the protocol and server version, which tools are
  enabled, the effective budget ceilings, a summary of the SQL and PromQL
  dialects, and which signals (metrics, logs, traces) the tenant has. Reads
  no data.
- `ravel_describe_data`: for one signal, the effective schema (fixed and
  typed attribute columns), which keys are indexed, the metric families with
  their type and unit, the freshness watermark, the coverage window, and
  the committed row count. That count is summed from commit records, so it
  counts rows before duplicate collapse and before any pending erasure: an
  upper bound, not the number of rows a query reads back. Page one, which
  carries the count, reports `accuracy.exact: false` and names the count in
  `accuracy.approximation`; later pages report `exact: true`.
  Logs and traces have no metric families, and the response says why; so
  does a metrics tenant with no metric metadata record. When the metric
  metadata read fails, a metrics call fails with the retryable class
  `unavailable` instead of answering zero families. Retries within 60
  seconds of the failed read fail the same way without reading the store.
  After that, a retry starts a background reread unless one is in flight,
  and still fails; a failed reread lets the next retry start another.
  Calls succeed once a read of the record does.

### Find labels

- `ravel_find_labels`: metric names for a selector, label names, or the
  values of one label under a selector. The list is observed from the data
  over the window that you asked for. The result states whether the list is
  complete for that window.

  The arguments choose the list:

  - `label_name` set: the values of that label, under the selector when
    one is given. `label_name: "__name__"` lists metric names, and that list
    includes the two log pseudo-metrics when there is no selector or the
    selector names one of them.
  - only `selector` set: the label names of the series the selector
    matches.

  A warning on every answer names the list that ran and repeats that it is
  observed from data over the window.

  A call must carry a selector or a label name. The server refuses a call
  that carries neither with `missing_argument`, and names the bounded form
  to ask for. An optional `filter` does not satisfy that bound. A selector
  or a label name narrows what the call resolves. A filter narrows only what
  the call returns.

  `time_range` is required. Each bound is an RFC 3339 timestamp in UTC
  (ending in Z or +00:00) or a decimal nanosecond string, and the window is
  half-open.

### Check a query first

- `ravel_explain_query`: validates a SQL statement, resolves the snapshot
  that it targets, and runs no scan. It returns the target table, the
  effective schema as `data.columns` with zero rows, how many segments it
  admits, the cost estimate beside the effective budget in `budget`, and the
  plan shape in `plan`.

  It explains SQL only. A statement that fails SQL validation returns
  `validation` with the engine's text and a next step naming
  `ravel_query_promql`. A statement that is not a `SELECT` returns
  `unsupported`. A statement that names two tables is refused with
  `invalid_argument` before anything is resolved, and the message names
  the one-table rule.

### Run a query

- `ravel_query_sql`: one read-only `SELECT` over one table, with a required
  `time_range` applied as a row filter, `max_rows`, budgets you can only
  lower, and a cursor for the next page. The statement is checked before
  anything is read: two tables, a write, a statement-level `LIMIT`, and an
  `ORDER BY` the tool cannot page by (a float column, a column that may be
  NULL, one the statement does not project) are refused with a next step.
  A statement ordered by a `count(...)` (`ORDER BY c` over
  `count(*) AS c`) pages only over the segments page one read, since a row
  committed later can change a count for groups already served: a later
  page whose segments changed is `cursor_expired` with a `rerun_page_one`
  next step. A statement with no `ORDER BY` over a table with no row
  identity runs unpaged, and so does one ordered by another aggregate, a
  window function or a subquery (`ORDER BY sum(value)`): a full page of
  either is `ok_bounded` with no cursor and a next step saying how to order
  it to page. An
  `evidence_ref` is refused as unsupported. The
  [reference](../reference/mcp.md#running-a-statement) lists each answer.
- `ravel_query_promql`: an instant or a range PromQL evaluation, with
  partial-coverage consent, step alignment, and the two log pseudo-metrics
  available the same as any other metric name. It answers one row per
  series, the series' timestamps and values as parallel arrays, and
  `max_rows` counts series: a capped result is `ok_bounded` with no cursor.
  A range of more than 11,000 steps is refused as `invalid_argument`, and a
  series whose samples do not fit `max_response_bytes` comes back with a
  prefix of them, counted in `samples.samples_omitted`. A result that
  skipped part of its coverage is refused as `unavailable` unless the call
  sets `allow_partial_coverage: true`, and is then `ok` with
  `accuracy.exact: false`. The
  [reference](../reference/mcp.md#evaluating-promql) lists each answer.

### Search logs

- `ravel_search_logs`: a typed log search compiled to one SQL `SELECT`
  over `logs`. It takes predicates on typed attribute columns and the `trace_id`
  key (`eq`, `ne`, `in`, `exists`), a `has_word` body search, a minimum
  severity, and a trace id, and requires `time_range`. A predicate key must
  be a typed attribute column of the tenant's `logs` table, or `trace_id`
  when it is an indexed key. Any other key is refused before the search
  runs, including an indexed key that is not a typed attribute column, with
  a next step naming `ravel_describe_data`. Call it with signal `logs`: a
  key the search accepts is a `column` row marked `declared: true`, or
  `trace_id` when it has an `indexed_key` row.
  Results order by `(ts, observed_ts)`. Rows with the same attribute key set
  form a group, and the attributes every row of the group shares are listed
  once, in the group's first row's `__ravel_group_attrs`. The
  [reference](../reference/mcp.md#searching-logs) gives the exact shape.

### Trace and time series

- `ravel_get_trace`: every span of one trace id. The time range is
  optional: omitted, the lookup reads every span the tenant retains; given,
  it bounds the read and filters the rows. Each span comes with its depth in
  the span tree (`__ravel_depth`) and whether its parent was returned
  (`__ravel_parent`: `root`, `present`, `missing`, `not_returned` or
  `cycle`). A capped result never calls a parent `missing`: it says
  `not_returned`, because the parent may be among the dropped rows. The
  [reference](../reference/mcp.md#span-tree) gives the exact rules. The
  optional logs pass is not built yet, and a call asking for it is refused
  as unsupported; `ravel_search_logs` with the trace id and a time range
  returns the trace's logs.
- `ravel_analyze_timeseries`: change-point detection or a summary statistic
  over each series of a PromQL range result, one row per series. The result
  names the method as a heuristic (`accuracy.exact` is `false`), and each
  row states the minimum point count that the method needs and whether the
  series was downsampled first. A series with too few points is a row
  marked `too_few_points`, not a failed call. The range rules are
  `ravel_query_promql`'s, and a read that skipped part of its coverage is
  refused; read the partial series through `ravel_query_promql` instead.

## Ravel Agents

A server built with both `--features mcp,agents` and run with both `--mcp`
and `--agents` adds six tools for [Ravel Agents](agents.md) to `tools/list`,
after the nine above. Without either feature or either flag, `tools/list` is
the nine tools exactly. All six are served, and each description starts with
"Ravel Agents:".

- `ravel_agent_describe`: an agent's identity, versions, state size, active
  turn and mailbox depth, without the state.
- `ravel_agent_state`: the state itself, with the active turn and the
  mailbox.
- `ravel_agent_send`: accept one input into an agent's inbox. It is the only
  tool here that writes. Pass an `idempotency_key`: a repeat with the same
  key answers the operation the first call accepted and accepts nothing new.
  If a send reports `outcome_unknown`, or ends at its deadline, send again
  with the same key.
- `ravel_agent_operation`: the status of the operation a send answered.
- `ravel_agent_events`: one page of the agent's events in order. A full page
  carries a cursor; pass it back to read the next page.
- `ravel_agent_blocked_effects`: the effects in doubt that hold a turn open,
  with the same text the agents inspector shows and the two reconcile
  requests an `agents_admin` caller can send. The tool does not reconcile.

Each call goes through the agents client route of the same name inside the
server, with the credential of the MCP request. The principal needs the
`agents` capability; without it every one of the six answers `unauthorized`,
the same refusal the HTTP route gives. A call the route has not answered by
its effective deadline ends with the `deadline` class. The [MCP reference](../reference/mcp.md#ravel-agents-tools)
lists their inputs, bounds and failure classes.

## The result envelope

Every tool call returns one envelope, on success and on failure.

### `status`

One of four values:

- `ok`: the query completed. A query with zero matching rows is `ok`. A
  `LIMIT` that the data did not fill is still `ok`.
- `ok_bounded`: a cap stopped the result and more rows exist, but no cursor
  exists for the rest. Either the statement has no total order or the tool
  mints no cursor. `presentation` states which cap stopped the result. A
  `ravel_find_labels` page cut short by the byte cap reports this status.
- `ok_page`: a cap stopped the result and a cursor exists. `presentation`
  states whether the row cap, the byte cap, or both caused it.
- `error`: the call failed. `failure` names why.

### `failure`

`null` on every status but `error`. On `error`, it names the failure class
and carries a message that is safe to show a person. The
[MCP reference](../reference/mcp.md#failure-classes) lists the classes.

### `data`

`columns`, `rows`, and `row_count`. `row_count` is the number of rows that
the query produced, even when `presentation.bytes_cap_hit` later drops some
of them from `rows` to fit the response cap.

### `scope`

What the server ran: the `signal`, the `table`, the `time_range` that it
used, the predicates that it applied, and the order of the returned rows.
When a result looks wrong, compare this block to what you asked for. The
server can apply a predicate slightly differently than the one you typed.

### `ids`

`query_id` for this call, and `audit_ref`, which points at the audit record
of the call when the deployment writes one.

Every query tool call gets its own `query_id`, a UUID. Each audit record the
call writes carries it as `query.id`, beside `query.language = mcp:<tool>`
and the engine's own language in `query.engine_language`, so the records of
one call can be found by its id and told apart from the same query sent over
HTTP. A call that runs two statements, such as `ravel_search_logs` with
`predicates`, writes two records under the one id. `ravel_capabilities`
reads no data and writes no audit record. `audit_ref` is empty in this
build. See
[Query ids and audit records](../reference/mcp.md#query-ids-and-audit-records).

### `visibility`

The snapshot that this call read:

- `snapshot_id`
- `ingest_watermark_hour`
- `min_commit_tokens_applied`: the commit tokens that the call honored, each
  in the wire form the caller sent; on a SQL call a token qualified by table
  is echoed as `<table>:<token>`
- `pinned`: whether the snapshot of this call stays held for a later paged
  call

`ingest_watermark_hour` is the greatest ingest hour among the segments that
this call resolved, as a decimal unix hour in a string. It is a freshness
bound. It is not the fold watermark of the catalog. The fold is a cost
boundary that a query reads past routinely, and it lags an acknowledged write
by around 2 h 25 m. A call that resolved no segments leaves the field empty
and says so in `warnings`.

### `coverage`

`complete` and `partial` state whether every piece of data that the query
needed was reachable. `fragments` names what was missing.
`unindexed_predicates` names each predicate that the server ran without an
index.

### `accuracy`

`exact` states whether the result is precise. `approximation` names the
method when it is not. A `ravel_query_promql` result returned under
`allow_partial_coverage: true` with `coverage.partial: true` reports
`exact: false`, and `approximation` says the result omits the data the query
could not reach.

`lower_bound_count` is reserved and always `false`. Ingest into `logs` and
`spans` is at-least-once, so a retry can store a row twice and a `COUNT`
there can include duplicates: the count is an upper bound on distinct
records, not a lower bound, and no tool marks it.

### `presentation`

How the server shaped the result to fit the response cap:

- `max_rows`
- `row_cap_hit` and `bytes_cap_hit`: which cap stopped the result
- `rows_omitted` and `cells_truncated`: how many rows the server omitted and
  how many cells it truncated
- `metadata_elided`: how many list entries were elided. It also counts a
  cursor dropped because it exceeded its own bound.
- `effective_max_response_bytes`: the cap in force
- `floor_applied`: whether the server raised a value that you sent below its
  minimum
- `cursor`: the paging cursor, when one exists

### `budget`

- `effective`: the server's ceiling and your request, combined to their
  minimum. No per-tenant ceiling applies in this build.
- `actual`: what the call spent
- `estimate`: what the server estimated before it read anything, from
  `ravel_explain_query` and from each tool that runs a PromQL, analytics or
  SQL query (the exemplars tool computes no estimate). A component
  the server cannot bound is `null`, not 0, and `estimate.unbounded` names
  it.
- `estimate_is_upper_envelope`: true when every component of the estimate
  is bounded, so the estimate is an upper envelope; false when
  `estimate.unbounded` names a component. `ravel_explain_query` and
  `ravel_query_sql` report the same value for one statement. A PromQL or
  analytics query that reads the log lane or a federated remote reports
  false, since neither is in the estimate.

### `evidence`

A list of references. Each is an opaque `ref` token plus a `blake3_256` of
the canonical row bytes that it covers. Redeem a reference later to prove
that a row has not changed. No tool emits an `evidence` block in this build;
see [Evidence references](#evidence-references).

### `warnings` and `next_steps`

`warnings` are non-fatal notes about the call. `next_steps` names a concrete
follow-up action, on both a success and a failure: narrow the time range,
ask for the bounded form of a listing, retry after a wait. A step for a cap
names the cap that actually cut the result and never advises raising one
already at its ceiling. A result whose only cut is shortened cells is
complete, status `ok` with no cursor, and its `read_full_cells` step says
how to read those cells in full; the
[reference](../reference/mcp.md#cap-next-steps) has the rule.

## Time inputs

Five tools take a required time input: `ravel_query_sql`,
`ravel_search_logs`, `ravel_find_labels`, `ravel_analyze_timeseries`, and
`ravel_query_promql`. There is no default window. If you omit the time
input, the call fails with `missing_argument`, except on
`ravel_query_promql`, whose modes are below. `ravel_get_trace` is the
exception: its time range is optional, and a lookup without one reads
every span the tenant retains.

`ravel_capabilities` and `ravel_describe_data` take no time input.
`ravel_describe_data` reports the coverage window and the freshness
watermark itself.

Pass a time as an RFC 3339 timestamp in UTC (ending in Z or +00:00) or as
an integer count of nanoseconds, given as a string. A timestamp with any
other offset is refused as `invalid_argument`. A raw JSON number cannot hold
a nanosecond epoch exactly.
For the same reason, every timestamp that the server returns is a nanosecond
count, given as a string.

Range-shaped inputs use `time_range`, a half-open interval: the start is
included, the end is not. A row at the end timestamp is outside the window.

`ravel_query_promql` has two mutually exclusive modes:

- Range mode takes `time_range` and `step`.
- Instant mode takes one `evaluation_time` and no `time_range`.

If you send both, or neither, or only one of `time_range` and `step`, the
call fails with `invalid_argument`. The steps run from the range's start
while below its end. The engine evaluates in milliseconds, so a time or a
step that is not a whole millisecond is refused as `invalid_argument`
rather than rounded. The step takes a unit, such as `30s`. A range may have
at most 11,000 steps, the engine's own limit; past it the call fails with
`invalid_argument` and a next step naming the smallest step that fits.

One series can hold up to 11,000 samples, which `max_rows` does not bound
because it counts series. When the samples do not fit
`max_response_bytes`, the series that does not fit comes back with a
prefix of its samples, `ts_ns` and `value` of one length, and
`samples.samples_omitted` says how many were left out. When every series
is present the result is `ok`, and its `read_full_cells` step says to widen
`step` or narrow `time_range`, or to raise `max_response_bytes`; a result
that also dropped a series is `ok_bounded`.

## Budgets you can lower

The effective budget for a call is the smaller of two values: the
server's ceiling and the value you sent. The server's ceilings come from two
places, both server-wide: its query engine configuration sets the ceiling
for `deadline_ms`, `max_bytes_scanned`, `max_store_requests` and
`max_segments`, and its MCP budget configuration sets the ceiling for
`max_response_bytes`, 4 MiB in this build, which has no setting to change
it. `max_rows` has a fixed ceiling of 5,000. No per-tenant
ceiling applies in this build. You can lower `deadline_ms`, `max_rows`,
`max_bytes_scanned`, `max_store_requests`, `max_segments` and
`max_response_bytes`. You cannot raise any of them past its ceiling.

The deadline covers the whole tool call. A `ravel_search_logs` call with
`predicates` first looks up the typed attribute columns and then runs its
search, and the search only gets the time the lookup left. Its
`budget.actual` counts the search only, and a warning on the page says so.

Defaults and floors:

- `max_rows`: 200, with a ceiling of 5,000.
- `max_response_bytes`: 512 KiB, with a floor of 256 KiB. If you send a value
  below the floor, the server raises it to the floor and
  `presentation.floor_applied` states so.
- `ravel_describe_data`: 100 metric families per page. An optional
  `cursor` input asks for the next page. The response carries
  `presentation.cursor` when more families exist or when
  `max_response_bytes` cut the page. Request the next page
  with the same signal and that cursor. The cursor follows the same
  codec, tenant binding, and lifetime as every other cursor. Its
  `budget.actual` counts the catalog resolve only: the metric metadata
  and tenant configuration reads are not counted, and every page says so
  in `warnings`.
- `ravel_find_labels`: at most 2,000 segments admitted for label
  resolution, fewer when your `max_segments` is lower. It
  takes no `max_rows`, and a page is bounded by bytes alone. A page cut
  short reports through `presentation.bytes_cap_hit` and
  `presentation.rows_omitted`. A page with `bytes_cap_hit` false is the
  complete match set for the window and filter that you asked for.

`ravel_explain_query` compares its cost estimate to the effective budget
before you run anything:

| Estimate | Budget |
| --- | --- |
| `estimated_requests` | `max_store_requests` |
| `estimated_store_bytes` | `max_bytes_scanned` |
| admitted segments | `max_segments` |

Segments from ingest hours the catalog has not folded yet are exempt from
`max_segments` and do not count against it. When any estimate exceeds its
budget, the call fails with `budget_estimate_exceeds_ceiling` and names the
factor to narrow the query by: the largest ratio of estimate to budget among
the ones that exceed.

An estimate component that the server cannot bound is never treated as zero.
The explain names it unbounded in `warnings` and sets
`estimate_is_upper_envelope` to false.

- A component with no budget, such as `estimated_decompressed_bytes` on
  the logs table, does not refuse the call.
- A component that has a budget refuses the call with
  `budget_estimate_exceeds_ceiling`, except on a Parquet table.
- On a Parquet table, `estimated_requests` and `estimated_store_bytes` are
  both unbounded, because a cold cache and evicted ranges cost more than
  the manifests count. The explain does not refuse. It adds a warning for
  each budget it did not check, because the Parquet reader enforces
  `max_store_requests` and `max_bytes_scanned` at scan time and refuses the
  read that would cross either.

## Cursors and evidence references

A cursor and an evidence reference are both short-lived tokens. The server
mints them on the fly and does not store them. Each carries the tenant, the
tool, a hash of the arguments that produced it, and the pinned snapshot that
it belongs to. No cursor and no evidence reference carries an object storage
key, a tenant name, or a credential.

### Cursors

A cursor stays valid until the protection horizon minus the grace period,
counted from the instant page one resolved. Every later page keeps that
instant, so the whole walk shares one window: a later cursor does not get a
fresh one. The deadline of the call that returned a cursor does not shorten
it; the next call runs under its own deadline.

The server checks a cursor in this order and stops at the first failure:

| Case | Failure |
|---|---|
| The token is over 1 MiB, is not valid base64, or is not a cursor of this format and version. | `cursor_invalid` |
| The token names another server process: the process that minted it has since restarted, or the process field was edited. Only the minting process can redeem a cursor, so this is checked before the token's integrity. | `cursor_expired` |
| The token was altered in any other way, so its integrity check fails. | `cursor_invalid` |
| The cursor is sent back for another tenant, to another tool, or with different arguments. | `cursor_invalid` |
| The cursor's lifetime has passed. | `cursor_expired` |
| A newer pending erasure, a change to the typed attribute columns, or (for a `ravel_query_sql` walk ordered by a count) a different segment set than page one read. | `cursor_expired` |

Paging order differs per tool:

- `ravel_query_sql` pages by keyset over the statement's `ORDER BY`,
  together with the tiebreak that the tool appends where the table has a
  row identity (`series_id` on `samples`). When that order is total, the
  cursor resumes after the page's last row. When it is not, a warning says
  so, the equal-group rule applies as for `ravel_search_logs`, and cursor
  paging continues. A page whose last row has no cursor position (a NaN
  or infinite order value, or a cursor over its 4 KiB bound) ends
  `ok_bounded` with no cursor and a `narrow_time_range` next step. The
  cursor pins a digest of the segments page one read. Under an `ORDER BY`
  on a `count(...)`, a later page is served only over those same segments,
  else it is `cursor_expired` with a `rerun_page_one` next step; a
  `time_range` that ends before the most recent ingest keeps them stable.
  Under an order on the row's own columns, a later page over changed
  segments is served with a warning that a row committed since may appear
  or be omitted. An `ORDER BY` on any other aggregate or window value mints
  no cursor at all; order by a count or by the row's own columns to page
  such a result.
- `ravel_search_logs` orders by `(ts, observed_ts)`, which is not always
  unique, because `logs` rows carry no row identity. The equal-group rule
  applies. A later page over segments that changed since page one carries
  the same warning.
- `ravel_get_trace` mints no cursor: it returns the trace's spans in one
  page, ordered by `start_ts` and `span_id`, and a trace larger than the row
  cap comes back bounded (`row_cap_hit`) rather than paged. Raise `max_rows`
  (up to 5,000) or narrow the time range to read more of it; a trace the
  byte cap cut needs smaller rows or a larger `max_response_bytes` instead.
- `ravel_query_promql` mints no cursor: a PromQL result has no order a
  cursor could resume. Its row cap counts series, and a result with more
  series than `max_rows` comes back `ok_bounded`.
- `ravel_describe_data` pages by keyset over the row kind and the name:
  columns first, then indexed keys, then metric families, each sorted by
  name in byte order, so `Zeta` comes before `alpha`. The cursor holds the
  last row's kind and name, and the next page starts after it, so a page
  cut inside the columns continues with the next column. The `table` row
  is on page one only. Each page reads its lists afresh. A row added later
  whose name sorts before the cursor's position is not returned by any
  later page, and `families_total` can differ from one page to the next.
  Every page reports page one's watermark and snapshot id and resolves at
  page one's instant. The column lookups, the audit record and the cursor
  deadline use the time of the call itself. A newer pending erasure on the
  same signal, a change to the typed attribute columns, or a column or key
  at the cursor's position that is no longer listed expires the cursor
  with `cursor_expired`.

The equal-group rule: a page never ends inside a group of equal tuples. If a
group does not fit on the page whole, the tool drops that whole group from
the page. The cursor points at the last complete group, and the next page
starts after it.

When no complete group fits in the row cap, the server returns the rows that
it has, up to the row cap, with status `ok_bounded` and no cursor.
`next_steps` carries the step for the cap that cut the group, by the
[cap step rule](../reference/mcp.md#cap-next-steps).

### Evidence references

No tool redeems an `evidence_ref` or emits an `evidence` block in this
build. Every tool refuses an `evidence_ref` before it reads anything:
`ravel_query_sql`, `ravel_query_promql`, `ravel_search_logs`,
`ravel_get_trace` and `ravel_analyze_timeseries`, which list it in their
input schemas, with `unsupported`, and every other tool, Ravel Agents tools
included, with `invalid_argument`. The budget arguments are the exception
to a schema's list: the server reads the six budget arguments (`max_rows`, `max_response_bytes`,
`deadline_ms`, `max_bytes_scanned`, `max_store_requests`, `max_segments`)
on every tool call, whether or not the tool's schema lists them, refuses a
call with `invalid_argument` when one of them has the wrong type, and applies
a well-typed one to the call's budget, clamped to its ceiling, except that
`ravel_analyze_timeseries` refuses `max_bytes_scanned`, `max_store_requests`
and `max_segments` as `unsupported`. The
[reference](../reference/mcp.md#evidence-references) has the same list.

The rest of this section describes the design. Redeeming a reference
re-executes the tool with the reference's own arguments. The server then compares the BLAKE3-256 digest of the canonical
row bytes.

| Pin state | Redemption |
|---|---|
| The pin is valid | The re-execution runs against the pinned snapshot of the reference. |
| The pin has expired | The re-execution runs fresh. It reports `pinned: false` and states whether the hash matched. `cursor_invalid` and `cursor_expired` do not apply. |

A matching hash proves that the bytes are identical. It proves nothing about
whether the same query returns that row today.

## Server limits

The server does not promise these things:

- **Completeness of a trace.** `ravel_get_trace` reports the spans that it
  found, and for each one whether its parent is among them. A `missing`
  parent tells you a span is absent from what the server holds or from the
  window you asked for; it does not tell you whether that span will still
  arrive. The tool does not promise that every span of a trace has arrived. Distributed
  tracing is best-effort, and a trace can still be receiving spans when you
  ask.
- **Immunity to prompt injection.** Telemetry text (a log body, a span
  attribute, an error message) comes back inside typed fields. The server
  never turns it into a tool description or an instruction. No server-side
  filtering makes a model immune to a hostile instruction hidden in that
  text. Treat telemetry content the same way as text from an untrusted
  webpage.
- **Dollar figures.** The server reports usage as request counts and byte
  counts, split by kind: data moved over the wire, served from cache, or
  decompressed. It never reports usage as a cost in money.

## If the result is empty

A `data.row_count` of zero and a call that failed are different signals. A
result with rows and `status` `ok_bounded` is a third: more rows exist and
the server minted no cursor for them. Work through the empty case in order:

1. **The call failed instead of returning zero rows.** Read `status`. If
   it is `error`, the empty `data` block is not the answer: read `failure`
   and `next_steps`.
2. **Nothing was ingested in the window you asked for.** `status` is `ok`.
   Compare `scope.time_range` to what you meant to ask. Then query the
   signal with a wider window.
3. **The data exists but has not become visible yet.** Compare
   `visibility.ingest_watermark_hour` to the end of your window. A window
   that reaches past that hour can be empty now and non-empty after the newer
   data is ingested. If the field is empty, read `warnings`. Either this
   operation does not report the field, or the call resolved no segments. The
   warning text differs for the two cases.
4. **A predicate matched nothing.** Read `scope.predicates_applied` to see
   the predicate that the server ran. It can differ from what you typed. For
   example, a typed-attribute-column name did not match and the server fell
   back to an unindexed map lookup.
5. **Only part of the data was reachable.** `coverage.complete` is
   `false`. `coverage.fragments` names what the call did not reach.
   `coverage.unindexed_predicates` names each predicate that ran without an
   index instead of failing outright.
6. **The cursor ran out.** A paging call with no more rows to give back
   returns `ok` with an empty `data.rows` and no `presentation.cursor`.
   That is the end of the result set, not a fault.

See [the MCP reference](../reference/mcp.md) for the shape of each tool and
every failure class.
