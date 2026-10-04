# Agents (MCP)

> The tools ship behind the `mcp` cargo feature and the `--mcp` flag, both
> off by default. Build `ravel-server` with `--features mcp`, then run it
> with `--mcp` (only supported under `--mode all` or `--mode query`) to
> mount `POST /mcp` on the query router; `--mcp-allowed-origins` is required
> once that listener is reachable from anywhere but localhost. One tool is
> served today, `ravel_capabilities`. The other eight are catalogued, so
> `tools/list` returns them, and a call to one refuses with a
> `method_not_found` protocol error until its body lands; the sections below
> describe the surface as designed. The [MCP reference](../reference/mcp.md)
> carries the split `ravel_capabilities` reports at runtime.

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
cannot write data, delete data, or trigger maintenance.

### Get oriented

- `ravel_capabilities`: the protocol and server version, which tools are
  enabled, the effective budget ceilings, a summary of the SQL and PromQL
  dialects, and which signals (metrics, logs, traces) the tenant has. Reads
  no data.
- `ravel_describe_data`: for one signal, the effective schema (fixed and
  typed attribute columns), which keys are indexed, the metric families with
  their type and unit, the freshness watermark, the coverage window, and
  exact row counts where the catalog can give them.

### Find labels

- `ravel_find_labels`: metric names for a selector, label names, or the
  values of one label under a selector. The list is observed from the data
  over the window that you asked for. The result states whether the list is
  complete for that window.

  A call must carry a selector or a label name. The server refuses a call
  that carries neither, and names the bounded form to ask for. An optional
  `filter` does not satisfy that bound. A selector or a label name narrows
  what the call resolves. A filter narrows only what the call returns.

### Check a query first

- `ravel_explain_query`: validates a SQL or PromQL statement, resolves the
  snapshot that it targets, and runs no scan. It returns the target table,
  the effective schema, how many segments it admits, the cost estimate
  against the effective budget, and the plan shape.

### Run a query

- `ravel_query_sql`: one `SELECT` over one table, with a required
  `time_range` applied as a row filter, `max_rows`, budgets you can only
  lower, and a cursor for the next page.
- `ravel_query_promql`: an instant or a range PromQL evaluation, with
  partial-coverage consent, step alignment, and the two log pseudo-metrics
  available the same as any other metric name.

### Search logs

- `ravel_search_logs`: a typed log search compiled to SQL. It uses indexed
  and typed-attribute predicates, `has_word` body search, severity, and
  trace id. Results order by timestamp. Rows that share an attribute set are
  grouped, so the shared attributes are not repeated.

### Trace and time series

- `ravel_get_trace`: every span of one trace id inside a required window,
  the span tree, which parents are missing, and which spans are orphaned. An
  optional logs pass runs alongside it, reported as an unindexed scan with
  its own cost.
- `ravel_analyze_timeseries`: change-point detection or a summary statistic
  over a PromQL range result. The result names the method as a heuristic,
  states the minimum point count that the method needs, and states whether
  the input was downsampled first.

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

### `visibility`

The snapshot that this call read:

- `snapshot_id`
- `ingest_watermark_hour`
- `min_commit_tokens_applied`: the commit tokens that the call honored
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
method when it is not. `lower_bound_count` is `true` on a `COUNT` over `logs`
or `spans`. Ingest there is at-least-once, and a retry can add rows that a
count cannot tell apart from originals.

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

- `effective`: the server ceiling, the tenant ceiling, and your request,
  combined to their minimum
- `actual`: what the call spent
- `estimate`: for `ravel_explain_query`
- `estimate_is_upper_envelope`: states that the estimate is an upper
  envelope, never a prediction

### `evidence`

A list of references. Each is an opaque `ref` token plus a `blake3_256` of
the canonical row bytes that it covers. Redeem a reference later to prove
that a row has not changed. `ravel_find_labels` emits no `evidence` block.

### `warnings` and `next_steps`

`warnings` are non-fatal notes about the call. `next_steps` names a concrete
follow-up action, on both a success and a failure: narrow the time range,
ask for the bounded form of a listing, retry after a wait.

## Time inputs

Six tools take a required time input: `ravel_query_sql`,
`ravel_search_logs`, `ravel_get_trace`, `ravel_find_labels`,
`ravel_analyze_timeseries`, and `ravel_query_promql`. There is no default
window. If you omit the time input, the call fails with `missing_argument`.

`ravel_capabilities` and `ravel_describe_data` take no time input.
`ravel_describe_data` reports the coverage window and the freshness
watermark itself.

Pass a time as an RFC 3339 string or as an integer count of nanoseconds,
given as a string. A raw JSON number cannot hold a nanosecond epoch exactly.
For the same reason, every timestamp that the server returns is a nanosecond
count, given as a string.

Range-shaped inputs use `time_range`, a half-open interval: the start is
included, the end is not. A row at the end timestamp is outside the window.

`ravel_query_promql` has two mutually exclusive modes:

- Range mode takes `time_range` and `step`.
- Instant mode takes one `evaluation_time` and no `time_range`.

If you send both, or neither, the call fails with `invalid_argument`.

## Budgets you can lower

The effective budget for a call is the smallest of three values: the
server's ceiling, the tenant's ceiling, and the value you sent. You can
lower `deadline`, `max_rows`, `max_bytes_scanned`, `max_store_requests`, and
`max_response_bytes`. You cannot raise any of them past its ceiling.

Defaults and floors:

- `max_rows`: 200, with a ceiling of 5,000.
- `max_response_bytes`: 512 KiB, with a floor of 256 KiB. If you send a value
  below the floor, the server raises it to the floor and
  `presentation.floor_applied` states so.
- `ravel_describe_data`: 100 metric families per page. An optional
  `cursor` input asks for the next page. The response carries
  `presentation.cursor` when more families exist. Request the next page
  with the same signal and that cursor. The cursor follows the same
  codec, tenant binding, and lifetime as every other cursor.
- `ravel_find_labels`: 2,000 segments admitted for label resolution. It
  takes no `max_rows`, and a page is bounded by bytes alone. A page cut
  short reports through `presentation.bytes_cap_hit` and
  `presentation.rows_omitted`. A page with `bytes_cap_hit` false is the
  complete match set for the window and filter that you asked for.

`ravel_explain_query` compares its cost estimate to the effective budget
before you run anything. When the estimate exceeds the budget, the call
fails with `budget_estimate_exceeds_ceiling` and names the factor to narrow
the query by. An estimate that the server cannot compute counts as exceeding
the budget. The server never treats it as zero.

## Cursors and evidence references

A cursor and an evidence reference are both short-lived tokens. The server
mints them on the fly and does not store them. Each carries the tenant, the
tool, a hash of the arguments that produced it, and the pinned snapshot that
it belongs to. No cursor and no evidence reference carries an object storage
key, a tenant name, or a credential.

### Cursors

A cursor stays valid until the earlier of the call's remaining deadline
and the protection horizon minus the grace period.

| Case | Failure |
|---|---|
| The server process that minted the cursor has since restarted. Only that process can redeem it. | `cursor_expired` |
| The cursor is sent back for the wrong tenant, or is altered. | `cursor_invalid` |

Paging order differs per tool:

- `ravel_query_sql` mints a cursor only when the statement's `ORDER BY`,
  together with the tiebreak that the tool appends, orders every row
  uniquely. When the tiebreak is not unique, the equal-group rule applies
  as for `ravel_search_logs`, and cursor paging continues.
- `ravel_search_logs` orders by a tuple that is not always unique, because
  `logs` rows carry no row identity. The equal-group rule applies.
- `ravel_get_trace` orders by `start_ts` and `span_id`, and every span
  carries a `span_id`. The equal-group rule never applies to it.

The equal-group rule: a page never ends inside a group of equal tuples. If a
group does not fit on the page whole, the tool drops that whole group from
the page. The cursor points at the last complete group, and the next page
starts after it.

When no complete group fits in the row cap, the server returns the rows that
it has, up to the row cap, with status `ok_bounded` and no cursor.
`next_steps` names narrowing `time_range`.

### Evidence references

Every data tool but `ravel_find_labels` accepts an optional `evidence_ref`
input. Redeeming a reference re-executes the tool with the reference's own
arguments. The server then compares the BLAKE3-256 digest of the canonical
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
  found, the parents that are missing, and the spans with no known parent.
  It does not promise that every span of a trace has arrived. Distributed
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
