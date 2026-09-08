# Agents (MCP)

> This page describes the surface as designed. The tools ship behind the
> `mcp` cargo feature and the `--mcp` flag, both off by default.

## What the MCP surface is for

An agent investigating an incident needs four things from Ravel: a way to
find what data exists without guessing, a way to run bounded queries with a
known cost, a way to move between metrics, logs, and traces on exact
identifiers, and a way to hand back a finding that a person can check again.
The Model Context Protocol (MCP) surface gives an agent host these four
things over one connection.

## Connecting

The server exposes MCP over Streamable HTTP at `POST /mcp`, on the same
listener as the rest of the query API. Authenticate with the same bearer
credential the HTTP query routes use: `Authorization: Bearer <token>`. There
is no separate agent credential and no second login step.

The server speaks two protocol revisions. `2026-07-28` carries its protocol
version and method name in headers on every request and needs no handshake.
`2025-11-25` is the older revision that most deployed clients still speak: it
opens with an `initialize` call and keeps a session id. Use whichever
revision your MCP client sends. The server answers both on the same
endpoint.

Every MCP request authenticates on its own. A cursor or an evidence
reference from an earlier call carries no authority by itself: the server
always checks it against the credential on the current request.

## The nine tools

The tools below are grouped by what you are trying to do. Every tool is
read-only: the default profile cannot write data, delete data, or trigger
maintenance.

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
  values of one label under a selector. The result states whether the list
  is declared through configuration, observed from data, or exact. Ask for a
  tenant-wide list with no filter and the tool refuses, naming the bounded
  form to ask for instead.

### Check a query before you run it

- `ravel_explain_query`: validates a SQL or PromQL statement, resolves the
  snapshot it targets, and returns the target table, the effective schema,
  how many segments it admits, the cost estimate against the effective
  budget, and the plan shape. It runs no scan.

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
  trace id. Results order by timestamp, and rows that share an attribute set
  are grouped so the shared attributes are not repeated.

### Investigate a trace or a time series

- `ravel_get_trace`: every span of one trace id inside a required window,
  the span tree, which parents are missing, and which spans are orphaned. An
  optional logs pass runs alongside it, reported as an unindexed scan with
  its own cost.
- `ravel_analyze_timeseries`: change-point detection or a summary statistic
  over a PromQL range result. The result names the method as a heuristic,
  states the minimum point count the method needs, and states whether the
  input was downsampled first.

## The result envelope

Every tool call returns one envelope, on success and on failure.

### `status`

One of four values, each a different fact about the result:

- `ok`: the query completed. A query with zero matching rows is `ok`. A
  `LIMIT` the data did not fill is still `ok`.
- `ok_bounded`: a row cap stopped the result and more rows exist, but the
  statement has no total order, so no cursor exists for the rest.
- `ok_page`: a cap stopped the result and a cursor exists. `presentation`
  states whether the row cap, the byte cap, or both caused it.
- `error`: the call failed. `failure` names why.

### `failure`

`null` on every status but `error`. On `error`, it names the failure class
(see the reference page) and carries a message safe to show a person.

### `data`

`columns`, `rows`, and `row_count`. `row_count` is the number of rows the
query actually produced, even when `presentation.bytes_cap_hit` later drops
some of them from `rows` to fit the response cap.

### `scope`

What the server actually ran: the `signal`, the `table`, the `time_range` it
used, the predicates it applied, and the order it returned rows in. Compare
this to what you asked for when a result looks wrong: the server can apply a
predicate slightly differently than the one you typed.

### `ids`

`query_id` for this call, and `audit_ref` pointing at its audit record, when
the deployment writes one.

### `visibility`

The snapshot this call read: `snapshot_id`, the `watermark_hour` the catalog
has folded up to, which `min_commit_tokens_applied` the call honored, and
`pinned`, whether this call's snapshot stays held for a later paged call.

### `coverage`

`complete` and `partial` state whether every piece of data the query should
have touched was reachable. `fragments` names what was missing, and
`unindexed_predicates` names any predicate the server ran without an index.

### `accuracy`

`exact` states whether the result is precise. `approximation` names the
method when it is not. `lower_bound_count` is `true` on a `COUNT` over
`logs` or `spans`, because ingest there is at-least-once and a retry can add
rows a count cannot tell apart from originals.

### `presentation`

How the result was shaped to fit the response cap: `max_rows`, whether
`row_cap_hit` or `bytes_cap_hit` stopped it, how many `rows_omitted` and
`cells_truncated`, how many list entries were `metadata_elided`, the
`effective_max_response_bytes` actually in force, whether `floor_applied`
raised a value you sent below the server's minimum, and the paging `cursor`,
when one exists.

### `budget`

`effective` (the server ceiling, the tenant ceiling, and your request,
combined to their minimum), `actual` (what the call spent), `estimate` (for
`ravel_explain_query`), and `estimate_is_upper_envelope`. The estimate is
an upper envelope, never a prediction, and the field says so.

### `evidence`

A list of references, each an opaque `ref` token plus a `sha256` of the
exact row bytes it covers. Redeem a reference later to prove a row has not
changed.

### `warnings` and `next_steps`

`warnings` are non-fatal notes about the call. `next_steps` names a concrete
follow-up action, on both a success and a failure: narrow the time range,
ask for the bounded form of a listing, retry after a wait.

## Time inputs

Every tool that reads data requires a time input. There is no default
window: omit one and the call fails with `missing_argument`.

Pass a time as an RFC 3339 string or as an integer count of nanoseconds,
given as a string. A raw JSON number cannot hold a nanosecond epoch exactly.

Range-shaped inputs use `time_range`, a half-open interval: the start is
included, the end is not. A row at exactly the end timestamp sits outside
the window.

`ravel_query_promql` has two mutually exclusive modes. Range mode takes
`time_range` and `step`. Instant mode takes one `evaluation_time` and no
`time_range`. Send both, or neither, and the call fails with
`invalid_argument`.

Every timestamp the server returns is a nanosecond count, given as a string,
for the same reason the inputs are.

## Budgets you can lower

The effective budget for a call is the smallest of three values: the
server's ceiling, the tenant's ceiling, and the value you sent. You can
lower `deadline`, `max_rows`, `max_bytes_scanned`, `max_store_requests`, and
`max_response_bytes`. You cannot raise any of them past its ceiling.

Defaults and floors:

- `max_rows`: 200, with a ceiling of 5,000.
- `max_response_bytes`: 512 KiB, with a floor of 256 KiB. Send a value below
  the floor and the server raises it to the floor; `presentation.floor_applied`
  states so.
- `ravel_describe_data`: 100 metric families per page.
- `ravel_find_labels`: 2,000 segments admitted for label resolution.

`ravel_explain_query` compares its cost estimate to the effective budget
before you run anything. When the estimate exceeds the budget, the call
fails with `budget_estimate_exceeds_ceiling` and names the factor to narrow
the query by. An estimate the server cannot compute counts as exceeding the
budget: the server never treats it as zero.

## Cursors and evidence references

A cursor and an evidence reference are both short-lived tokens the server
mints on the fly, not something the server stores. Each carries the tenant,
the tool, a hash of the arguments that produced it, and the pinned snapshot
it belongs to.

A cursor stays valid until the earlier of the call's remaining deadline
and the protection horizon minus the grace period. Only the server
process that minted a cursor can redeem it: a cursor from a process
that has since restarted fails with `cursor_expired`. A cursor sent
back for the wrong tenant, or altered, fails with `cursor_invalid`.

`ravel_query_sql` mints a cursor only when the statement's `ORDER BY`,
together with the tiebreak the tool appends, orders every row uniquely.
`ravel_search_logs` orders by a tuple that is not always unique, since
`logs` rows carry no row identity. `ravel_get_trace` orders by `start_ts`
and `span_id`, and every span carries a `span_id`.
When a page would end inside a group of equal rows, `ravel_search_logs`
drops that whole group from the page rather than split it, and the next
cursor starts after the group. If no complete group fits in one page, the
call returns `ok_bounded` with no cursor, and `next_steps` names narrowing
`time_range`.

An evidence reference works the same way, with one difference: redeeming it
after its pin expires re-runs the query fresh instead of failing, reports
`pinned: false`, and reports whether the row's hash still matches. A
matching hash proves the bytes are identical. It proves nothing about
whether the same query would return that row today.

No cursor and no evidence reference carries an object storage key, a tenant
name, or a credential.

## What the server does not promise

- **Completeness of a trace.** `ravel_get_trace` reports the spans it
  found, the parents that are missing, and the spans with no known parent.
  It does not promise every span for a trace has arrived: distributed
  tracing is best-effort, and a trace can still be receiving spans when you
  ask.
- **Immunity to prompt injection.** Telemetry text (a log body, a span
  attribute, an error message) comes back inside typed fields, and the
  server never turns it into a tool description or an instruction. No
  server-side filtering makes a model immune to a hostile instruction
  hidden in that text. Treat telemetry content the same way you would treat
  text from an untrusted webpage.
- **Dollar figures.** Usage is reported as request counts and byte counts,
  split by kind: data moved over the wire, served from cache, or
  decompressed. The server never reports usage as a cost in money.

## If the result is empty

A `data.row_count` of zero and a call that failed are different signals.
Work through these in order:

1. **The call failed instead of returning zero rows.** Check `status`. If
   it is `error`, the empty `data` block is not the answer: read `failure`
   and `next_steps`.
2. **Nothing was ingested in the window you asked for.** `status` is `ok`.
   Compare `scope.time_range` to what you meant to ask, and check the
   signal with a wider window.
3. **The data exists but has not become visible yet.** Compare
   `visibility.watermark_hour` to your window's end. A window that reaches
   past the watermark can be honestly empty for now and non-empty once the
   catalog catches up.
4. **A predicate matched nothing.** Read `scope.predicates_applied` to see
   the predicate the server actually ran, which can differ from what you
   typed, for example when a typed-attribute-column name did not match and
   the server fell back to an unindexed map lookup.
5. **Only part of the data was reachable.** `coverage.complete` is
   `false`. `coverage.fragments` names what the call did not reach, and
   `coverage.unindexed_predicates` names any predicate that ran without an
   index instead of failing outright.
6. **The cursor ran out.** A paging call with no more rows to give back
   returns `ok` with an empty `data.rows` and no `presentation.cursor`.
   That is the end of the result set, not a fault.

See [the MCP reference](../reference/mcp.md) for the exact shape of each
tool and every failure class.
