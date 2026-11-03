# MCP tools reference

> The `POST /mcp` route exists in builds compiled with the `mcp` cargo
> feature, and is mounted when `--mcp` is passed; both are off by default.
> All nine tools are served: `ravel_capabilities`, `ravel_describe_data`,
> `ravel_find_labels`, `ravel_explain_query`, `ravel_query_sql`,
> `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace` and
> `ravel_analyze_timeseries`. `ravel_capabilities` lists them in
> `tools.enabled`; its `tools.catalogued` list, for a declared name a build
> does not serve, is empty.

The tools use the Model Context Protocol over `POST /mcp`. Each tool lists
what it takes, what it returns, its bounds, and the failure classes that it
can produce. See [the MCP server guide](../guides/mcp-server.md) for the tool
grouping, the envelope fields, and the empty-result checklist.

## Tools

Each tool's subsection below is generated from the catalog the server
advertises in `tools/list`: its title, description and annotations, and each
argument with its JSON type and whether it is required. Tools appear in the
order `tools/list` lists them. Arguments are listed required first, then
optional, each group sorted by name; inside an object type, a field marked
`?` is optional. Do not edit the subsections by hand: regenerate them with

```sh
RAVEL_UPDATE_MCP_REFERENCE=1 cargo test -p ravel-mcp --test mcp_reference
```

### Query tools

<!-- BEGIN GENERATED MCP TOOLS -->
#### `ravel_capabilities`

- Title: Capabilities
- Description: Protocol and server version, enabled tools, effective budget ceilings, dialect summaries, tenant hash, enabled signals
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

No arguments.

#### `ravel_describe_data`

- Title: Describe data
- Description: Effective schema, indexed keys, metric families, freshness watermark, coverage window, committed row count (an upper bound)
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `signal` | string | yes |  |
| `cursor` | string | no |  |

#### `ravel_find_labels`

- Title: Find labels
- Description: Metric names, label names, or label values for a selector
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |  |
| `deadline_ms` | integer | no |  |
| `filter` | string | no |  |
| `label_name` | string | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. A label list is bounded by its own page, but the page is still what a caller may need to make smaller. |
| `selector` | string | no |  |

#### `ravel_explain_query`

- Title: Explain query
- Description: Validate a SQL statement, estimate its cost, and return the plan shape. No scan runs.
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | yes |  |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |  |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. An explain returns a plan and an effective schema, both of which grow with the statement. |

#### `ravel_query_sql`

- Title: Query SQL
- Description: One SELECT over one table
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | yes |  |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |  |
| `cursor` | string | no |  |
| `deadline_ms` | integer | no |  |
| `evidence_ref` | string | no | Not served by this build: any value is refused as `unsupported`. |
| `max_bytes_scanned` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `max_rows` | integer | no | Rows returned; default 200, ceiling 5,000. |
| `max_segments` | integer | no |  |
| `max_store_requests` | integer | no |  |

#### `ravel_query_promql`

- Title: Query PromQL
- Description: Instant or range PromQL evaluation
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | yes |  |
| `allow_partial_coverage` | boolean | no | Absent means no consent: a partial-coverage result is refused rather than the whole call failing to deserialize. |
| `deadline_ms` | integer | no |  |
| `evaluation_time` | string | no | Set alone for an instant evaluation. Exactly one of this or the `time_range`/`step` pair must be present. |
| `evidence_ref` | string | no | Not served by this build: any value is refused as `unsupported`. |
| `max_bytes_scanned` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `max_rows` | integer | no | Series rows returned; default 200, ceiling 5,000. |
| `max_segments` | integer | no |  |
| `max_store_requests` | integer | no |  |
| `step` | string | no | A duration with a unit, such as `30s`. The step and every time must be a whole millisecond. |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | no | Set together with `step` for a range evaluation. Exactly one of this pair or `evaluation_time` must be present. Steps run from `start_ns` while below `end_ns`. |

#### `ravel_search_logs`

- Title: Search logs
- Description: Typed log search compiled to SQL, ordered by (ts, observed_ts). Rows grouped by attribute key set; __ravel_group_attrs lists attributes the group shares once
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |  |
| `cursor` | string | no |  |
| `deadline_ms` | integer | no |  |
| `evidence_ref` | string | no | Not served by this build: any value is refused as `unsupported`. |
| `has_word` | string | no | Rows whose body holds these words as a run of whole tokens. |
| `max_bytes_scanned` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `max_rows` | integer | no | Rows returned; default 200, ceiling 5,000. |
| `max_segments` | integer | no |  |
| `max_store_requests` | integer | no |  |
| `predicates` | array of object {`key`: string, `op`: string (`eq`, `ne`, `in` or `exists`), `value`?: string or array of string} | no |  |
| `severity` | string | no | trace, debug, info, warn, error or fatal: that level or above. |
| `trace_id` | string | no | 32 hex characters, either case. |

#### `ravel_get_trace`

- Title: Get trace
- Description: Every span of one trace id, ordered by start time, with __ravel_depth and __ravel_parent (root, present, missing, not_returned, cycle). time_range is optional; without it every span the tenant retains is read
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `trace_id` | string | yes | 32 hex characters, either case. |
| `deadline_ms` | integer | no |  |
| `evidence_ref` | string | no | Not served by this build: any value is refused as `unsupported`. |
| `include_logs` | boolean | no | Not served by this build: `true` is refused as `unsupported`; for logs, use ravel_search_logs. |
| `max_bytes_scanned` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `max_rows` | integer | no | Spans returned; default 200, ceiling 5,000. |
| `max_segments` | integer | no |  |
| `max_store_requests` | integer | no |  |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | no | Optional. Omitted, the lookup reads every span the tenant retains. |

#### `ravel_analyze_timeseries`

- Title: Analyze timeseries
- Description: change_point or summary over a PromQL range result
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `op` | string (`change_point` or `summary`) | yes |  |
| `query` | string | yes |  |
| `step` | string | yes | A duration with a unit, such as `30s`. The step and every time must be a whole millisecond. |
| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |  |
| `deadline_ms` | integer | no |  |
| `evidence_ref` | string | no | Not served by this build: any value is refused as `unsupported`. |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. An analysis runs a PromQL range evaluation underneath, so it takes the same deadline and response bound the evaluation would. |
| `max_rows` | integer | no | Series rows returned; default 200, ceiling 5,000. |

<!-- END GENERATED MCP TOOLS -->

### What each query tool returns

The rules an input schema cannot carry, and each tool's output blocks,
bounds and failure classes.

| Tool | Notes | Output blocks used | Bounds | Failure classes |
| --- | --- | --- | --- | --- |
| `ravel_capabilities` | `tools.enabled` lists the query catalog's served tools, and `tools.catalogued` any it declares without serving (empty in this build). The six Ravel Agents tools appear in `tools/list` only, not in either list | `data` | none; reads no data | `unauthorized`, `invalid_argument`, `internal` |
| `ravel_describe_data` | `signal` is `metrics`, `logs` or `traces`. The committed row count is an upper bound, summed from commit records before duplicate collapse and pending erasure | `data` (`kind`, `name`, `value` rows: `table`, `column`, `indexed_key`, `family`), `scope`, `visibility`, `coverage`, `accuracy` (`exact: false` on page one, for the row count), `presentation`, `budget` | 100 metric families per page; the schema declares no budget input, and a page is bounded by `max_response_bytes`, 512 KiB default, 256 KiB floor | `unauthorized`, `invalid_argument`, `unavailable`, `deadline`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_find_labels` | A call carrying neither a `selector` nor a `label_name` is refused, whatever its `filter`. See [Which list `ravel_find_labels` returns](#which-list-ravel_find_labels-returns) | `data`, `scope`, `coverage`, `presentation` | at most 2,000 segments admitted for resolution, fewer when the effective `max_segments` is lower; `max_response_bytes` 512 KiB default, 256 KiB floor. No `max_rows`: a page is bounded by bytes, and a page cut short reports through `presentation.bytes_cap_hit` and `presentation.rows_omitted` | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_explain_query` | SQL only: see [Explaining a statement](#explaining-a-statement) | `data` (effective schema as `data.columns`, zero rows), `scope`, `visibility`, `budget` (the estimate beside `effective`), `plan` (a text block that the explain tool alone populates) | compares the estimate against the effective budget | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_estimate_exceeds_ceiling`, `deadline`, `unavailable`, `internal` |
| `ravel_query_sql` | The `SELECT` is read-only, its rows are filtered to `time_range`, and it is paged by a keyset cursor. See [Running a statement](#running-a-statement) | `data`, `scope` (`order_by` is the effective order, tiebreak included), `visibility`, `presentation`, `budget` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_query_promql` | Exactly one mode: `time_range` with `step`, or `evaluation_time`. One row per series. See [Evaluating PromQL](#evaluating-promql) | `data` (`series`, `samples`), `scope` (the instant or range, the step alignment, the lookback), `coverage`, `accuracy`, `budget` | `max_rows` 200 series, ceiling 5,000; past it the result is `ok_bounded` with no cursor; a range of at most 11,000 steps; `max_response_bytes` 512 KiB default, 256 KiB floor, a series over it cut to a prefix of its samples | `unauthorized`, `missing_argument`, `invalid_argument`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `internal` |
| `ravel_search_logs` | Compiled to one SQL `SELECT` over `logs`. The shared attributes of a group are listed once per group. See [Searching logs](#searching-logs) | `data` (`__ravel_group`, `__ravel_group_attrs`, then the `logs` columns), `scope` (`predicates_applied`, `order_by`), `visibility`, `presentation`, `budget` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_get_trace` | Rows of the `spans` table, each with its depth in the span tree and whether its parent was returned. With `time_range`, only spans that start inside it are returned. See [Span tree](#span-tree) | `data` (`__ravel_depth`, `__ravel_parent`, then the `spans` columns), `scope`, `presentation`, `budget` | `max_rows` 200, ceiling 5,000; past it the result is cut, with status `ok_bounded`, `presentation.row_cap_hit` set and no cursor; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `invalid_argument`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_analyze_timeseries` | One row per series, reported as a heuristic. `max_bytes_scanned`, `max_segments` and `max_store_requests` are refused with `unsupported`. See [Analyzing a series](#analyzing-a-series) | `data`, `scope` (the range, the step alignment, the lookback), `coverage`, `accuracy` (`exact: false`, naming the heuristic), `presentation`, `budget` | `max_rows` 200 series, ceiling 5,000; past it the result is `ok_bounded` with no cursor; at most 1,000 matched series; a range of at most 11,000 steps; `max_response_bytes` 512 KiB default, 256 KiB floor; every row reports the method's minimum point count | `unauthorized`, `missing_argument`, `invalid_argument`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `internal` |

`ravel_capabilities` and `ravel_describe_data` return metadata only. No tool
emits an `evidence` block in this build, and no tool redeems an
`evidence_ref`; see [Evidence references](#evidence-references) for what each
tool does with one.

`ravel_get_trace` runs the same lookup as `GET /api/traces/<trace_id>`
(see [Fetching one trace by id](../guides/traces.md#fetching-one-trace-by-id)).
`time_range` is optional. Without it, the lookup resolves from time zero and
reads every folded part and the unsealed tail the tenant still retains, so its
cost grows with retention. With it, the bounds are kept exactly. Unlike the
HTTP route, where `start` and `end` only choose which objects are read, the
tool also drops every span that starts outside `time_range`. `include_logs:
true` is refused with `unsupported`; for a trace's logs, call
`ravel_search_logs` with a `trace_id` and a `time_range`.

#### Span tree

The tool adds two columns in front of the `spans` columns. They describe
the tree over the rows returned; the row order stays `(start_ts, span_id)`.

| Column | Value |
| --- | --- |
| `__ravel_parent` | `root`: the span has no parent id. `present`: a returned span has the parent's id. `missing`: no returned span has it, and the result is complete. `not_returned`: no returned span has it, and the result is not complete. `cycle`: following parents from this span leads back to it |
| `__ravel_depth` | how many returned ancestors the span has: 0 for a `root`, and 0 for a span whose parent is `missing` or `not_returned`. Null for a `cycle` span, and for a span whose ancestors lead into a cycle |

A result is complete when `presentation.row_cap_hit` and
`presentation.bytes_cap_hit` are false, `presentation.rows_omitted` is 0,
and `coverage.complete` is true. In any other result an absent parent is
`not_returned`, never `missing`: it may be among the rows the caps
dropped. A `missing` parent is not among the spans the lookup returned:
it was never stored, it is no longer retained, or, with a `time_range`, it
starts outside the window.

When two returned spans share a span id, the first in row order is the
parent: its children resolve to it, and their depth follows it.

The labels are computed after the row and byte caps, over the rows that
survive them. The byte cap is applied with both columns already filled
with the widest value each can take, and the real values are written
afterwards, so the two columns cannot push the response past the cap the
byte fit applied.

`next_steps` names the remedy:

- `widen_time_range`: a parent is `missing` and the call named a
  `time_range`. The parent may start outside it; widen or drop the window.
- `raise_max_rows`: a parent is `not_returned`. Raise `max_rows` (up to
  5,000) or narrow `time_range`.

Otherwise neither step is listed, including for a `missing` parent when
the call named no `time_range`.

The two column names carry the `__ravel_` prefix by convention; the
server does not reserve the prefix, so a declared attribute column with
the same name is not refused.

### Ravel Agents tools

Six more tools join `tools/list`, after the nine above, only when the server
is built with both the `mcp` and `agents` features and run with both `--mcp`
and `--agents`. In every other build or run `tools/list` is the nine-tool
catalog, byte for byte. Each call runs in process through the agents client
route it names, with the MCP request's own headers, so the route's resolver
and its `agents` capability check decide it: a principal without the
capability gets `unauthorized`. Every one of them is served. Each call is
bounded by its effective deadline, the `budget.effective.deadline_ms` its
envelope reports: a route that has not answered by then ends the call with
`deadline`. A `ravel_agent_send` that ends that way may or may not have
accepted its input, so it says the outcome is unknown and adds the
`resend_with_the_same_key` next step.

Only `ravel_agent_events` applies a row cap. Its envelope reports the page
size it used, `max_rows` held to 1,000, as both `presentation.max_rows` and
`budget.effective.max_rows`. The other five tools never cut their rows at
`max_rows`, so their envelopes, and every failed agents call, report
`presentation.max_rows` as 0 and carry no `max_rows` in `budget.effective`.

The subsections below are generated from the agents catalog. They appear on
this page whatever the build; the paragraphs above say when a server serves
them.

<!-- BEGIN GENERATED MCP AGENTS TOOLS -->
#### `ravel_agent_describe`

- Title: Describe agent
- Description: Ravel Agents: an agent's identity, state version, open turn and inbox depth, without its state
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | string | yes |  |
| `kind` | string | yes |  |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |

#### `ravel_agent_send`

- Title: Send to agent
- Description: Ravel Agents: accept one input into an agent's inbox and return its operation. A repeat with the same idempotency_key returns the same operation and accepts nothing new
- Annotations: `readOnlyHint: false`, `destructiveHint: false`, `idempotentHint: true`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `handler` | string | yes |  |
| `idempotency_key` | string | yes | Becomes the send's request id: a repeat with the same key answers the same operation and accepts nothing new. |
| `key` | string | yes |  |
| `kind` | string | yes |  |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `payload` | any JSON | no | The handler's input, any JSON value. |

#### `ravel_agent_operation`

- Title: Agent operation
- Description: Ravel Agents: an operation's status, and the effect blocking it
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `operation` | string | yes | The handle a send answered. |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |

#### `ravel_agent_state`

- Title: Agent state
- Description: Ravel Agents: an agent's state at its current version, open turn and inbox
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | string | yes |  |
| `kind` | string | yes |  |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |

#### `ravel_agent_events`

- Title: Agent events
- Description: Ravel Agents: an agent's events in order, one page after the cursor
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | string | yes |  |
| `kind` | string | yes |  |
| `cursor` | string | no | Resume after this cursor; omitted reads from the oldest event. |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |
| `max_rows` | integer | no | Events per page; default 200, at most 1,000. |
| `operation` | string | no | Only this operation's events. |

#### `ravel_agent_blocked_effects`

- Title: Blocked effects
- Description: Ravel Agents: an agent's effects in doubt and the reconcile request that unblocks each
- Annotations: `readOnlyHint: true`, `destructiveHint: false`, `openWorldHint: false`

| Argument | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | string | yes |  |
| `kind` | string | yes |  |
| `deadline_ms` | integer | no |  |
| `max_response_bytes` | integer | no | Bounds the whole serialized envelope; default 512 KiB, floored at 256 KiB. |

<!-- END GENERATED MCP AGENTS TOOLS -->

| Tool | Returns | Agents route | Bounds | Failure classes |
| --- | --- | --- | --- | --- |
| `ravel_agent_describe` | One row: agent id, incarnation, state version, content type, state size in bytes, active turn and mailbox depth, without the state itself | `GET /api/v1/agents/state` | `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `invalid_argument`, `deadline`, `unavailable`, `internal` |
| `ravel_agent_send` | Accept one input into an agent's inbox; the only tool that writes. `idempotency_key` is sent as the request id | `POST /api/v1/agents/send` | `max_response_bytes` 512 KiB default, 256 KiB floor; one row: `operation`, `accept_seq`, `duplicate`. A repeat with the same key answers the same operation with `duplicate: true` and accepts nothing new | `unauthorized`, `invalid_argument`, `deadline`, `unavailable`, `internal` (an `outcome_unknown` answer and a `deadline` both add the `resend_with_the_same_key` next step) |
| `ravel_agent_operation` | One operation's status, accept sequence and blocking effect | `GET /api/v1/agents/operation` | `max_response_bytes` 512 KiB default, 256 KiB floor; one row; a blocked operation's next step names the blocking effect | `unauthorized`, `invalid_argument`, `deadline`, `unavailable`, `internal` |
| `ravel_agent_state` | The agent's state, active turn and mailbox | `GET /api/v1/agents/state` | `max_response_bytes` 512 KiB default, 256 KiB floor; the route bounds the state itself by `--agents-max-state-bytes` | `unauthorized`, `invalid_argument`, `deadline`, `unavailable`, `internal` |
| `ravel_agent_events` | One page of the agent's events in `event_seq` order, optionally one operation's | `POST /api/v1/agents/events` | `max_rows` 200, at most 1,000; a full or byte-capped page carries `presentation.cursor`, the last event it kept | `unauthorized`, `invalid_argument`, `cursor_expired` (with a `restart_from_oldest_cursor` next step), `deadline`, `unavailable`, `internal` |
| `ravel_agent_blocked_effects` | Effects in doubt, each with the inspector's text and the two reconcile requests that unblock it | `GET /api/v1/agents/effects` | `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `invalid_argument`, `deadline`, `unavailable`, `internal` |

The `tools/list` result is 16,177 bytes with the nine tools and 24,066
bytes with all fifteen, against a budget of 24,576 bytes. A test pins both
figures exactly.

### Explaining a statement

`ravel_explain_query` explains SQL only, and runs no scan.

| Statement | Answer |
| --- | --- |
| fails SQL validation | `validation`, with the engine's text and a `next_steps` entry naming `ravel_query_promql` |
| not a `SELECT` | `unsupported` |
| names two tables | `invalid_argument` from the one-table rule, before the explain resolves anything |
| one `SELECT` over one table | the explain, compared against the effective budget |

The comparison pairs each bounded estimate with one budget:

| Estimate | Budget |
| --- | --- |
| `estimated_requests` | `max_store_requests` |
| `estimated_store_bytes` | `max_bytes_scanned` |
| admitted segments | `max_segments` |

Recent-exempt segments, from ingest hours the catalog has not folded, are
not admitted and do not count. When any estimate exceeds its budget the
answer is `budget_estimate_exceeds_ceiling`, and its message names the
factor to narrow by: the largest ratio of estimate to budget among the
ones that exceed.

An estimate component the server cannot bound is named in `warnings` as
unbounded, is never read as zero, and sets `estimate_is_upper_envelope` to
false.

- With no budget to compare against (`estimated_decompressed_bytes` on
  every table but `samples`), the call does not refuse.
- With a budget, the call refuses with `budget_estimate_exceeds_ceiling`,
  except on a Parquet table.
- On a Parquet table, `estimated_requests` and `estimated_store_bytes` are
  unbounded, because the manifests count a cold, full scan and an uncached
  footer or an evicted range costs more. The call does not refuse. A
  warning names each budget the explain did not check: the Parquet reader
  enforces `max_store_requests` and `max_bytes_scanned` at scan time and
  refuses the read that would cross either.

### Running a statement

`ravel_query_sql` checks the statement from its text, in this order, before
it reads anything:

| Statement | Answer |
| --- | --- |
| names two tables | `invalid_argument` from the one-table rule, with a `split_the_statement` next step |
| not a read-only `SELECT` | `unsupported` |
| fails SQL validation | `validation`, with the engine's text and a `next_steps` entry naming `ravel_query_promql` |
| cannot be paged: for example its own `LIMIT`, `OFFSET`, `FETCH` or `TOP`, an `ORDER BY` term that is not a projected column or may be NULL, or a float `ORDER BY` term | `invalid_argument` naming the planner's reason, with a `rewrite_the_statement` next step |
| no `ORDER BY`, and no projected row identity to order by (only `samples` has one: `ts` and `series_id`) | runs as written, unpaged |
| any other `SELECT` over one table | runs, paged |

A float `ORDER BY` term is refused because a NaN or infinite value has no
cursor position: the rows after it could not be reached. Order by another
column to page the same rows.

`time_range` filters rows, not only segments: a row whose timestamp is
outside the half-open window is not returned, even when its segment
overlaps the window.

A page holds at most `max_rows` rows. The tool appends a tiebreak to the
statement's `ORDER BY` where the table has one (`series_id` on `samples`)
and reports the whole order in `scope.order_by`. Under a total order the
cursor resumes after the page's last row. Under an order that is not
total, a warning says so, and the page ends before the last group of rows
equal under the order; see [Page order](#page-order).

A full page ends `ok_bounded` with no cursor, and a `next_steps` entry, in
these cases:

| Case | Next step |
| --- | --- |
| The statement has no order to page by | `add_an_order_by` |
| No complete group of equal rows fits the page | `narrow_time_range` |
| The last row's order values could not be read, or one is NaN or infinite | `narrow_time_range` |
| The cursor after the last row is over its 4 KiB bound, as a long string order value makes it | `narrow_time_range` |

A warning names the reason in each case. None of them is a failure.

A cursor is bound to the statement, `time_range` and the effective
`max_rows`; a call that changes any of them is refused as
`cursor_invalid`. A later page resolves at page one's instant and audits at
its own clock, as under [Redemption](#redemption).

### Searching logs

`ravel_search_logs` takes these filters, all optional and all combined
with `AND`:

| Input | Shape | Compiles to |
| --- | --- | --- |
| `predicates` | a list of `{"key", "op", "value"}`. `op` is `eq` or `ne` with one string `value`, `in` with a non-empty list of strings, or `exists` with no `value` | `"key" = v`, `"key" <> v`, `"key" IN (v, ...)`, `"key" IS NOT NULL` |
| `has_word` | a string holding at least one letter or digit | `has_word("body", 'term')`: the body holds the term's words as a run of whole tokens, case-insensitive |
| `severity` | `trace`, `debug`, `info`, `warn`, `error` or `fatal`, in either case | `"severity_num" >= n`, where `n` is 1, 5, 9, 13, 17 or 21: that level or above |
| `trace_id` | 32 hexadecimal characters, either case | `"trace_id" = X'...'` |

A predicate `key` must be a typed attribute column of the tenant's `logs`
table, or `trace_id` when it is an indexed key. The tool reads both lists
as `ravel_describe_data` reports them, at the call's own clock, and refuses
any other key as `invalid_argument` with a next step naming
`ravel_describe_data`. Each `value` must parse as the key's type: an `i64`
column takes a
decimal integer, a `bool` column `true` or `false`, a `bytes` column and
`trace_id` hexadecimal. `ne` follows SQL: a row where the key is null
matches neither `eq` nor `ne`.

Every check above runs before the statement does. The filters then compile
to `SELECT * FROM logs WHERE ... ORDER BY "ts", "observed_ts"`, which passes
the same one-table rule and SQL validation as a `ravel_query_sql`
statement. A caller value enters the statement only as a quoted literal,
never as statement text. `scope.predicates_applied` lists the compiled
conditions and `scope.order_by` the order. `time_range` filters rows, as on
`ravel_query_sql`.

The result's first two columns are added by the tool, and the `logs`
columns follow in table order. The two tool columns carry the `__ravel_`
prefix of the `logs` schema's own synthetic names, so a typed
attribute column named `group` or `group_attrs` is returned once, under its
own name, beside them:

| Column | Value |
| --- | --- |
| `__ravel_group` | the row's attribute group, numbered from 0 in order of first appearance on the page. A group is the rows whose `attrs` have the same set of keys |
| `__ravel_group_attrs` | on the first row of each group, the attributes (key and value) every row of the group on the page carries, listed once; null on the group's other rows |
| `attrs` | the row's attributes that are not in its group's `__ravel_group_attrs` |

`attrs` holds the resource attributes beside the record's own. Groups are
computed per page. When the byte cap drops rows from the end of a page,
`__ravel_group_attrs` is still carried by every kept row of the group, but
it may list fewer attributes than the kept rows share. Timestamps are
nanosecond strings, as everywhere in the envelope.

The order `(ts, observed_ts)` is not total. A page never ends inside a
group of rows equal on that pair; see [Page order](#page-order). A page on
which no complete group fits returns the rows it has, `ok_bounded`, with no
cursor and a `narrow_time_range` next step.

A cursor is bound to the filters, `time_range` and the effective
`max_rows`; a call that changes any of them is refused as
`cursor_invalid`. A later page resolves at page one's instant, as under
[Redemption](#redemption).

### Evaluating PromQL

`ravel_query_promql` checks its arguments before it reads anything:

| Arguments | Answer |
| --- | --- |
| `evaluation_time` alone | an instant evaluation at that time |
| `time_range` and `step` | a range evaluation |
| both modes, neither, `time_range` without `step`, or `step` without `time_range` | `invalid_argument`, with a next step naming the two modes |
| a `step` without a unit (`30`), zero, or one that does not parse | `invalid_argument` |
| `time_range.end_ns` not after `time_range.start_ns` | `invalid_argument` |
| a range of more than 11,000 steps, the engine's own limit | `invalid_argument`, with a next step naming the smallest whole-millisecond step that fits, or to narrow `time_range` |
| an instant, a bound or a step that is not a whole millisecond | `invalid_argument` naming the figure in nanoseconds |
| an `evidence_ref` | `unsupported` |

The engine evaluates at millisecond resolution. A time with a
sub-millisecond part is refused rather than rounded, because rounding
would evaluate a time the call did not name.

`time_range` is half-open, as on every other tool. The steps are
`start_ns + k * step` for every `k` with `start_ns + k * step < end_ns`,
so `end_ns` itself is not evaluated. `scope.time_range` reports the range
in nanosecond strings; an instant evaluation reports the instant as both
bounds. `scope.predicates_applied` carries the query, a line stating the
instant or the step alignment and the last step evaluated, and the 5 m
lookback: an instant selector at step `t` reads the latest sample in
`(t - 5m, t]`.

A vector or matrix result is one row per series, with two columns:
`series`, the label set, and `samples`, an object of parallel arrays.
`samples.ts_ns` holds nanosecond strings, `samples.value` follows the
precision rule (`NaN`, `+Inf` and `-Inf` as strings, the sign of zero
kept), and `samples.histogram` is present only when a step of the series
is a native histogram, whose `value` is then `null`. A scalar or string
result is one row with one `value` column.

`max_rows` counts series. Past it, the first `max_rows` series are
returned whole, `data.row_count` is the number of series the evaluation
produced, `presentation.rows_omitted` counts the rest, and the status is
`ok_bounded`. A PromQL result has no order a cursor could resume, so the
tool mints none; narrow the selector to see the others.

`max_response_bytes` bounds the samples too. When the series do not all
fit, the series that fit whole come back whole, in order. The first that
does not comes back with a prefix of its samples: `ts_ns`, `value` and
`histogram` cut to the same length, and `samples.samples_omitted`, a
string count, giving the samples left out. `samples` stays an object; it
is not cut to text as other oversized cells are. That series is dropped
instead when not one of its samples fits and a series before it was
kept. `presentation.cells_truncated` counts the cut series,
`presentation.rows_omitted` also counts every series dropped, the status is
`ok_bounded`, and a next step says to widen `step` or narrow `time_range`
for fewer samples per series, or to narrow the query for fewer series.

A result that skipped part of its coverage, such as an unreachable
federated cluster, is refused as `unavailable` unless the call sets
`allow_partial_coverage: true`, the class the HTTP route's 503 maps to.
The refusal carries a next step naming the argument. With consent the
result is returned with `coverage.partial: true`.

The log metric names `ravel_log_lines` and `ravel_log_bytes` are
queryable here as on `/api/v1/query`; see
[the query guide](../guides/query.md).

### Analyzing a series

`ravel_analyze_timeseries` evaluates `query` over `time_range` at `step`,
then runs `op` over each series of the result. Its range arguments follow
the [Evaluating PromQL](#evaluating-promql) rules exactly, through the same
check, and are refused with the same messages before anything is read: a
step with a unit, whole milliseconds, the end after the start, and at most
11,000 steps. An `op` other than `change_point` or `summary` is
`invalid_argument`. A query that does not evaluate to a range vector is
`invalid_argument`, and one matching more than 1,000 series is
`budget_exceeded`.

The call goes through the same service method as
`POST /api/v1/analytics`, so admission, the deadline clamp, the usage
record and the query audit event (language `analytics`) are the route's.
`scope` reports the range, the step alignment and the lookback as on
`ravel_query_promql`: each step reads the latest sample in `(t - 5m, t]`,
so the analysis is over the step values, not every stored sample.

The result is one row per series. Both ops are reported as heuristics:
`accuracy.exact` is `false`, and `accuracy.approximation` names the
method, its minimum point count, and how many of the series were
downsampled. Every row carries `series` (the label set), `points` (the
non-NaN step values analyzed), `nan_excluded`, `downsampled`, `min_points`
and `too_few_points`. A series with fewer than `min_points` points is a
row with `too_few_points: true` and null figures; the call does not fail.

| `op` | Figures | `min_points` | Downsampled |
| --- | --- | --- | --- |
| `change_point` | `kind` (`spike`, `dip`, `step_change`, `trend_change`, `distribution_change`, `stationary` or `indeterminable`), `ts_ns` (the change's timestamp, a nanosecond string, null when none was found), `score` | 22 | over 2,000 points, bucket-averaged to 2,000 before the search |
| `summary` | `median`, `mad`, `stddev`, `variance` (population) | 1 | never |

The figures follow the precision rule: `NaN`, `+Inf` and `-Inf` as
strings, the sign of zero kept.

`max_rows` counts series, as on `ravel_query_promql`: past it the first
`max_rows` rows are returned, `data.row_count` is the number of series
analyzed, and the status is `ok_bounded` with no cursor.

The tool takes no partial-coverage consent. A read that skipped part of
its coverage, such as an unreachable federated cluster, is refused as
`unavailable`, with next steps to retry or to read the partial series
through `ravel_query_promql` with `allow_partial_coverage: true`: an
analysis over a subset of the series is not marked anywhere in its rows.
The scan budgets `max_bytes_scanned`, `max_segments` and
`max_store_requests` are refused with `unsupported`, because the analytics
service call takes no per-request scan budget; the deadline and the query
engine's configured limits bound the read, as on `POST /api/v1/analytics`.

### Which list `ravel_find_labels` returns

| Arguments | List |
| --- | --- |
| `label_name: "__name__"`, with or without a selector | metric names. The two log pseudo-metrics are included when there is no selector or the selector names one of them |
| any other `label_name`, with or without a selector | the values of that label |
| `selector` only | the label names of the series the selector matches |
| neither | refused with `missing_argument` before any read |

A warning on every answer names which list ran, and says that the list is
observed from data over the window rather than declared, complete for the
window unless `coverage.partial` is true.

### Filtering a label list

`ravel_find_labels` takes an optional `filter`, a case-sensitive substring
match over the strings that the call is about to return.

- The filter applies after the list is produced and before the page cap. A
  returned page is complete for that filter, and a truncation report means
  that more matches exist.
- `scope.predicates_applied` reports the filter.
- The match is case-sensitive because label names and values are exact byte
  strings.
- An empty `filter` string is `invalid_argument`, not a request to match
  everything.
- A filter does not stand in for a selector or a label name. Those bound
  which data the call resolves, and a filter bounds only the output. A call
  that carries a filter and neither of the other two is still refused with
  `missing_argument`.

The page cap that follows the filter is the byte cap. This tool advertises
no `max_rows`. `max_response_bytes` bounds a page, 512 KiB by default with a
256 KiB floor. Every string that matches the filter and fits inside the byte
cap is on the page.

| Page | Report |
| --- | --- |
| The cap stopped the page short | `presentation.bytes_cap_hit` is set, `presentation.rows_omitted` counts what did not fit, `data.row_count` is the full match count, and the status is `ok_bounded`, not `ok`. |
| `bytes_cap_hit` is false | The page is the complete match set for that filter over the requested window. |

## The envelope

Every tool result carries this shape, on success and on failure. See
[the MCP server guide](../guides/mcp-server.md#the-result-envelope) for the meaning
of each field.

```json
{
  "status": "ok | ok_bounded | ok_page | error",
  "failure": null,
  "data": {"columns": [], "rows": [], "row_count": 0},
  "plan": null,
  "scope": {"signal": "", "table": "", "time_range": {}, "predicates_applied": [], "order_by": []},
  "ids": {"query_id": "", "audit_ref": ""},
  "visibility": {"snapshot_id": "", "ingest_watermark_hour": "", "pinned": false, "min_commit_tokens_applied": []},
  "coverage": {"complete": true, "partial": false, "fragments": [], "unindexed_predicates": []},
  "accuracy": {"exact": true, "approximation": null, "lower_bound_count": false},
  "presentation": {"max_rows": 200, "row_cap_hit": false, "bytes_cap_hit": false, "rows_omitted": 0, "cells_truncated": 0, "metadata_elided": 0, "entries_truncated": 0, "scalars_truncated": 0, "effective_max_response_bytes": 524288, "floor_applied": false, "cursor": null},
  "budget": {"effective": {}, "actual": {}, "estimate": {}, "estimate_is_upper_envelope": true},
  "evidence": [{"ref": "", "covers": "data.rows", "blake3_256": ""}],
  "warnings": [],
  "next_steps": [{"action": "", "detail": ""}]
}
```

Integers and timestamps travel as JSON strings for every value, whatever
its magnitude, so parse them as strings.

A time input, such as a `time_range` bound, is either an RFC 3339 timestamp
in UTC (ending in Z or +00:00) or a decimal nanosecond epoch, both as
strings. A timestamp with any other offset is refused as
`invalid_argument`.

`visibility.min_commit_tokens_applied` echoes each token in the wire form the
caller sent: on a SQL call a token qualified by table reads `<table>:<token>`.

### The byte cap

`max_response_bytes` bounds the serialized size of this whole envelope, not
only `data.rows`. When the envelope exceeds the cap:

- The server drops rows from the end of `data.rows` until the envelope fits.
- `data.row_count` stays at the true count.
- The server sets `presentation.bytes_cap_hit` and
  `presentation.rows_omitted`.
- The server always keeps at least one row when the query produced one. It
  shortens the cells of that row under a per-cell budget and does not drop
  the row.
- A number, a timestamp, a boolean, and a hex-encoded binary id never
  shorten. A string or a structured value that exceeds the per-cell budget
  is cut to that budget.

Three counters say what the fit removed outside `data.rows`:

| Counter | Counts |
| --- | --- |
| `metadata_elided` | List entries dropped because their list was over its count bound, plus the cursor when it was dropped for being over its own bound. |
| `entries_truncated` | Entries kept but cut because the entry was over its own size bound. A cut entry carries a truncation marker. |
| `scalars_truncated` | Scalar cuts. Sub-bounded scalars are cut to their own bounds first. The allowance pass then cuts `plan`, the failure message, and the budget values. |

The cursor carries its own 4 KiB bound, separate from the scalar allowance.
A cursor over its bound is never cut, because cutting a token breaks its
authentication code. It is a server defect, and the server drops the cursor.

| Result | Report of the dropped cursor |
| --- | --- |
| Carries no other failure | The defect is the failure, and its class is `internal`. |
| Already failed | The original class, message, and counter stay, because they say why the call failed. `metadata_elided` and a warning that names the bound report the dropped cursor. |

`ravel_query_sql` is the exception. Its cursor carries the page's own order
values, so a long string value can put it over the bound without any server
defect. It measures the cursor when it mints one, and a page whose cursor
would be over the bound ends `ok_bounded` with no cursor, a warning, and a
`narrow_time_range` next step (see [Running a statement](#running-a-statement)).

### The row cap

The first-row guarantee applies to the byte cap. It does not apply to
the row cap. When the equal-group rule leaves no complete group inside
the row cap, the server returns the rows it has, up to the row cap.
The status is `ok_bounded`. The server mints no cursor. `next_steps`
names narrowing `time_range` as the fix.

A result with rows and `ok_bounded` means more rows exist and the
server minted no cursor. An empty result is `ok` with `row_count` 0.

## Snapshot identity and freshness

### `visibility.snapshot_id`

`visibility.snapshot_id` is a hash over seven inputs:

- the tenant hash
- the signal
- the half-open time range
- the minimum commit-token watermark
- the erasure predicates pending at resolve time
- the typed attribute columns declared
- the instant the resolve ran at

Two calls that resolve the same seven report the same `snapshot_id`. The id
identifies those inputs, not the set of segments that they resolved to. A
later call that reports the same value is not a promise that it read the same
objects.

Each of the seven avoids a collision across calls that are not the same
resolve:

- Without the tenant hash, two tenants that ask the same question of disjoint
  data report one id.
- Without the resolve instant, two calls a day apart over the same historical
  window report one id while they read different segments.

A cursor pins the same resolve inputs plus its keyset position, so every
page of one cursor sequence reports one id. A redemption resolves at the
instant the cursor was minted, not at the redeeming call's clock.

### `visibility.ingest_watermark_hour`

`visibility.ingest_watermark_hour` is the greatest ingest hour bucket among
the segments that the call's snapshot resolved to. It travels as the decimal
unix hour (whole hours since the epoch) in a JSON string, like every other
integer in the envelope. It is not `YYYYMMDDHH`, and it is not the
`YYYYMMDDTHH` text that appears in object keys.

It is an ingest-time bound, so no client clock moves it. It is not the
catalog's fold watermark. `watermark_hour` unqualified is the fold watermark
everywhere else in Ravel, and this field is a different quantity.

The fold watermark is a cost boundary. A resolve serves hours at or below it
from snapshot parts and lists everything above it live, so a query routinely
reads data that the fold has not reached. The fold watermark also lags an
acknowledged write by around 2 h 25 m. The parts add up to 2 h 25 m 30 s:

- up to 1 h from the write to the end of its ingest hour
- the 1 h flush lifetime
- the 5 m skew allowance
- the 15 m fold margin
- the fold's 5 m interval
- the 30 s HEAD cache

A resolve that returned no segments has no greatest ingest hour to report.
It reports the field empty and says which of two reasons applies in
`warnings`:

| Case | Warning |
| --- | --- |
| An empty resolve | `visibility.ingest_watermark_hour is absent: this operation resolved no segments` |
| A tool that never measures freshness | `visibility.ingest_watermark_hour is not reported by this operation` |

An empty resolve is a result, not a gap in the tool.

Event-time bounds are a different quantity. `coverage` reports what the data
covers in event time.

## Failure classes

| Class | Meaning |
| --- | --- |
| `unauthorized` | The credential does not resolve to a tenant. |
| `invalid_argument` | An argument is well-formed but not acceptable as given: two mutually exclusive fields set together, a value out of range, an empty `filter` string, a label list bounded by neither a selector nor a label name. |
| `missing_argument` | A required argument, most often a time input, was not sent. |
| `validation` | The query engine rejected the statement itself. The message is the engine's own text, safe to show. |
| `unsupported` | The request names a construct or an operation the tool does not implement. For `ravel_query_sql` this also covers a result column the JSON encoding cannot hold: the message is `column "<name>" of type <arrow type> cannot be encoded as JSON: <reason>; cast the column in the query to a type JSON can hold, or read it over /api/v1/sql with the Arrow IPC format`, and never carries the value. |
| `budget_estimate_exceeds_ceiling` | `ravel_explain_query` found the statement's estimated cost above the effective budget. The failure names the factor to narrow by. |
| `budget_exceeded` | A running call passed one of its budgets. The failure names the counter that tripped. |
| `deadline` | The call passed its effective deadline. |
| `unavailable` | A transient storage or catalog fault. Retryable. |
| `snapshot_invalidated` | The pinned snapshot was invalidated by concurrent maintenance. Retryable once. |
| `cursor_expired` | The cursor's minting process or its deadline is gone, or an erasure it did not pin now reaches its signal and time range. |
| `cursor_invalid` | The cursor does not match the tenant, the tool, or the arguments presented with it. |
| `internal` | A server-side fault, including corrupt stored data. Not retryable. The message is fixed and carries no storage detail. |

A failure is a tool result with an error marker set, not a transport-level
error, so the calling model can read `next_steps` and correct itself.
Malformed JSON-RPC and an unknown tool name are the only cases answered at
the protocol level instead.

## Cursor rules

### What a cursor carries

A cursor is a token with a keyed message authentication code. The server
mints it fresh for each call and does not store it. It carries:

- the tenant hash
- the tool name
- a hash of the arguments
- the signal
- the half-open time range
- the minimum commit-token watermark that the page was resolved against
- which erasure predicates were pending
- a 32-byte digest of the typed attribute columns that were declared
- the position to resume from

It pins a snapshot by these resolve inputs, not by enumerating segments.

The typed attribute columns are pinned as a digest, not as a list, so the
token's size does not depend on how many columns a tenant declares. The digest is
taken over the set of (key, type) pairs: the order the columns are listed
in and any repeats do not change it. The pending erasure predicates are
carried whole, because redemption asks of each predicate in force whether
the cursor pinned it, and a digest cannot answer that.

### Redemption

A cursor stays valid until the earlier of the call's remaining deadline
and the protection horizon minus the grace period.

Redeeming a cursor does two things. The server holds nothing in between
calls.

1. It resolves the snapshot against the pinned watermark, not against a
   fresh observation. This makes the re-resolve deterministic. The resolve
   runs at the instant the cursor was minted, not at the redeeming call's
   clock. The page's typed attribute column and indexed key lookups and its
   query audit record's timestamp use the redeeming call's own clock.
2. It re-executes the original statement against that snapshot with a keyset
   predicate.

Redemption never compares the pinned watermark against a watermark observed
later. Commit tokens from different writers have no ordering between them to
compare. Whether a pinned token is still satisfiable is the catalog's answer
at resolve time.

Only the process that minted a cursor can redeem it, so a load balancer
needs sticky routing to a paging client. A tampered or wrong-tenant token
fails with `cursor_invalid`.

### Expiry

Redemption refuses a structurally valid, correctly bound cursor with
`cursor_expired` in three cases. In each case the caller re-runs the query.

- The effective deadline of the cursor has passed. That deadline is already
  clamped to the protection horizon minus the grace period. So this is also
  the check for a compaction having become free to take the pinned data
  apart.
- An erasure predicate is in force that the cursor did not pin, over the
  cursor's own signal, whose event-time window overlaps the cursor's time
  range. A windowless predicate overlaps every range. The overlap is judged
  on the signal and the range only, never on the predicate's matchers,
  because a cursor holds no records to match.
- The typed attribute columns the redeeming resolve observed hash to a
  different digest than the pinned one: a column was added, removed,
  renamed or retyped. The same columns in another order do not expire it.

Those are the three cases that a redemption can detect. An erasure that arrives
after a cursor is minted and finishes before it is redeemed is in neither
set, so no check sees it. That case stays empty only while a cursor cannot
outlive an erasure:

| Quantity | Bound | Under the defaults |
| --- | --- | --- |
| Cursor lifetime | At most the protection horizon minus a 24 h grace | 1 h 05 m under the default horizon |
| Erasure completion | Cannot finish before its seal wait | 4 h 05 m under the default ingest lag |

An operator who raises the protection horizon lengthens the first without
moving the second. The grace that this server subtracts is a fixed 24 h. A
30 h horizon leaves a 6 h cursor lifetime, above the 4 h 05 m floor. Page two
can then come from a snapshot that an erasure has changed.

### Page order

`ravel_query_sql` mints a cursor only when the statement's `ORDER BY`,
plus a tiebreak the tool appends, is a total order over the projection.
When the tiebreak is not unique, the equal-group rule applies as for
`ravel_search_logs`. A page never ends inside a group of equal tuples.
The cursor points at the last complete group. Cursor paging continues.

`ravel_search_logs` orders by `(ts, observed_ts)`. That pair is not
unique, because `logs` rows carry no row identity and ingest is
at-least-once. If a group of rows equal on the pair does not fit on the
page whole, the tool drops the whole group from the page, and the cursor
resumes after the last complete group. To request the second page, pass the
cursor from `presentation.cursor` back as `cursor`, with the same
arguments.

`ravel_get_trace` mints no cursor: it returns the trace's spans in one
page ordered by `(start_ts, span_id)`, and a trace larger than the row cap
comes back with status `ok_bounded` and `row_cap_hit` rather than paged.
Raise `max_rows` (up to 5,000) or narrow `time_range` to read more of it.

When no complete group fits in the row cap, the server returns the
rows it has, up to the row cap, with status `ok_bounded` and no cursor.

### `ravel_describe_data` pages

`ravel_describe_data` also pages with a cursor. The response carries
`presentation.cursor` when a page did not hold every row: more than 100
families, or rows that `max_response_bytes` cut. Request the next page
with the same signal and that cursor. The cursor follows the same codec,
tenant binding, and lifetime as every other cursor, and it is refused as
`cursor_invalid` by any other tool and by a call naming another signal.

Rows come in kind order: one `table` row (segments, committed rows, the
event-time coverage window, `families_total`), then one `column` row per
fixed column and typed attribute column, then one `indexed_key` row per
indexed key, then up to 100 `family` rows. Within each kind, rows sort by
name in byte order (`Zeta` sorts before `alpha`). The `table` row is on
page one only. `committed_rows` is summed from commit records, so it
counts rows before duplicate collapse and before any pending erasure: an
upper bound on the rows a query reads back. Counting those needs segment
reads, which this tool does not do. Page one therefore reports
`accuracy.exact: false` with `accuracy.approximation` set to
`committed_rows is an upper bound: summed from commit records, counted
before duplicate collapse and before pending erasure`. A later page
carries no `table` row and reports `accuracy.exact: true`. Logs and traces have no metric families, and give the reason in
`families_absent_reason` and in `warnings`. So does a metrics tenant with
no metric metadata record: the reason is `the tenant has no metric metadata
record`.

A metrics call whose metric metadata read failed is not an empty page. It
fails with class `unavailable`, which is retryable, and a message saying
the metric metadata read failed. The server caches the outcome of that
read per tenant. For 60 seconds after the call that read it, a retry
fails the same way without reading the store again. After that the
cached failure is stale: a call starts a background reread unless one is
already in flight, and still fails. A failed reread leaves the entry
stale, so the first call after each failed reread completes starts
another; one reread is in flight at a time. Once a read of the record
succeeds, calls answer the families. Logs and traces
do not read the metric metadata record and are unaffected.

The order is keyset, `scope.order_by` `["kind", "name"]`: the cursor's
position is the last served row's kind and name, and the next page
resumes after it. A cut inside the `column` rows resumes at the next
column, then serves the indexed keys and the families; a cut inside the
`indexed_key` rows resumes at the next key, then serves the families; a
cut inside the `family` rows resumes at the next family. It is not an
offset.

A later page reads its column, key and family lists afresh rather than
from page one:

- The typed attribute columns are pinned. A changed set expires the
  cursor, as below.
- Fixed columns are the server's compiled table schema. They change only
  with the server binary.
- The logs indexed keys are the tenant's current declaration, and the
  traces indexed key is `trace_id`. Neither is pinned.
- The family list is not pinned: each page reads the tenant's metric
  metadata record again.

When the column or key that a position names is no longer listed, the
cursor expires (`cursor_expired`) and the page serves no rows. A row added
after page one whose name sorts after the position appears on a later
page. One whose name sorts before the position is not returned by any
later page of that sequence, and nothing reports it.
`families_total` is the count in the record the page read, so it can
differ between pages of one sequence. A later page says so in `warnings`.

A logs cursor carries a fixed-size digest of the typed attribute column
set, not the set, so a tenant with thousands of typed attribute columns
pages like any other. A changed set expires the cursor, as below.

Page one's freshness watermark is pinned into that cursor. Every later
page reports the same value in `visibility.ingest_watermark_hour` and the
same `visibility.snapshot_id`. A later page runs its catalog resolve at
page one's instant. The pinned value is page one's measurement. A later page can
resolve over a superset of page one's segments, because the live listing
above the fold watermark picks up whatever has committed since. Everything
else on a later page runs on the call's own clock:

- the typed attribute column lookup and its 60-second freshness horizon;
- the indexed key lookup;
- the redemption deadline;
- the query audit record's timestamp and window end.

A column declared between pages is therefore seen once that horizon has
passed, and the page expires the cursor. A newer pending erasure on the
same signal, or a changed set of typed attribute columns, expires the
cursor (`cursor_expired`) and the page serves no rows. An erasure on
another signal does not. A resolve that found no segments reports no
watermark and sets `visibility.resolved_no_segments`.

`budget.actual` counts the catalog resolve only. The metric metadata
record read and the tenant configuration read are not counted, and every
page says so in `warnings`.

### Evidence references

No tool redeems an `evidence_ref` or emits an `evidence` block in this
build. What each tool does with an `evidence_ref` argument today:

| Tool | An `evidence_ref` argument |
| --- | --- |
| `ravel_query_sql`, `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace`, `ravel_analyze_timeseries` | Listed in the input schema, and refused with `unsupported` |
| `ravel_find_labels` | Not in the input schema, and refused with `invalid_argument` |
| `ravel_capabilities`, `ravel_describe_data`, `ravel_explain_query`, and the six Ravel Agents tools | Not in the input schema. Accepted and ignored, like any argument the schema does not list |

The rest of this section describes the design. Redeeming a reference
re-executes the tool with the reference's own arguments. The re-execution runs against the reference's
pinned snapshot while the pin is valid. The server then compares the
BLAKE3-256 digest of the canonical row bytes. The digest travels in the
evidence entry's `blake3_256` field, named for the function that produced
it.

After the pin expires, redemption re-executes fresh instead of using the
pin. It reports `pinned: false` and states whether the hash matched.
`cursor_invalid` and `cursor_expired` do not apply to an evidence reference
after its pin expires. A fresh re-execution runs instead of either
failure.

## Budget defaults and floors

| Budget | Default | Ceiling or floor | Caller can lower |
| --- | --- | --- | --- |
| `deadline` | set by the server | server ceiling | yes |
| `max_rows` | 200 | ceiling 5,000 | yes |
| `max_bytes_scanned` | server ceiling | server ceiling | yes |
| `max_store_requests` | server ceiling | server ceiling | yes |
| `max_response_bytes` | 512 KiB | floor 256 KiB | yes, raised to the floor if lower |
| `max_response_bytes` ceiling | 4 MiB | an operator may configure a lower one | no; a larger request clamps to it |
| cursor or evidence token length | 1 MiB | fixed | no; a longer token is refused unread |
| `presentation.cursor` in the envelope | 4 KiB | fixed | no; a cursor over its bound is dropped, as an `internal` failure on an otherwise-successful result and as a counted drop with a warning on one that already failed; `ravel_query_sql` mints none over it and ends the page `ok_bounded` |
| metric families per `ravel_describe_data` page | 100 | fixed | no |
| segments admitted for `ravel_find_labels` resolution | 2,000 | fixed | no |
| tokens in a SQL statement | 1,000 | fixed | no; a statement over it is refused before it is parsed, as `validation` |

The effective budget for a call is the smallest of the server's ceiling,
the tenant's ceiling, and the value the caller sent. A caller can never
raise a value past its ceiling.

## Protocol headers per revision

The server will serve both listed protocol revisions on the same `POST
/mcp` endpoint. It will re-authenticate every request against the bearer
credential regardless of revision. The `Origin` header will be validated
against the server's allowed-origins configuration on every request.

| Revision | Handshake | Headers this revision requires | Session |
| --- | --- | --- | --- |
| `2026-07-28` | none | `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` | none; each call stands alone |
| `2025-11-25` | `initialize` | `MCP-Protocol-Version` required, not `Mcp-Method` or `Mcp-Name` | `Mcp-Session-Id`, held in server memory |

A request that mismatches its own revision's header rule will fail before
the tool layer runs.
