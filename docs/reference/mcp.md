# MCP tools reference

> This page describes the surface as designed. The tools ship behind the
> `mcp` cargo feature and the `--mcp` flag, both off by default.

Every route below is served over `POST /mcp`, using the Model Context
Protocol. A reader who knows which tool they want comes here for its exact
shape: what it takes, what it returns, its bounds, and the failure classes
it can produce. See [the agents guide](../guides/agents.md) for the
narrative walkthrough and the worked reasoning behind the tool grouping,
the envelope, and the empty-result checklist.

## Tools

<!-- mcp-tools:begin -->
| Tool | Purpose | Inputs | Output blocks used | Bounds | Failure classes |
| --- | --- | --- | --- | --- | --- |
| `ravel_capabilities` | Protocol and server version, enabled tools, effective budget ceilings, dialect summaries, tenant hash, enabled signals | none | `data` | none; reads no data | `unauthorized`, `internal` |
| `ravel_describe_data` | Effective schema, indexed keys, metric families, freshness watermark, coverage window, exact row counts where available | `signal` | `data`, `scope`, `visibility`, `coverage` | 100 metric families per page | `unauthorized`, `invalid_argument`, `unavailable`, `deadline`, `internal` |
| `ravel_find_labels` | Metric names, label names, or label values for a selector | a selector or a label name, plus a filter; an unfiltered tenant-wide list is refused | `data`, `scope`, `coverage` | 2,000 segments admitted for resolution | `unauthorized`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_explain_query` | Validate a SQL or PromQL statement, estimate its cost, and return the plan shape. No scan runs. | `query`, `time_range` | `data` (effective schema as `data.columns`, zero rows), `scope`, `budget`, `plan` (a text block that the explain tool alone populates) | compares the estimate against the effective budget | `unauthorized`, `invalid_argument`, `validation`, `unsupported`, `budget_estimate_exceeds_ceiling`, `internal` |
| `ravel_query_sql` | One `SELECT` over one table | `query`, `time_range` (required), `max_rows`, lowerable budgets, an optional `cursor` | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_query_promql` | Instant or range PromQL evaluation | `query`, either `time_range` and `step` or `evaluation_time` (exactly one mode), partial-coverage consent | `data`, `scope`, `coverage`, `accuracy`, `budget` | `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `internal` |
| `ravel_search_logs` | Typed log search compiled to SQL | indexed and typed-attribute predicates, `has_word`, severity, trace id, `time_range` (required) | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_get_trace` | Spans of one trace id, the span tree, missing parents, orphans | `trace_id`, `time_range` (required), an optional logs pass | `data`, `scope`, `coverage`, `presentation`, `budget` | the shared budgets in the section below | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_analyze_timeseries` | `change_point` or `summary` over a PromQL range result | `query`, `time_range`, `step`, `op` | `data`, `accuracy`, `budget` | reports the minimum point count the method needs | `unauthorized`, `missing_argument`, `invalid_argument`, `unsupported`, `deadline`, `unavailable`, `internal` |
<!-- mcp-tools:end -->

## The envelope

Every tool result carries this shape, whether it succeeds or fails. See
[the agents guide](../guides/agents.md#the-result-envelope) for what each
field means and how to read it.

```json
{
  "status": "ok | ok_bounded | ok_page | error",
  "failure": null,
  "data": {"columns": [], "rows": [], "row_count": 0},
  "scope": {"signal": "", "table": "", "time_range": {}, "predicates_applied": [], "order_by": []},
  "ids": {"query_id": "", "audit_ref": ""},
  "visibility": {"snapshot_id": "", "watermark_hour": "", "pinned": false, "min_commit_tokens_applied": []},
  "coverage": {"complete": true, "partial": false, "fragments": [], "unindexed_predicates": []},
  "accuracy": {"exact": true, "approximation": null, "lower_bound_count": false},
  "presentation": {"max_rows": 200, "row_cap_hit": false, "bytes_cap_hit": false, "rows_omitted": 0, "cells_truncated": 0, "metadata_elided": 0, "effective_max_response_bytes": 524288, "floor_applied": false, "cursor": null},
  "budget": {"effective": {}, "actual": {}, "estimate": {}, "estimate_is_upper_envelope": true},
  "evidence": [{"ref": "", "covers": "data.rows", "sha256": ""}],
  "warnings": [],
  "next_steps": [{"action": "", "detail": ""}]
}
```

`max_response_bytes` bounds the serialized size of this whole envelope, not
only `data.rows`. When the envelope would exceed the cap, the server drops
rows from the end of `data.rows` until it fits, keeps `data.row_count` at
the true count, and sets `presentation.bytes_cap_hit` and
`presentation.rows_omitted`. The server always keeps at least one row when
the query produced one, shortening its cells under a per-cell budget rather
than dropping it. A number, a timestamp, a boolean, and a hex-encoded
binary id never shorten; a string or a structured value that exceeds the
per-cell budget is cut to that budget.

## Failure classes

| Class | Meaning |
| --- | --- |
| `unauthorized` | The credential does not resolve to a tenant. |
| `invalid_argument` | An argument is well-formed but not acceptable as given: two mutually exclusive fields set together, a value out of range, an unfiltered tenant-wide list. |
| `missing_argument` | A required argument, most often a time input, was not sent. |
| `validation` | The query engine rejected the statement itself. The message is the engine's own text, safe to show. |
| `unsupported` | The request names a construct or an operation the tool does not implement. |
| `budget_estimate_exceeds_ceiling` | `ravel_explain_query` found the statement's estimated cost above the effective budget. The failure names the factor to narrow by. |
| `budget_exceeded` | A running call passed one of its budgets. The failure names the counter that tripped. |
| `deadline` | The call passed its effective deadline. |
| `unavailable` | A transient storage or catalog fault. Retryable. |
| `snapshot_invalidated` | The pinned snapshot was invalidated by concurrent maintenance. Retryable once. |
| `cursor_expired` | The cursor's minting process, or its deadline, is gone. |
| `cursor_invalid` | The cursor does not match the tenant, the tool, or the arguments presented with it. |
| `internal` | An unexpected fault. The message is fixed and carries no storage detail. |

A failure is a tool result with an error marker set, not a transport-level
error, so the calling model can read `next_steps` and correct itself.
Malformed JSON-RPC and an unknown tool name are the only cases answered at
the protocol level instead.

## Cursor rules

A cursor is a token with a keyed message authentication code, minted fresh
for each call rather than stored server-side. It carries the tenant hash,
the tool name, a hash of the arguments, the position to resume from, and
the pinned snapshot, including which erasure predicates were pending and
which typed attribute columns were declared at mint time.

A cursor stays valid until the earlier of the call's remaining deadline
and the protection horizon minus the grace period. Redeeming a cursor
re-executes the original statement against the pinned snapshot with a
keyset predicate; the server holds nothing in between calls. Only the
process that minted a cursor can redeem it, so a load balancer needs
sticky routing to a paging client.

`ravel_query_sql` mints a cursor only when the statement's `ORDER BY`,
plus a tiebreak the tool appends, is a total order over the projection.
The same equal-group rule below applies when that tiebreak is not
unique. `ravel_search_logs` orders by a tuple of timestamp, observed
timestamp, trace id, span id, and a body hash. That tuple is not
unique, because `logs` rows carry no row identity and ingest is
at-least-once. When a page would end inside a group of equal tuples,
the tool drops the whole group from the page instead of splitting it,
and the cursor resumes after the group. `ravel_get_trace` orders by
`(start_ts, span_id)`, which is unique, so the equal-group rule never
applies to it. When no complete group fits in a page, the result is
`ok_bounded` with no cursor.

An evidence reference is a token of the same family, with a `sha256` of the
canonical row bytes added. Redeeming it while its pin is valid re-executes
against the pin and compares hashes. After the pin expires, redemption
re-executes fresh, reports `pinned: false`, and reports whether the hash
still matches.

## Budget defaults and floors

| Budget | Default | Ceiling or floor | Caller can lower |
| --- | --- | --- | --- |
| `deadline` | set by the server | server ceiling | yes |
| `max_rows` | 200 | ceiling 5,000 | yes |
| `max_bytes_scanned` | server ceiling | server ceiling | yes |
| `max_store_requests` | server ceiling | server ceiling | yes |
| `max_response_bytes` | 512 KiB | floor 256 KiB | yes, raised to the floor if lower |
| metric families per `ravel_describe_data` page | 100 | fixed | no |
| segments admitted for `ravel_find_labels` resolution | 2,000 | fixed | no |

The effective budget for a call is the smallest of the server's ceiling,
the tenant's ceiling, and the value the caller sent. A caller can never
raise a value past its ceiling.

## Protocol headers per revision

The server serves both listed protocol revisions on the same `POST /mcp`
endpoint and re-authenticates every request against the bearer credential
regardless of revision. The `Origin` header is validated against the
server's allowed-origins configuration on every request.

| Revision | Handshake | Headers this revision requires | Session |
| --- | --- | --- | --- |
| `2026-07-28` | none | `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` | none; each call stands alone |
| `2025-11-25` | `initialize` | `MCP-Protocol-Version` required, not `Mcp-Method` or `Mcp-Name` | `Mcp-Session-Id`, held in server memory |

A request that mismatches its own revision's header rule fails before the
tool layer runs.
