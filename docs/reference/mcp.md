# MCP tools reference

> This page describes the surface as designed. The tools ship behind the
> `mcp` cargo feature and the `--mcp` flag, both off by default. The
> `POST /mcp` route does not exist until the feature ships.

Every route below will be served over `POST /mcp`, using the Model Context
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
| `ravel_describe_data` | Effective schema, indexed keys, metric families, freshness watermark, coverage window, exact row counts where available | `signal`, an optional `cursor` | `data`, `scope`, `visibility`, `coverage`, `presentation` | 100 metric families per page | `unauthorized`, `invalid_argument`, `unavailable`, `deadline`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_find_labels` | Metric names, label names, or label values for a selector | a selector or a label name, plus a filter, `time_range` (required), an optional `evidence_ref`, `deadline_ms`, `max_response_bytes`. An unfiltered tenant-wide list is refused | `data`, `scope`, `coverage`, `evidence` | 2,000 segments admitted for resolution | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_explain_query` | Validate a SQL or PromQL statement, estimate its cost, and return the plan shape. No scan runs. | `query`, `time_range`, `deadline_ms`, `max_response_bytes` | `data` (effective schema as `data.columns`, zero rows), `scope`, `budget`, `plan` (a text block that the explain tool alone populates) | compares the estimate against the effective budget | `unauthorized`, `invalid_argument`, `validation`, `unsupported`, `budget_estimate_exceeds_ceiling`, `internal` |
| `ravel_query_sql` | One `SELECT` over one table | `query`, `time_range` (required), `max_rows`, lowerable budgets, an optional `cursor`, an optional `evidence_ref` | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_query_promql` | Instant or range PromQL evaluation | `query`, either `time_range` and `step` or `evaluation_time` (exactly one mode), partial-coverage consent, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `coverage`, `accuracy`, `budget`, `evidence` | `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `internal` |
| `ravel_search_logs` | Typed log search compiled to SQL | indexed and typed-attribute predicates, `has_word`, severity, trace id, `time_range` (required), an optional `cursor`, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_get_trace` | Spans of one trace id, the span tree, missing parents, orphans | `trace_id`, `time_range` (required), an optional logs pass, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `coverage`, `presentation`, `budget`, `evidence` | the shared budgets in the section below | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_analyze_timeseries` | `change_point` or `summary` over a PromQL range result | `query`, `time_range`, `step`, `op`, an optional `evidence_ref`, `deadline_ms`, `max_response_bytes` | `data`, `accuracy`, `budget`, `evidence` | reports the minimum point count the method needs | `unauthorized`, `missing_argument`, `invalid_argument`, `unsupported`, `deadline`, `unavailable`, `internal` |
<!-- mcp-tools:end -->

`ravel_capabilities` and `ravel_describe_data` return metadata only.
Neither tool accepts `evidence_ref` or emits an `evidence` block.

## The envelope

Every tool result carries this shape, whether it succeeds or fails. See
[the agents guide](../guides/agents.md#the-result-envelope) for what each
field means and how to read it.

```json
{
  "status": "ok | ok_bounded | ok_page | error",
  "failure": null,
  "data": {"columns": [], "rows": [], "row_count": 0},
  "plan": null,
  "scope": {"signal": "", "table": "", "time_range": {}, "predicates_applied": [], "order_by": []},
  "ids": {"query_id": "", "audit_ref": ""},
  "visibility": {"snapshot_id": "", "watermark_hour": "", "pinned": false, "min_commit_tokens_applied": []},
  "coverage": {"complete": true, "partial": false, "fragments": [], "unindexed_predicates": []},
  "accuracy": {"exact": true, "approximation": null, "lower_bound_count": false},
  "presentation": {"max_rows": 200, "row_cap_hit": false, "bytes_cap_hit": false, "rows_omitted": 0, "cells_truncated": 0, "metadata_elided": 0, "entries_truncated": 0, "scalars_truncated": 0, "effective_max_response_bytes": 524288, "floor_applied": false, "cursor": null},
  "budget": {"effective": {}, "actual": {}, "estimate": {}, "estimate_is_upper_envelope": true},
  "evidence": [{"ref": "", "covers": "data.rows", "blake3_256": ""}],
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

Three counters say what the fit removed outside `data.rows`.
`metadata_elided` counts list entries dropped because their list was over
its count bound. `entries_truncated` counts entries kept but cut because
the entry was over its own size bound; a cut entry carries a truncation
marker. `scalars_truncated` counts scalar cuts and the cursor drop.
Sub-bounded scalars are cut to their own bounds first. The allowance pass
then cuts `plan`, the failure message, and the budget values. If the
scalars are still over, the cursor is dropped and announced with a
warning and a next step. Cutting a token breaks its authentication code,
so it is dropped rather than cut.

The first-row guarantee applies to the byte cap. It does not apply to
the row cap. When the equal-group rule leaves no complete group inside
the row cap, the server returns the rows it has, up to the row cap.
The status is `ok_bounded`. The server mints no cursor. `next_steps`
names narrowing `time_range` as the fix.

A result with rows and `ok_bounded` means more rows exist and the
server minted no cursor. A genuinely empty result is `ok` with
`row_count` 0.

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
When the tiebreak is not unique, the equal-group rule applies exactly
as for `ravel_search_logs`. A page never ends inside a group of equal
tuples. The cursor points at the last complete group. Cursor paging
continues.

`ravel_search_logs` orders by a tuple of timestamp, observed timestamp,
trace id, span id, and a body hash. That tuple is not unique, because
`logs` rows carry no row identity and ingest is at-least-once. When a
page would end inside a group of equal tuples, the tool drops the whole
group from the page instead of splitting it, and the cursor resumes
after the group. The second page is requested by passing the cursor
from `presentation.cursor` back as `cursor`, with the same arguments.

`ravel_get_trace` orders by `(start_ts, span_id)`, which is unique, so
the equal-group rule never applies to it.

When no complete group fits in the row cap, the server returns the
rows it has, up to the row cap, with status `ok_bounded` and no cursor.

`ravel_describe_data` also pages with a cursor. The response carries
`presentation.cursor` when more families exist. Request the next page
with the same signal and that cursor. The cursor follows the same codec,
tenant binding, and lifetime as every other cursor.

Every data tool accepts an optional `evidence_ref` input. Redeeming a
reference re-executes the tool with the reference's own arguments. The
re-execution runs against the reference's pinned snapshot while the pin is
valid. The server then compares the BLAKE3-256 digest of the canonical row
bytes. The digest travels in the evidence entry's `blake3_256` field, named
for the function that produced it.
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
| metric families per `ravel_describe_data` page | 100 | fixed | no |
| segments admitted for `ravel_find_labels` resolution | 2,000 | fixed | no |

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
