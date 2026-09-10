# MCP tools reference

> The `POST /mcp` route exists in builds compiled with the `mcp` cargo
> feature, and is mounted when `--mcp` is passed; both are off by default.
> One tool is served today, `ravel_capabilities`. The other eight are in the
> catalog (so `tools/list` returns them) and refuse every call with a
> `method_not_found` protocol error until their bodies land; their entries
> below describe the surface as designed. `ravel_capabilities` reports the
> split at runtime: `tools.enabled` is what this deployment will actually
> run, `tools.catalogued` is what it declares but does not yet serve.

Every tool below is served over `POST /mcp`, using the Model Context
Protocol. A reader who knows which tool they want comes here for its exact
shape: what it takes, what it returns, its bounds, and the failure classes
it can produce. See [the agents guide](../guides/agents.md) for the
narrative walkthrough and the worked reasoning behind the tool grouping,
the envelope, and the empty-result checklist.

## Tools

<!-- mcp-tools:begin -->
| Tool | Purpose | Inputs | Output blocks used | Bounds | Failure classes |
| --- | --- | --- | --- | --- | --- |
| `ravel_capabilities` | Protocol and server version, served tools (`tools.enabled`) and declared-but-unserved ones (`tools.catalogued`), effective budget ceilings, dialect summaries, tenant hash, enabled signals | none | `data` | none; reads no data | `unauthorized`, `invalid_argument`, `internal` |
| `ravel_describe_data` | Effective schema, indexed keys, metric families, freshness watermark, coverage window, exact row counts where available | `signal`, an optional `cursor` | `data`, `scope`, `visibility`, `coverage`, `presentation` | 100 metric families per page | `unauthorized`, `invalid_argument`, `unavailable`, `deadline`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_find_labels` | Metric names, label names, or label values for a selector | a selector or a label name, an optional `filter`, `time_range` (required), `deadline_ms`, `max_response_bytes`. A call carrying neither a selector nor a label name is refused, whatever its filter | `data`, `scope`, `coverage` | 2,000 segments admitted for resolution | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_explain_query` | Validate a SQL or PromQL statement, estimate its cost, and return the plan shape. No scan runs. | `query`, `time_range`, `deadline_ms`, `max_response_bytes` | `data` (effective schema as `data.columns`, zero rows), `scope`, `budget`, `plan` (a text block that the explain tool alone populates) | compares the estimate against the effective budget | `unauthorized`, `invalid_argument`, `validation`, `unsupported`, `budget_estimate_exceeds_ceiling`, `internal` |
| `ravel_query_sql` | One `SELECT` over one table | `query`, `time_range` (required), `max_rows`, lowerable budgets, an optional `cursor`, an optional `evidence_ref` | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `validation`, `unsupported`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_query_promql` | Instant or range PromQL evaluation | `query`, either `time_range` and `step` or `evaluation_time` (exactly one mode), partial-coverage consent, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `coverage`, `accuracy`, `budget`, `evidence` | `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `internal` |
| `ravel_search_logs` | Typed log search compiled to SQL | indexed and typed-attribute predicates, `has_word`, severity, trace id, `time_range` (required), an optional `cursor`, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `visibility`, `accuracy`, `presentation`, `budget`, `evidence` | `max_rows` 200, ceiling 5,000; `max_response_bytes` 512 KiB default, 256 KiB floor | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `snapshot_invalidated`, `cursor_expired`, `cursor_invalid`, `internal` |
| `ravel_get_trace` | Spans of one trace id, the span tree, missing parents, orphans | `trace_id`, `time_range` (required), an optional logs pass, an optional `evidence_ref`, `deadline_ms`, `max_bytes_scanned`, `max_response_bytes`, `max_rows`, `max_segments`, `max_store_requests` | `data`, `scope`, `coverage`, `presentation`, `budget`, `evidence` | the shared budgets in the section below | `unauthorized`, `missing_argument`, `invalid_argument`, `budget_exceeded`, `deadline`, `unavailable`, `internal` |
| `ravel_analyze_timeseries` | `change_point` or `summary` over a PromQL range result | `query`, `time_range`, `step`, `op`, an optional `evidence_ref`, `deadline_ms`, `max_response_bytes` | `data`, `accuracy`, `budget`, `evidence` | reports the minimum point count the method needs | `unauthorized`, `missing_argument`, `invalid_argument`, `unsupported`, `deadline`, `unavailable`, `internal` |
<!-- mcp-tools:end -->

`ravel_capabilities` and `ravel_describe_data` return metadata only.
Neither tool accepts `evidence_ref` or emits an `evidence` block.
`ravel_find_labels` accepts no `evidence_ref` and emits no `evidence`
block either: nothing defines what a label list attests to, so the tool
mints no reference.

### Filtering a label list

`ravel_find_labels` takes an optional `filter`, a case-sensitive substring
match over the strings the call is about to return. It is applied after the
list is produced and before the page cap, so a returned page is complete
for that filter, and a truncation report means more matches exist. The
filter is reported in `scope.predicates_applied`.

The match is case-sensitive because label names and values are exact byte
strings. An empty `filter` string is `invalid_argument`, not a request to
match everything.

A filter does not stand in for a selector or a label name. Those bound
which data the call resolves; a filter bounds only the output. A call that
carries a filter and neither of the other two is still refused with
`invalid_argument`.

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

Integers and timestamps travel as JSON strings for every value, whatever
its magnitude, so parse them as strings.

Three counters say what the fit removed outside `data.rows`.
`metadata_elided` counts list entries dropped because their list was over
its count bound, plus the cursor when it was dropped for being over its own
bound. `entries_truncated` counts entries kept but cut because
the entry was over its own size bound; a cut entry carries a truncation
marker. `scalars_truncated` counts scalar cuts. Sub-bounded scalars are
cut to their own bounds first. The allowance pass then cuts `plan`, the
failure message, and the budget values. The cursor carries its own 4 KiB
bound, separate from the scalar allowance. A cursor over its bound is
never cut, because cutting a token breaks its authentication code; it is
a server defect, and the cursor is dropped. On a result that carries no
other failure, that defect is the failure and its class is `internal`. On
a result that already failed, the original class, message, and counter
stay: they say why the call failed, and an `internal` in their place would
send you to file a bug instead of retrying or narrowing. The dropped
cursor is then reported by `metadata_elided` and by a warning naming the
bound, so the defect is visible either way.

The first-row guarantee applies to the byte cap. It does not apply to
the row cap. When the equal-group rule leaves no complete group inside
the row cap, the server returns the rows it has, up to the row cap.
The status is `ok_bounded`. The server mints no cursor. `next_steps`
names narrowing `time_range` as the fix.

A result with rows and `ok_bounded` means more rows exist and the
server minted no cursor. A genuinely empty result is `ok` with
`row_count` 0.

## Snapshot identity and freshness

`visibility.snapshot_id` is a hash over the resolve inputs a cursor pins:
the signal, the half-open time range, the minimum commit-token watermark,
the erasure predicates pending at resolve time, and the typed attribute
columns declared. Two calls that resolve the same inputs report the same
`snapshot_id`. It identifies those inputs, not the set of segments they
resolved to, so a later call reporting the same value is not a promise
that it read the same objects.

`visibility.watermark_hour` is the greatest ingest hour bucket among the
segments the call's snapshot resolved to. It is an ingest-time bound, so
no client clock moves it, and it is not the catalog's fold watermark. The
fold watermark is a cost boundary: a resolve serves hours at or below it
from snapshot parts and lists everything above it live, so a query
routinely reads data the fold has not reached. It also lags an
acknowledged write by around 2 h 25 m under the default flush lifetime,
skew allowance, and fold margins, which would read as hours of staleness
beside an answer resolved a minute ago.

Event-time bounds are a different quantity. What the data covers in event
time is reported under `coverage`, not here.

## Failure classes

| Class | Meaning |
| --- | --- |
| `unauthorized` | The credential does not resolve to a tenant. |
| `invalid_argument` | An argument is well-formed but not acceptable as given: two mutually exclusive fields set together, a value out of range, an empty `filter` string, a label list bounded by neither a selector nor a label name. |
| `missing_argument` | A required argument, most often a time input, was not sent. |
| `validation` | The query engine rejected the statement itself. The message is the engine's own text, safe to show. |
| `unsupported` | The request names a construct or an operation the tool does not implement. |
| `budget_estimate_exceeds_ceiling` | `ravel_explain_query` found the statement's estimated cost above the effective budget. The failure names the factor to narrow by. |
| `budget_exceeded` | A running call passed one of its budgets. The failure names the counter that tripped. |
| `deadline` | The call passed its effective deadline. |
| `unavailable` | A transient storage or catalog fault. Retryable. |
| `snapshot_invalidated` | The pinned snapshot was invalidated by concurrent maintenance. Retryable once. |
| `cursor_expired` | The cursor's minting process or its deadline is gone, or an erasure it did not pin now reaches its signal and time range. |
| `cursor_invalid` | The cursor does not match the tenant, the tool, or the arguments presented with it. |
| `internal` | An unexpected fault. The message is fixed and carries no storage detail. |

A failure is a tool result with an error marker set, not a transport-level
error, so the calling model can read `next_steps` and correct itself.
Malformed JSON-RPC and an unknown tool name are the only cases answered at
the protocol level instead.

## Cursor rules

A cursor is a token with a keyed message authentication code, minted fresh
for each call rather than stored server-side. It carries the tenant hash,
the tool name, a hash of the arguments, the signal, the half-open time
range, the minimum commit-token watermark the page was resolved against,
which erasure predicates were pending, which typed attribute columns were
declared, and the position to resume from. It pins a snapshot by these
resolve inputs, not by enumerating segments.

A cursor stays valid until the earlier of the call's remaining deadline
and the protection horizon minus the grace period. Redeeming a cursor
resolves the snapshot against the pinned watermark rather than against a
fresh observation, which is what makes the re-resolve deterministic, and
re-executes the original statement against it with a keyset predicate; the
server holds nothing in between calls. The pinned watermark is that resolve
input and nothing more: redemption never compares it against a watermark
observed later, because commit tokens from different writers have no
ordering between them to compare, and whether a pinned token is still
satisfiable is the catalog's answer at resolve time.

That resolve also runs at the instant the cursor was minted, not at the
redeeming call's clock. A page sequence that re-listed at the current
instant would walk a moving snapshot, and pinning a watermark while
resolving against a later one would leave the pin decorative.

Redemption refuses a structurally valid, correctly bound cursor with
`cursor_expired` in two cases. The first is that its effective deadline has
passed: because that deadline is already clamped to the protection horizon
minus the grace period, this is also the check for a compaction having
become free to take the pinned data apart. The second is that an erasure
predicate is in force which the cursor did not pin, over the cursor's own
signal, whose event-time window overlaps the cursor's time range; a
windowless predicate overlaps every range. That overlap is judged on the
signal and the range only, never on the predicate's matchers, since a
cursor holds no records to match. In both cases the caller re-runs the
query. Only the process that minted a cursor can redeem it, so a load
balancer needs sticky routing to a paging client. A tampered or
wrong-tenant token fails with `cursor_invalid`.

Those are the two cases a redemption can detect. An erasure that arrives
after a cursor is minted and finishes before it is redeemed is in neither
set, so no check sees it. That case stays empty only while a cursor cannot
outlive an erasure: a cursor lives at most the protection horizon minus a
24 h grace, 1 h 05 m under the default horizon, and an erasure cannot
finish before its seal wait, 4 h 05 m under the default ingest lag. An
operator who raises the protection horizon lengthens the first without
moving the second, and the grace this server subtracts is a fixed 24 h. A
30 h horizon leaves a 6 h cursor lifetime, above the 4 h 05 m floor, and
page two can then come from a snapshot an erasure has changed.

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

Page one's freshness watermark is pinned into that cursor, and every later
page reports the same value in `visibility.watermark_hour`. A sequence that
re-measured the watermark per page would describe a moving state, with
nothing to say whether two pages differ because time passed or because the
data differs. The pinned value is page one's measurement: a later page may
resolve over a superset of page one's segments, because the live listing
above the fold watermark picks up whatever has committed since.

Every data tool except `ravel_find_labels` accepts an optional
`evidence_ref` input. Redeeming a reference re-executes the tool with the
reference's own arguments. The re-execution runs against the reference's
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
| `presentation.cursor` in the envelope | 4 KiB | fixed | no; a cursor over its bound is dropped, as an `internal` failure on an otherwise-successful result and as a counted drop with a warning on one that already failed |
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
