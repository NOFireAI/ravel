# Query traces

Ravel serves traces three ways:

- `GET /api/traces/<trace_id>` returns every span of one trace.
- The `spans` SQL table answers any span query on `POST /api/v1/sql` and
  Flight SQL.
- The MCP tool `ravel_get_trace` returns one trace as a span tree (see the
  [MCP reference](../reference/mcp.md#ravel_get_trace)).

Ravel has no Jaeger or Tempo query API and no TraceQL. The examples use the
quickstart's token and listener; the [query guide](query.md) covers the
shared `POST /api/v1/sql` mechanics.

<a id="fetching-one-trace-by-id"></a>

## Fetch one trace

```sh
curl -s -H "Authorization: Bearer demo-token" \
  http://127.0.0.1:4318/api/traces/00112233445566778899aabbccddeeff
```

The response is the `POST /api/v1/sql` envelope (`status`, `data` with
`columns` and `rows`, and `stats`), one row per span in the row shape of
[the `spans` table](#the-spans-table). Send
`Accept: application/vnd.apache.arrow.stream` to get Arrow IPC instead.

`trace_id` is 32 hexadecimal characters, in either case. Any other value is
a 400 with a typed body, before any data is read:

```json
{"status": "error", "errorType": "bad_data", "error": "invalid trace id \"7a7a\": expected 32 hexadecimal characters"}
```

A trace the tenant does not hold is a 200 with zero rows, not a 404. The
route needs the `sql` cargo feature, which the published image carries.

The query string takes the same optional parameters as `POST /api/v1/sql`:

- `start` and `end` are Unix seconds as floats, parsed the same way.
- `timeout` is in seconds, and can only lower the server's deadline.
- `min_commit_token` can repeat. Each value is a token, or a token qualified
  with the one table the lookup reads, `spans:<token>`. Any other qualifier is
  a 400.

The route runs `SELECT * FROM spans WHERE trace_id = X'<trace_id>' ORDER BY
start_ts, span_id` through the SQL endpoint's handler, with the same
authentication, tenant, limits, deadline, and query audit record.

### Lookups without a window

With no `start`, the lookup resolves from time zero. It covers every folded
part and the unsealed tail that the tenant still retains, instead of the SQL
endpoint's one hour before `end`. An omitted `end` is the current time.

Explicit `start` and `end` are kept exactly and bound which objects are
listed. On this route they do not filter rows: a span that starts outside the
window can still be returned when its object is read. `ravel_get_trace`
resolves its `time_range` the same way and also filters rows, so it returns
only the spans that start inside the range.

The no-window rule covers this route and `ravel_get_trace` only. A
`POST /api/v1/sql` request that gives no window still reads one hour before
`end`, even when its statement pins a `trace_id`. To read a whole trace over
SQL, pass `"start": 0`.

A lookup without a window resolves every part of the tenant, so its cost
grows with retention: each covering part costs catalog reads, and each
segment no [key-index leaf](#key-index-leaves) covers costs a KEY_IDX probe.
When you know roughly when the trace ran, pass `start` and `end`.

## Query spans with SQL

Send a statement over `spans` to `POST /api/v1/sql`:

```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT trace_id, span_id, service_name, name, duration_ns FROM spans WHERE status_code = 2 ORDER BY start_ts LIMIT 20"}'
```

`POST /api/v1/sql` and Flight SQL can join `spans` with `samples`, `logs`,
`alerts` and `audit`, reading one snapshot per signal; a `min_commit_token`
on such a join is qualified with the table it bounds (`spans:<token>`). See
[Joins across tables](query.md#joins-across-tables) for the terms, including
the best-effort memory ceiling.

### Worked queries

One trace, its spans ordered by start time:

```sql
SELECT span_id, parent_span_id, name, service_name, start_ts, duration_ns,
       status_code
FROM spans
WHERE trace_id = '00112233445566778899aabbccddeeff'
ORDER BY start_ts;
```

All error spans in a one-hour window:

```sql
SELECT trace_id, span_id, service_name, name, duration_ns
FROM spans
WHERE status_code = 2
  AND start_ts >= TIMESTAMP '2026-08-19T00:00:00'
  AND start_ts <  TIMESTAMP '2026-08-19T01:00:00'
ORDER BY start_ts;
```

The slowest spans for one service:

```sql
SELECT trace_id, span_id, name, duration_ns
FROM spans
WHERE service_name = 'checkout'
  AND start_ts >= TIMESTAMP '2026-08-19T00:00:00'
ORDER BY duration_ns DESC
LIMIT 20;
```

Every span of one operation, slower than 500 ms:

```sql
SELECT trace_id, span_id, service_name, duration_ns, status_code
FROM spans
WHERE name = 'GET /cart'
  AND duration_ns > 500000000
  AND start_ts >= TIMESTAMP '2026-08-19T00:00:00'
ORDER BY duration_ns DESC;
```

The first query uses the `trace_id` fast path. The others combine a time
window with a status, service, name, or duration prune (see
[Which predicates prune](#which-predicates-prune)).

### Trace id literals

- For `=` and `!=`, a 32-character hex string and a 16-byte binary literal
  (`X'00112233...'`) plan identically. Both take the `trace_id` fast path and
  return the same rows.
- For `IN` and `NOT IN`, use one form for the whole list: every element a
  32-character hex string, or every element a binary literal. A list that
  mixes a hex string with any other element fails to plan.
- The hex-string spelling also plans against the `trace_id` column of the
  `logs` table, which is `FixedSizeBinary(16)` at the same width.

### The `spans` table

| column           | type                          | notes                                        |
|------------------|-------------------------------|----------------------------------------------|
| `trace_id`       | `FixedSizeBinary(16)`         | trace identity, never null                   |
| `span_id`        | `FixedSizeBinary(8)`          | span identity, never null                    |
| `parent_span_id` | `FixedSizeBinary(8)`          | null on a root span                          |
| `name`           | `Utf8`                        | span (operation) name                        |
| `start_ts`       | `Timestamp(ns)`               | span start                                   |
| `end_ts`         | `Timestamp(ns)`               | span end                                     |
| `status_code`    | `UInt8`                       | `0` Unset, `1` Ok, `2` Error                 |
| `status_message` | `Utf8`                        | null when the span set no message            |
| `attrs`          | `Map(Utf8, Utf8)`             | merged resource, scope, and span attributes  |
| `service_name`   | `Utf8`                        | from `attrs["service.name"]`, null when absent |
| `duration_ns`    | `Int64`                       | computed `end_ts - start_ts`, never stored   |
| `events`         | `List(Struct{ts_unix_nano Int64, name Utf8, attrs Map(Utf8, Utf8)})` | span events, null when the span carried none |
| `links`          | `List(Struct{trace_id FixedSizeBinary(16), span_id FixedSizeBinary(8), trace_state Utf8, attrs Map(Utf8, Utf8)})` | span links, null when the span carried none |

`status_code` is the stored OTLP byte. To read it as text, map the three
values in SQL, for example `CASE status_code WHEN 0 THEN 'Unset' WHEN 1 THEN
'Ok' WHEN 2 THEN 'Error' END`.

Query `duration_ns` like any other column (`WHERE duration_ns > 5e8`). The
reader answers it from the stored duration range of each block.

Span rows are at-least-once, like log rows: a client retry after a lost ack
can return a span more than once (see
[At-least-once rows](query.md#at-least-once-rows)).

### Span events

`events` holds one element per OTLP span event, in the order the span
recorded them. Each element carries:

- `ts_unix_nano`: the raw OTLP `time_unix_nano`, not a `Timestamp` column.
- `name`: the name of the event.
- `attrs`: a `Map(Utf8, Utf8)` in the same shape as the span's own `attrs`.

On a span that carried no events, the column is null, not an empty list.

Event attribute values are stringified the same way as span attributes:

- A string stays as it is.
- An integer or a boolean is spelled as in Prometheus text (`42`, `true`,
  `+Inf`).
- A byte value becomes lowercase hex.
- An array or a key-value-list value has no `Map(Utf8, Utf8)` spelling and is
  dropped from `attrs`.

The column is therefore a lossy projection. The raw bytes stay available as
`attrs['_events_raw']` for anything that the column cannot represent.

To filter on one event, expand the list with `unnest`:

```sql
SELECT trace_id, span_id, e['name'] AS event_name,
       e['attrs']['exception.type'] AS exception_type
FROM (SELECT trace_id, span_id, unnest(events) AS e FROM spans
      WHERE start_ts >= TIMESTAMP '2026-08-19T00:00:00')
WHERE e['name'] = 'exception';
```

`unnest` drops a span whose `events` is null, so the result holds one row per
event, not per span. `e['name']` reads a struct field.
`e['attrs']['exception.type']` reads a map key, and is null when the event
did not set that key.

### Span links

`links` holds one element per OTLP span link, in the order the span recorded
them. Each element carries:

- `trace_id` and `span_id` of the linked span: `FixedSizeBinary`, the same
  widths as the span-level `trace_id`/`span_id` columns.
- `trace_state`: empty, never null, when the link set none.
- `attrs`: a `Map(Utf8, Utf8)` in the same shape as the span's own `attrs`.

On a span that carried no links, the column is null, not an empty list.

Link attribute values are stringified the same way as event and span
attribute values. `attrs['_links_raw']` is the lossless hex form for the
value kinds that `Map(Utf8, Utf8)` cannot spell. Ravel decodes `links` from
that attribute, at one protobuf decode per row whenever a query selects the
column.

To filter on the attributes of one link, expand the list with `unnest`:

```sql
SELECT trace_id, span_id, l['trace_id'] AS linked_trace_id,
       l['attrs']['relationship'] AS relationship
FROM (SELECT trace_id, span_id, unnest(links) AS l FROM spans
      WHERE start_ts >= TIMESTAMP '2026-08-19T00:00:00')
WHERE l['attrs']['relationship'] = 'follows_from';
```

## Incomplete traces

A trace is incomplete when the query does not see all of its spans. This
happens in two ways:

- Some spans of the trace have not landed yet. Ravel acknowledges each span
  batch when the batch is durable. It never buffers a whole trace to wait for
  its siblings. A tail-sampling or completeness buffer belongs at the
  collector.
- The parent or root span of the trace is outside the queried time window. A
  child span can start and end inside a window whose parent started before
  it. A window query returns the spans in the window, not the whole trace.

To read a whole trace regardless of the window, use
[`GET /api/traces/<trace_id>`](#fetching-one-trace-by-id) without `start`
and `end`, or query by `trace_id` over SQL with `"start": 0`. Neither flags a
result as incomplete. The MCP tool `ravel_get_trace` does: it labels each
span whose parent is not among the returned spans `missing` or, when a cap
cut the result, `not_returned` (see the
[MCP reference](../reference/mcp.md#span-tree)). No route waits for a missing
root or sibling; each returns the spans in view.

## How Ravel stores and reads spans

A span is one record in an RSPAN segment object. Object storage is the only
durable copy. Records in a segment sort by `(trace_id, start_ts)`, so all
spans of one trace occupy a contiguous run of blocks.

Ingest routes each span to a shard by a BLAKE3 hash of its `trace_id`, so all
spans of one trace land on the same shard. A trace whose spans straddle an
online reshard activation can split across two shards; a lookup by id still
returns every stored span from wherever it landed.

The reader skips blocks that cannot match a query with:

- A skip index in each object. For each block it holds the `trace_id` range,
  the time interval (`start_ts` minimum and `end_ts` maximum), the
  `duration_ns` range, and a one-byte `status_mask`.
- A BLOOM section, with a per-block bloom filter over the tokens of
  `service.name` and span `name`.
- A block-local `service_name` column.
- A KEY_IDX section, which indexes the first 8 bytes of every trace id the
  object holds.

[docs/span-segment-format.md](../span-segment-format.md) is the normative
spec of the layout.

### Bounded trace reads

A `trace_id =` query lists the segments of every shard in the matched time
window, as any other query does; the ingest routing does not narrow the
listing. It then decodes only the blocks that can hold the trace:

1. Each object no [key-index leaf](#key-index-leaves) covers is probed in its
   KEY_IDX with small ranged reads: the object's last 8 KiB (its footer),
   then the index directory and one bucket.
2. An object KEY_IDX proves the trace absent from is not read further. With a
   footer that fits the 8 KiB suffix, that costs three requests, the first at
   most 8 KiB and all three under 16 KiB, and no skip index read.
3. An object that holds the trace reads its skip index, drops every block
   whose `trace_id` range excludes the trace or whose bounds miss the
   statement's other predicates, and reads only the kept block run.

A trace that shares those 8 bytes can pass the probe; the decoded rows are
matched against the full trace id, which removes it. The probe trades block
reads for index reads: a lookup reads fewer bytes, but it can issue more
requests, since every uncovered object pays those reads whether or not it
holds the trace.

Objects KEY_IDX ruled out are counted in
`stats.pruning.segmentsPrunedByKeyIndex`, and the KEY_IDX reads are reported
under the `keyIndex` entry of `stats.phases`.

### Key-index leaves

When the catalog is folded with key-index leaves (`ravel-server
--fold-key-index-leaves`, or `ravel-cli catalog fold --key-index-leaves`),
each folded part also carries a small index of the trace ids its objects
hold. A `trace_id =` lookup reads two ranged GETs of that index per part,
then opens only the objects that hold the trace, each at the blocks the
index names, without reading their KEY_IDX. Objects of the part that do not
hold the trace are not read at all. Objects no leaf covers (the unsealed
tail, a part folded without leaves, and the parts past the lookup's
ceiling of 1,024 parts or `1,024 x (1 + min(k, 64))` leaf GETs for `k`
distinct trace ids, 2,048 for one) fall back to the per-object KEY_IDX probe
above. For a `trace_id IN (...)` list, a part whose worst-case leaf GETs
exceed two GETs for each of its objects in the query's time window, what the
per-object probe would cost there, is also left to that probe without a leaf
GET.

Objects a leaf ruled out are also counted in
`stats.pruning.segmentsPrunedByKeyIndex`. The response's `stats.keyIndex`
reports what the lookup did: `partsCovered`, `partsUncovered`,
`partsOverTierOne` (the parts left to the per-object probe by that cost
test), `partsBeyondCeiling`, `leavesRejected`, `reservationsRefused`,
`leafGetRequests`, `leafWireBytes`, `keysProbed`, `skippedByBudget` and
`skipIdxDropped`. It is present only on a statement with a `trace_id`
filter. `keysProbed` is the number of distinct trace ids the part-level
check used: 1 for `trace_id = <literal>`, and the distinct count for a
`trace_id IN (...)` list of 2 to 1,024 distinct ids or the same ids written as
`trace_id = a OR trace_id = b`. [The query engine
reference](../query-engine.md) defines each figure. The process exports `ravel_query_key_index_parts_total` by
`signal` and `outcome`, and `ravel_query_key_index_leaf_gets_total` by
`signal`.

A `trace_id =` filter on the `logs` table probes log objects the same way
when the tenant indexes `trace_id` (see
[exact-key index fields](operations/configuration.md#exact-key-index-fields)).

### Which predicates prune

Pruning only decides which blocks the reader decodes. DataFusion always
applies the original `WHERE` predicate again above the scan, so a query never
returns a wrong row. A predicate that the reader cannot prune with still
filters rows exactly, and the query reads more blocks.

These predicates prune at the skip-index level:

- `trace_id = <literal>` selects the trace fast path. The literal is a 16-byte
  binary or a 32-character hex string. The reader drops every block whose
  `trace_id` range excludes the target.
- `start_ts` and `end_ts` range comparisons (`>=`, `>`, `<`, `<=`, `=`, and
  `BETWEEN`) fold into one time window. The reader drops every block whose time
  interval does not overlap it.
- `duration_ns` range comparisons fold into a duration window. The reader drops
  every block whose stored `duration_ns` range does not overlap it.
- `status_code = <literal>` (and `status_code IN (...)`, and a same-axis `OR`
  of `status_code` equalities) maps to status bits. The reader drops every
  block whose `status_mask` clears every requested bit. A `status_code = 2`
  query skips every block with no Error span.

These predicates prune at the bloom level:

- `service_name = <literal>` probes the block's `service.name` bloom. The
  reader skips a block whose bloom proves the token absent. A bloom never proves
  presence, so a positive probe still reads the block, and the predicate is
  always re-checked as a residual.
- `name = <literal>` probes the block's span-name bloom the same way.

Every other predicate filters rows only and prunes no blocks:

- The pruning predicates must be top-level `AND` conjuncts. A disjunction
  (`OR`) inside a conjunct drops that conjunct from pruning. The exception is
  a same-axis `OR` or `IN` list on `status_code` or `duration_ns`, which
  prunes as the union of its parts, and a disjunction whose every arm is
  `trace_id = <literal>`, which uses the key index exactly as the
  equivalent `trace_id IN (...)` list does. A `trace_id` disjunction with any
  other arm uses no key.
- Attribute equality on the `attrs` map (`attrs['k'] = 'v'`) does not prune.
  It is evaluated exactly over the merged map.
- A predicate over `events` or `links` does not prune. A query that selects
  either column turns off the columnar fast path: Ravel rebuilds `events`
  from the span's attribute pages, as it does `attrs`, and decodes `links`
  out of the merged `attrs` map. A single-node query that selects neither
  column never builds them.
- Under distributed execution, the column selection is not pushed to the
  workers. Each worker scans the full `spans` schema, `events` and `links`
  included, for every row that it returns. The coordinator drops the columns
  that the query did not select.
