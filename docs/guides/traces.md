# Querying traces

Ravel stores every span in RSPAN objects and serves them through the `spans`
SQL table. Read the [query guide](query.md) first for the shared
`POST /api/v1/sql` mechanics.

## How Ravel stores spans

A span is one record in an RSPAN segment object. Object storage is the only
durable copy. Records in a segment sort by `(trace_id, start_ts)`, so all
spans of one trace occupy a contiguous run of blocks.

The reader uses these structures to skip blocks that cannot match a query:

- A skip index in each object. For each block it holds the `trace_id` range,
  the time interval (`start_ts` minimum and `end_ts` maximum), the
  `duration_ns` range, and a one-byte `status_mask`.
- A BLOOM section, with a per-block bloom filter over the tokens of
  `service.name` and span `name`.
- A block-local `service_name` column.

The full, normative spec of the on-disk layout is
[docs/span-segment-format.md](../span-segment-format.md).

## Bounded trace reads

A `trace_id =` query decodes only the blocks that can hold the trace. The
skip index drops every block whose `trace_id` range excludes the target.

Ingest routes each span to a shard by its `trace_id`. The router hashes the
`trace_id` with BLAKE3 and picks the shard from the hash. All spans of one
trace land on the same shard. In the objects of that shard they sort together
into a contiguous block run.

The catalog listing at query time does not use this routing. A `trace_id =`
query lists and opens the segments of every shard in the matched time window,
the same as any other query. Only the per-object decode is bounded.

Online resharding can move a tenant to a new shard count while spans still
arrive. A trace whose spans straddle a reshard activation can split across
two shards. A trace-by-id query still returns every stored span from
wherever it landed.

## The `spans` table

`POST /api/v1/sql` serves five tables from one endpoint: `samples`
(metrics), `logs`, `spans`, `alerts` (alert state transitions), and `audit`
(the audit trail). Each query targets one table. Ravel decides the target
from the `FROM` clause of the query before planning. It rejects a query that
names two real tables. The `samples` and `logs` tables follow the same
one-signal-per-query rule.

The `spans` table has these columns:

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

`duration_ns` is a computed column, `end_ts - start_ts`. Query it like any
other column (`WHERE duration_ns > 5e8`). The reader answers it from the
stored duration range of each block.

### Span events

`events` is a list of structs, one element per OTLP span event, in the order
that the span recorded them. Each element carries:

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
event, not per span. Both subscripts above are `get_field` lookups.
`e['name']` reads a struct field. `e['attrs']['exception.type']` reads a map
key, and evaluates to null when the event did not set that key.

### Span links

`links` is a list of structs, one element per OTLP span link, in the order
that the span recorded them. Each element carries:

- `trace_id` and `span_id` of the linked span: `FixedSizeBinary`, the same
  widths as the span-level `trace_id`/`span_id` columns.
- `trace_state`: empty, never null, when the link set none.
- `attrs`: a `Map(Utf8, Utf8)` in the same shape as the span's own `attrs`.

On a span that carried no links, the column is null, not an empty list.

Link attribute values are stringified the same way as event and span
attribute values. `attrs['_links_raw']` is the lossless hex form for the
value kinds that `Map(Utf8, Utf8)` cannot spell.

To filter on the attributes of one link, expand the list with `unnest`:

```sql
SELECT trace_id, span_id, l['trace_id'] AS linked_trace_id,
       l['attrs']['relationship'] AS relationship
FROM (SELECT trace_id, span_id, unnest(links) AS l FROM spans
      WHERE start_ts >= TIMESTAMP '2026-08-19T00:00:00')
WHERE l['attrs']['relationship'] = 'follows_from';
```

`events` reads the nested event columns of RSPAN v4. `links` has no separate
column on disk. On every RSPAN version, Ravel decodes `links` straight from
the plain `_links_raw` attribute. The column costs one protobuf decode per
row whenever a query selects it.

### Which predicates prune

All pruning is widen-only. DataFusion always applies the original `WHERE`
predicate again above the scan, so a query never returns a wrong row. Pruning
only decides which blocks the reader decodes. A predicate that the reader
cannot prune with still filters rows exactly, and the query reads more
blocks.

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
  presence, so a positive probe still reads the block.
- `name = <literal>` probes the block's span-name bloom the same way.

Every other predicate is a DataFusion residual only. It prunes no blocks.
Notes on these shapes:

- The pruning predicates must be top-level `AND` conjuncts. A disjunction
  (`OR`) inside a conjunct drops that conjunct from pruning. The exception is
  a same-axis `OR` or `IN` list on `status_code` or `duration_ns`, which
  prunes as the union of its parts.
- `service_name` and `name` equality push down, and DataFusion also always
  re-checks them as a residual, because a bloom is a false-positive filter.
- Attribute equality on the `attrs` map (`attrs['k'] = 'v'`) does not prune.
  It is evaluated exactly over the merged map.
- A predicate over `events` does not prune. A query that selects the column
  turns off the columnar fast path: Ravel rebuilds the events from the
  attribute pages of the span, the same as `attrs`. A single-node query that
  does not select `events` never builds the column.
- A predicate over `links` does not prune. A query that selects the column
  also turns off the columnar fast path, because the fast path never builds
  the merged `attrs` map that `links` decodes `_links_raw` out of. A
  single-node query that does not select `links` never decodes it.
- Under distributed execution, the column selection is not pushed to the
  workers. Each worker scans the full `spans` schema, `events` and `links`
  included, for every row that it returns. The coordinator drops the columns
  that the query did not select.

### Worked queries

Find one trace by id and order its spans by start time:

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
window with a status, service, name, or duration prune.

How a `trace_id` literal plans:

- For `=` and `!=`, a 32-character hex string and a 16-byte binary literal
  (`X'00112233...'`) plan identically. Both take the `trace_id` fast path and
  return the same rows. Ravel plans the literal, and re-applies it as a
  residual, over the real `FixedSizeBinary(16)` column.
- For `IN`, only the binary form plans. `trace_id IN (X'00112233...', ...)`
  works, and `trace_id IN ('00112233...', ...)` still fails to plan.
  DataFusion builds an `IN` list directly from its operands and does not
  consult an expression planner.
- The hex-string spelling also plans against the `trace_id` column of the
  `logs` table, which is `FixedSizeBinary(16)` at the same width.

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

To read a whole trace regardless of the window, query by `trace_id` and widen
or drop the time bounds. Ravel does not flag a result as incomplete. It
returns the spans in view and never waits for a missing root or sibling.
