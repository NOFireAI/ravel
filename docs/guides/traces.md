# Querying traces

Ravel stores every span in RSPAN objects and serves them through the `spans`
SQL table. This guide explains how spans are stored, why a single trace is a
bounded read, how to query the `spans` table, and what "incomplete trace"
means. Read the [query guide](query.md) first for the shared `POST /api/v1/sql`
mechanics; this guide covers only what is specific to spans.

## How Ravel stores spans

A span is one record in an RSPAN segment object. Object storage is the only
durable copy. Records in a segment sort by `(trace_id, start_ts)`, so all spans
of one trace occupy a contiguous run of blocks. Each object carries a skip
index that holds, per block, the block's `trace_id` range, its time interval
(`start_ts` minimum and `end_ts` maximum), its `duration_ns` range, and a
one-byte `status_mask`. RSPAN v4 objects also carry a BLOOM section with a
per-block bloom filter over the tokens of `service.name` and span `name`, plus a
block-local `service_name` column. The reader uses these structures to skip
blocks that cannot match a query.

This guide does not repeat the on-disk layout. The full, normative spec is
[docs/span-segment-format.md](../span-segment-format.md). Read it to change the
format; read this guide to query it.

## Why one trace is a bounded read

Ingest routes each span to a shard by its `trace_id`. The router hashes the
`trace_id` with BLAKE3 and picks the shard from the hash, so all spans of one
trace land on the same shard, and within that shard's objects they sort
together into a contiguous block run.

The query-time catalog listing does not yet exploit this routing: a
`trace_id =` query still lists and opens every shard's segments in the
matched time window, the same as any other query. What is bounded is the
per-object decode: the skip index drops every block whose `trace_id` range
excludes the target, so the reader decodes only the blocks that can hold the
trace, on whichever shard(s) it lists. Shard-level pruning by `trace_id` is a
later capability, not a current one.

One caveat applies regardless. ADR-0052 online resharding can move a tenant
to a new shard count while spans are still arriving. A trace whose spans
straddle a reshard activation can split across two shards; a trace-by-id
query still returns every stored span from wherever it landed.

## The `spans` table

`POST /api/v1/sql` serves three tables from one endpoint: `samples`
(metrics), `logs`, and `spans`. Each query targets exactly one table. Ravel
decides the target from the query's `FROM` clause before planning and rejects a
query that names two real tables. This matches the one-signal-per-query rule the
`samples` and `logs` tables already follow.

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

`status_code` is the stored OTLP byte. To read it as text, map the three values
in SQL, for example `CASE status_code WHEN 0 THEN 'Unset' WHEN 1 THEN 'Ok' WHEN
2 THEN 'Error' END`.

`duration_ns` is a computed column, not a stored one. It is exactly `end_ts -
start_ts`, and both endpoints are already stored, so Ravel exposes the
difference as a column rather than writing it to every row. You query it like
any other column (`WHERE duration_ns > 5e8`); the reader answers it from each
block's stored duration range.

### Which predicates prune

All pruning is widen-only. DataFusion always re-applies the original `WHERE`
predicate above the scan, so a query never returns a wrong row. Pruning only
decides which blocks the reader decodes; it never decides which rows the query
returns. A predicate the reader cannot prune with is still evaluated exactly, it
just reads more blocks.

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

These predicates prune at the bloom level (RSPAN v4):

- `service_name = <literal>` probes the block's `service.name` bloom. The
  reader skips a block whose bloom proves the token absent. A bloom never proves
  presence, so a positive probe still reads the block.
- `name = <literal>` probes the block's span-name bloom the same way.

Every other predicate is evaluated as a DataFusion residual only. It prunes no
blocks but still filters rows exactly. Notes on the shapes above:

- The pruning predicates must be top-level `AND` conjuncts. A disjunction (`OR`)
  inside a conjunct drops that conjunct from pruning, except a same-axis `OR` or
  `IN` list on `status_code` or `duration_ns`, which prunes as the union of its
  parts.
- `service_name` and `name` equality push down but are also always re-checked
  as a residual, because a bloom is a false-positive filter.
- Attribute equality on the `attrs` map (`attrs['k'] = 'v'`) does not prune. It
  is evaluated exactly over the merged map. Span attribute pruning is a later,
  undecided epic, not a current capability.

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

The first query uses the `trace_id` fast path. The others combine a time window
with a status, service, name, or duration prune. Each `WHERE` clause is also
re-applied exactly above the scan, so every result is correct whether or not a
block was pruned.

## Incomplete traces

A trace is incomplete when the query does not see all of its spans. This happens
in two ways:

- Some of the trace's spans have not landed yet. Ravel acknowledges each span
  batch as it is durable and never buffers a whole trace to wait for its
  siblings. A tail-sampling or completeness buffer belongs at the collector, not
  in Ravel.
- The trace's parent or root span falls outside the queried time window. A child
  span can start and end inside a window whose parent started before it. A
  window query returns the spans in the window, not the whole trace.

To read a whole trace regardless of window, query by `trace_id` and widen or
drop the time bounds. Ravel does not flag a result as incomplete; it returns
exactly the spans in view and never waits for a missing root or sibling.

## What Ravel does not store

Span links are not stored. A link points from one span to another span in a
different trace. Ravel deliberately keeps links out of RSPAN; they will get
their own design decision when a query needs them. A query over the `spans`
table cannot filter or return links.
</content>
</invoke>
