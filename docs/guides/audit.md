# Audit

Ravel writes an immutable record for the actions that have to be attributable
later: every SQL statement it executes, every legal hold set or cleared, and
every reshard. Each record is written by a Ravel process, never by a client,
and they are read back through the `audit` SQL table.

## What is recorded

Every audit record carries a `kind` attribute naming what it is. There are
three kinds today, and a query selects one with `attrs['kind']`.

### `kind = query`

One record for every SQL statement Ravel executes, written after the statement
runs. Both transports submit it: `POST /api/v1/sql` submits one per request, and
Flight SQL one per executed statement. The record comes from the server's own
execution path, never from the request body or the ticket a client sent, so a
tenant can neither forge one nor suppress one for a statement it ran.

**These records are not written on a stock build.** Both transports submit
their event through a sink that startup fills with a no-op, and no shipped
path constructs the real pipeline, so `attrs['kind'] = 'query'` selects
nothing until a deployment attaches one. The handler behavior and the record
shape below are in place; only the install is missing. The two kinds that
follow are written directly by the maintenance process and are present on any
deployment that has taken those actions.

Attributes:

- `query.language`: the query language, `sql` today.
- `query.tenant`: the hex hash of the resolved tenant, so the record is
  attributed to the tenant Ravel authenticated rather than to any identity the
  client claimed.
- `query.status`: `ok` or `error`, the request's outcome.
- `query.window_start_ns` and `query.window_end_ns`: the request's resolved
  event-time range. The HTTP body carries that range explicitly and it is
  recorded verbatim. A Flight statement's window is consumed earlier, when the
  snapshot is pinned, and is not carried on the path that runs the statement, so
  the Flight path records `0` and `0` rather than a fabricated range. Read a
  `0`/`0` window as "not known at audit time", not as an empty window.
- `query.text`: the statement text, verbatim and untruncated. The request body
  was already size-bounded before the record was written.

`ts_ns` is the request timestamp, `body` is a one-line summary
(`sql query ok`), and `severity_text` is `INFO` for `ok` and `ERROR` for
`error`, so failed statements are selectable without parsing the map.

### `kind = legal_hold`

One record for every hold set and every hold cleared. A hold is not a mutable
flag: setting and clearing both append a new record, and current hold state is
derived by folding them per scope, with the greatest `ts_ns` winning and a tie
resolving to `set`.

Attributes:

- `hold.op`: `set` or `clear`.
- `hold.scope`: one object-key prefix the hold covers. A whole tenant is one
  prefix; holding a single shard takes three records, because a shard's data,
  commit records, and compacted objects live under three sibling prefixes.
- `hold.reason`: optional free text on a set.

`ts_ns` is the set-at or cleared-at time, `body` restates the operation and the
scope, and `severity_text` is `INFO`.

### `kind = reshard`

One record for every shard-count change applied to a tenant's signal, because a
reshard decides where that signal's future data lands and is therefore an
attributable control-plane change.

Attributes:

- `reshard.signal`: the signal that was resharded.
- `reshard.from_shard_count` and `reshard.to_shard_count`: the counts before and
  after.
- `reshard.generation`: the provisioning generation the change created.
- `reshard.activation_hour`: the hour from which the new count applies.

`severity_text` is `INFO`, and `body` restates the change in one line.

## Who writes them, and where they live

Only Ravel writes audit records. There is no ingest path that accepts one, so a
record's presence is evidence a Ravel process took the action, and its absence
cannot be arranged by a client.

The records sit under the tenant's audit prefix on two shards that are retained
differently:

- Legal-hold and reshard records are on the first shard. Nothing deletes it, for
  any role, so a hold record cannot be destroyed and the control-plane trail is
  complete for the life of the tenant.
- Query-audit records are on the second shard, which the maintain process
  compacts and age-sweeps on a 90-day window.

So a tenant reads every retained statement: 90 days of query audit, and every
legal-hold and reshard record ever written. The two shards are disjoint key
paths, and the access policies behind that split are in
[operations/configuration.md](operations/configuration.md).

The read side floors an `audit` query's scan set at both shards regardless of a
deployment's shard count, so a `--shards 1` process still lists the query-audit
shard. Without that floor such a deployment would list only the legal-hold shard
and return an exact-looking answer with every statement missing from it.

## The `audit` table

`audit` is one of the five tables the SQL surface serves (`samples`, `logs`,
`spans`, `alerts`, `audit`), on `POST /api/v1/sql` and on Flight SQL, under the
same one-signal-per-query rule: a query names exactly one of them, and a query
that names two is rejected with a 400 before any listing.

The table is deliberately generic. It promotes no kind-specific field into a
column, because the kinds differ and more will be added; everything specific to
a kind is in the `attrs` map.

| column          | type              | notes                                        |
|-----------------|-------------------|----------------------------------------------|
| `ts_ns`         | `Timestamp(ns)`   | the event time of the audited action          |
| `severity_text` | `Utf8`            | `INFO`, or `ERROR` on a failed statement      |
| `body`          | `Utf8`            | one-line summary of the record                |
| `attrs`         | `Map(Utf8, Utf8)` | `kind` plus that kind's attributes            |

Read one attribute with a subscript: `attrs['kind']`, `attrs['query.status']`,
`attrs['hold.scope']`.

### Which predicates prune

Only a `ts_ns` range prunes. Comparisons of `ts_ns` against a literal timestamp
(`>=`, `>`, `<`, `<=`, `=`, and `BETWEEN`) fold into one window that prunes
objects and blocks; they must be top-level `AND` conjuncts, and an `OR` inside a
conjunct drops that conjunct from pruning.

Every other predicate, including every `attrs['k'] = 'v'` subscript, prunes
nothing and is evaluated exactly above the scan. Pruning is widen-only either
way: it decides how much is read, never which rows come back. Always give an
`audit` query a time window; the attribute predicates filter the result but do
not bound the read.

### Worked queries

The first two read `kind = query` records, which a stock build does not write;
they return rows once a deployment attaches the pipeline. The legal-hold query
below reads records the maintenance process writes directly and works today.

Every statement your tenant ran in one hour, newest first:

```sql
SELECT ts_ns,
       attrs['query.status'] AS status,
       attrs['query.text'] AS statement
FROM audit
WHERE attrs['kind'] = 'query'
  AND ts_ns >= TIMESTAMP '2026-08-19T09:00:00'
  AND ts_ns <  TIMESTAMP '2026-08-19T10:00:00'
ORDER BY ts_ns DESC;
```

Every failed statement in a day:

```sql
SELECT ts_ns, attrs['query.text'] AS statement
FROM audit
WHERE attrs['kind'] = 'query'
  AND attrs['query.status'] = 'error'
  AND ts_ns >= TIMESTAMP '2026-08-19T00:00:00'
  AND ts_ns <  TIMESTAMP '2026-08-20T00:00:00'
ORDER BY ts_ns;
```

`severity_text = 'ERROR'` selects the same rows without reading the map, which
is worth using when a query returns many rows.

Every legal-hold change, with the scope it covered:

```sql
SELECT ts_ns,
       attrs['hold.op'] AS op,
       attrs['hold.scope'] AS scope,
       attrs['hold.reason'] AS reason
FROM audit
WHERE attrs['kind'] = 'legal_hold'
  AND ts_ns >= TIMESTAMP '2026-01-01T00:00:00'
ORDER BY ts_ns;
```

Every reshard, oldest first:

```sql
SELECT ts_ns,
       attrs['reshard.signal'] AS signal,
       attrs['reshard.from_shard_count'] AS from_count,
       attrs['reshard.to_shard_count'] AS to_count,
       attrs['reshard.activation_hour'] AS activation_hour
FROM audit
WHERE attrs['kind'] = 'reshard'
  AND ts_ns >= TIMESTAMP '2026-01-01T00:00:00'
ORDER BY ts_ns;
```

## Reading the audit trail is audited

A query over `audit` is a SQL statement, so it submits one more query-audit
record, exactly as any other statement does. On a deployment that has attached
the pipeline the trail therefore records its own readers: the statement you
just ran appears in the next `audit` query you run, and an investigation that
reads the trail repeatedly adds one record per read. That is the intended
behavior and not something to work around. Until the pipeline is attached
nothing is written, so no record of the readers exists either.

## Tenancy

Resolution is per tenant hash, so an `audit` query reads that tenant's own
records and cannot reach another tenant's. `attrs['query.tenant']` on a record
is the hash of the tenant Ravel resolved for the audited request, which is the
same tenant reading it back.

## Cost

An `audit` query reads through the same fetcher a `logs` query reads through, so
its bytes are cached by the same RAM and disk tiers and accounted through the
same funnel. Audit records are not folded into the catalog, so a query lists the
tenant's audit commit records for its window on every call, one bounded listing
per shard, on top of whatever the query-audit shard's own compaction has already
merged.

## Background

The two signals and their record format are
[ADR-0040](../adrs/0040-alerts-and-audit-signals.md); the custody rules that
require a query-audit and legal-hold trail are
[ADR-0042](../adrs/0042-compliance-custody.md) and
[ADR-0062](../adrs/0062-encryption-posture-and-evidential-audit.md); the SQL
tables over both signals, and the read-side shard floor that keeps an `audit`
scan complete, are
[ADR-1101](../adrs/1101-alerts-and-audit-sql-tables.md).
