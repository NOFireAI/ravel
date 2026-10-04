# Audit

Ravel writes an immutable audit record for every SQL statement that it
executes, every legal hold set or cleared, and every reshard. You read the
records through the `audit` SQL table. A Ravel process writes each record. A
client cannot write one.

## What is recorded

Every audit record carries a `kind` attribute. There are three kinds today.
A query selects one kind with `attrs['kind']`.

### `kind = query`

Ravel writes one record for every query that it executes, after the query
runs. This applies to every query surface:

- `POST /api/v1/sql` and Flight SQL
- `/api/v1/query` and `/api/v1/query_range`
- `/api/v1/labels`, `/api/v1/label_values`, and `/api/v1/series`
- `/api/v1/analytics`
- `/api/v1/query_exemplars`

The record comes from the execution path of the server. It does not come from
the request body or from the ticket that a client sent. A tenant cannot forge
a record, and cannot suppress the record of a query that it ran.

One group-commit pipeline writes the records of every surface. Ravel installs
the pipeline only in the query-serving modes (`all` and `query`). The
`maintain` and `gateway` modes serve no query surface and install no pipeline.

#### Write failures

The pipeline makes two writes for each batch: the data object, then its commit
record. Each write retries a transient object-store error (a timeout or a
throttle response) a few times with a short backoff. A non-transient error
fails the batch. An error that continues through every attempt also fails the
batch.

`--audit-mode` sets what happens when the pipeline cannot make a record
durable:

| `--audit-mode` | Result |
|---|---|
| `required` (default) | The query fails with a 503 (HTTP) or `Unavailable` (Flight). |
| `best-effort` | Ravel logs the failure, counts it on `ravel_audit_write_failures_total`, and lets the response proceed. |

Use `best-effort` for a deployment that prefers an unaudited response to a
failed query.

#### Batching

`--audit-max-batch` sets how many records the pipeline groups into one write.
`--audit-max-age` sets how long a record waits before the pipeline forces its
group out. If you set neither flag, both take the defaults of the pipeline.

The pipeline writes a record immediately when all three of these conditions
are true:

- No other record is in the queue.
- The record was not submitted while a write was in progress.
- The pipeline picked up its previous record at least `--audit-max-age`
  earlier.

As a result, sequential queries do not wait for a batch. A record submitted
during the write of another record waits for that write, then for a full
`--audit-max-age`, then for its own write.

#### Shutdown

On shutdown, the server stops every listener first. Then it drains the
buffered records in one last group write. After that write, no query surface
can submit again. The drain is bounded at `--audit-max-age` plus five seconds.

- A slow store finishes well inside the bound. The write is one object PUT
  plus one commit record, and each retries a few times before the pipeline
  stops.
- If the store never answers, the bound elapses. A warning says that the
  buffered records are possibly not durable, and shutdown completes.

Records already written are unaffected. In `required` mode, no response was
released for the buffered records.

#### Query text

`--audit-text` sets how Ravel records the `query.text` attribute.

| `--audit-text` | What Ravel stores |
|---|---|
| `redacted` (default) | A tokenization that keeps the structure of the query. |
| `plaintext` | The query text verbatim. |

Under `redacted`, a `tok_<hex>` token replaces every string literal in a SQL
statement and every label-matcher value in a PromQL expression. Table names,
column names, metric names, label names, operators, keywords, and the
`LIMIT`/`OFFSET` counts stay readable. The same value gives the same token
everywhere. You can therefore correlate a record across queries and surfaces,
and Ravel does not store the value. Tokenization runs before the record
reaches the pipeline, so Ravel writes no verbatim text to the audit object or
to its commit record.

`plaintext` is an explicit opt-in for a regime that requires the original
text. It puts everything that the query carried into the audit trail for the
whole retention window of that trail. This includes personal data in a
literal or in a matcher value.

#### Token key

The token key comes from `RAVEL_AUDIT_TOKEN_KEY`. The value is 64 hex
characters, for a 32-byte key. Ravel accepts hex only.

- If `RAVEL_AUDIT_TOKEN_KEY` is not set, Ravel derives the key from the
  deployment key that `--tenant-hash-key-file` configures.
- If neither is available, a query-serving process refuses to start under
  `redacted`. It does not fall back to verbatim text.
- An unkeyed deployment (`--tenant-hash-unkeyed`) must set
  `RAVEL_AUDIT_TOKEN_KEY` or pass `--audit-text plaintext`.
- A `gateway` or `maintain` process never reads the key, because it writes no
  query-audit record.

Keep the key for as long as you keep the records tokenized under it. A
different key gives different tokens for the same value. Records written
under a lost key stay readable, but they do not correlate with later records.

#### Attributes

- `query.language`: the surface that produced the record: `sql`, `promql`,
  `labels`, `label_values`, `series`, `analytics`, or `exemplars`.
- `query.tenant`: the hex hash of the resolved tenant. The record names the
  tenant that Ravel authenticated, whatever identity the client claimed.
- `query.status`: `ok` or `error`, the outcome of the request, or `attempted`.
- `query.text`: the query text as that surface understands it, in the form
  that `--audit-text` selected. Under `redacted`, Ravel stores a text that it
  cannot parse as a single token over the whole text. The record then shows
  that the query ran, and its content is not stored.
- `query.window_start_ns` and `query.window_end_ns`: the resolved event-time
  range, in the terms that the request of that surface uses.

`ts_ns` is the request timestamp. `body` is a one-line summary
(`sql query ok`). `severity_text` is `INFO` for `ok` and `ERROR` for `error`,
so you can select failed queries without parsing the map.

The number of records depends on the request:

| Request | Records |
|---|---|
| A query that reaches execution | One. |
| A SQL `CREATE` or `DROP` statement that passes the `ddl` capability check | Two: `attempted` before it touches storage, then `ok` or `error` with its outcome. |
| A DDL statement refused for the capability | One. |
| A request refused before that | None. |

These requests are refused before that and write no record:

- A request that fails authentication.
- A body that is not valid JSON.
- A malformed `timeout`, `start` or `end`.
- A query that the fleet-wide admission limit refuses.

An absent record therefore does not mean an absent request. To count
statements and not records, leave out the `attempted` rows.

`query.text` holds this text for each surface:

| `query.language` | `query.text` |
|---|---|
| `sql` | The SQL statement. |
| `promql`, `analytics` | The PromQL expression. |
| `labels`, `label_values`, `series` | The joined selector list. |
| `exemplars` | The selector. |

The window attributes hold this range for each surface:

| Surface | Recorded window |
|---|---|
| `sql`, `analytics` | The resolved range of the request. |
| `promql` instant query | Its one instant, as `(t, t)`. |
| `promql` range query | `(start, end)`. |
| `labels`, `label_values`, `series` | The resolved selector window. |
| `exemplars` | Its own resolved start and end. |
| Flight SQL statement | `0` and `0`. |

The Flight path consumes the window earlier, when it pins the snapshot. It
does not carry the window on the path that runs the statement. Read a `0`/`0`
window as "not known at audit time". It is not an empty window.

### `kind = legal_hold`

Ravel writes one record for every hold set and every hold cleared. A hold is
not a mutable flag. A set and a clear each append a new record. Ravel derives
the current hold state for each scope from these records. The record with the
greatest `ts_ns` wins, and a tie resolves to `set`.

Attributes:

- `hold.op`: `set` or `clear`.
- `hold.scope`: one object-key prefix that the hold covers. A whole tenant is
  one prefix. A hold on a single shard takes three records, because the data,
  the commit records, and the compacted objects of a shard are under three
  sibling prefixes.
- `hold.reason`: optional free text on a set.

`ts_ns` is the set-at or cleared-at time. `body` restates the operation and
the scope. `severity_text` is `INFO`.

### `kind = reshard`

Ravel writes one record for every shard-count change applied to a signal of a
tenant. A reshard decides where the future data of that signal lands.

Attributes:

- `reshard.signal`: the signal that was resharded.
- `reshard.from_shard_count` and `reshard.to_shard_count`: the counts before and
  after.
- `reshard.generation`: the provisioning generation the change created.
- `reshard.activation_hour`: the hour from which the new count applies.

`severity_text` is `INFO`, and `body` restates the change in one line.

## Writers and storage

Only Ravel writes audit records. No ingest path accepts one. A record
therefore shows that a Ravel process took the action, and a client cannot
arrange for a record to be absent.

The records are under the audit prefix of the tenant, on two shards with
different retention:

- Legal-hold and reshard records are on the first shard. Nothing deletes it, for
  any role, so a hold record cannot be destroyed and the control-plane trail is
  complete for the life of the tenant.
- Query-audit records are on the second shard, which the maintain process
  compacts and age-sweeps on a 90-day window.

A tenant can therefore read 90 days of query audit, and every legal-hold and
reshard record ever written. The two shards are disjoint key paths. The access
policies behind that split are in
[operations/configuration.md](operations/configuration.md).

An `audit` query always scans both shards, whatever the shard count of the
deployment. A `--shards 1` process therefore still lists the query-audit
shard.

## The `audit` table

`audit` is one of the five tables that the SQL surface serves (`samples`,
`logs`, `spans`, `alerts`, `audit`), on `POST /api/v1/sql` and on Flight SQL.
A query must name one table only. Ravel rejects a query that names two with a
400, before any listing.

The table has no kind-specific column. Everything specific to a kind is in the
`attrs` map.

| column          | type              | notes                                        |
|-----------------|-------------------|----------------------------------------------|
| `ts_ns`         | `Timestamp(ns)`   | the event time of the audited action          |
| `severity_text` | `Utf8`            | `INFO`, or `ERROR` on a failed statement      |
| `body`          | `Utf8`            | one-line summary of the record                |
| `attrs`         | `Map(Utf8, Utf8)` | `kind` plus that kind's attributes            |

Read one attribute with a subscript: `attrs['kind']`, `attrs['query.status']`,
`attrs['hold.scope']`.

### Which predicates prune

Only a `ts_ns` range prunes. Always give an `audit` query a time window.

Comparisons of `ts_ns` against a literal timestamp (`>=`, `>`, `<`, `<=`, `=`,
and `BETWEEN`) combine into one window that prunes objects and blocks. The
comparisons must be top-level `AND` conjuncts. An `OR` inside a conjunct
removes that conjunct from pruning.

Every other predicate prunes nothing, and Ravel evaluates it above the scan.
This includes every `attrs['k'] = 'v'` subscript. The attribute predicates
filter the result but do not bound the read. Pruning is widen-only: it decides
how much Ravel reads, and never which rows come back.

### Worked queries

The first two queries read `kind = query` records, which every query surface
writes. The legal-hold query reads records that the maintenance process writes
directly.

Every statement your tenant ran in one hour, newest first. The query leaves
out the `attempted` record of a DDL statement, so each statement appears once,
with its outcome:

```sql
SELECT ts_ns,
       attrs['query.status'] AS status,
       attrs['query.text'] AS statement
FROM audit
WHERE attrs['kind'] = 'query'
  AND attrs['query.status'] <> 'attempted'
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

`severity_text = 'ERROR'` selects the same rows and does not read the map. Use
it when a query returns many rows.

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
record. The statement that you ran appears in the next `audit` query that you
run. An investigation that reads the trail repeatedly adds one record per
read. This behavior is intended.

## Tenancy

An `audit` query reads the records of its own tenant and cannot reach the
records of another tenant, because resolution is per tenant hash.
`attrs['query.tenant']` on a record is the hash of the tenant that Ravel
resolved for the audited request. This is the same tenant that reads the
record back.

The write side keeps the same separation. One process serves every tenant
that it authenticates, and one group-commit pipeline writes their query-audit
records. Each record carries its tenant. The tenant is not fixed when the
pipeline starts. A write groups the records by tenant and stores each group
under the audit prefix of that tenant. A deployment with no static tenant
list, which is the usual OIDC and mTLS case, routes the same way.

The pipeline refuses a batch that mixes tenants. It does not store that batch
under one of them.

## Cost

An `audit` query reads through the same fetcher as a `logs` query. The same
RAM tier and disk tier cache its bytes, and the same funnel accounts for
them.

Audit records are not folded into the catalog. A query therefore lists the
audit commit records of the tenant for its window on every call. This is one
bounded listing per shard, on top of whatever the query-audit shard's own
compaction has already merged.

## Background

- The two signals and their record format:
  [ADR-0040](../adrs/0040-alerts-and-audit-signals.md).
- The custody rules that require a query-audit and legal-hold trail:
  [ADR-0042](../adrs/0042-compliance-custody.md) and
  [ADR-0062](../adrs/0062-encryption-posture-and-evidential-audit.md).
- The SQL tables over both signals, and the read-side shard floor that keeps
  an `audit` scan complete:
  [ADR-1101](../adrs/1101-alerts-and-audit-sql-tables.md).
