# Alerting

Ravel evaluates alert rules on a schedule, compares each rule's query result
against a condition, and posts a notification to a sink when the rule changes
state. A rule is either an observability alert (a PromQL query plus a numeric
threshold) or a security detection (a SQL query plus a returns-any-row
condition). There is one rule engine for both.

## Turning evaluation on, and where it runs

Alert evaluation is off by default. It turns on when `--alert-rules-file` names
a JSON file that holds at least one rule. With the flag absent, or the file
empty of rules, no evaluator runs and no alert can fire.

Evaluation runs only in a process that builds a query engine, which is
`--mode all` or `--mode query`. A `gateway` or `maintain` process ignores the
rules file entirely. It does not fail: it logs a warning at startup naming the
mode and stating that the rules will never be evaluated and no alert will ever
fire. So an operator who puts the rules file on a gateway or maintain
deployment gets no alerts and no error. Put the rules file on the process that
answers queries.

Alert state transitions are written durably to object storage under their own
signal prefix, and they are read back through the `alerts` SQL table. Alerts
reach you through the sinks below as they happen, and the same transitions stay
queryable afterwards, so a dashboard or an investigation can read Ravel's own
alert history. See [Querying alert history](#querying-alert-history).

## The rules file

The file is JSON with a top-level `rules` array. Each entry is one rule:

```json
{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "cpu-hot",
      "promql": "max by (instance) (cpu_usage)",
      "condition": {"type": "threshold", "op": "gt", "value": 0.9},
      "for": "5m",
      "labels": {"severity": "page"},
      "annotations": {"summary": "CPU over 90% for five minutes"}
    },
    {
      "tenant": "acme",
      "rule_id": "access-denied-burst",
      "sql": "select 1 from logs where has_word(body, 'denied') limit 1",
      "condition": {"type": "non_empty_result"},
      "annotations": {"summary": "denied access log lines in the lookback window"}
    }
  ]
}
```

Fields on a rule:

- `tenant` (required): the tenant id this rule belongs to, matching a
  `--tenant-token` tenant.
- `rule_id` (required): a stable operator-chosen identifier. Together with
  `labels` it forms the alert's identity, so keep it stable across restarts.
- Exactly one of `promql` or `sql` (required): the query text. Naming both, or
  neither, fails startup.
- `condition` (required): a tagged object.
  - `{"type": "threshold", "op": "gt", "value": 0.9}` for a PromQL rule. The
    rule fires when any series value satisfies `value <op> threshold`. `op` is
    one of `gt`, `ge`, `lt`, `le`, `eq`, `ne`.
  - `{"type": "non_empty_result"}` for a SQL rule. The rule fires when the query
    returns at least one row, so write the query to return no rows when
    nothing matched: a bare aggregate such as `count(*)` always returns one
    row and would fire on every tick.
  - A PromQL query takes a `threshold` condition and a SQL query takes a
    `non_empty_result` condition. The other pairing fails startup, not once per
    tick.
- `labels` (optional): a string map attached to every alert the rule produces.
  Part of the alert's identity.
- `annotations` (optional): a string map carried on the notification (a summary,
  a runbook link). Not part of identity.
- `for` (optional): a humantime duration (`5m`, `30s`), the pending-before-firing
  delay. Omitted, the rule fires on the first tick its condition holds; set, the
  condition must hold continuously for that long first.
- `repeat_interval` (optional): a humantime duration for how often a rule that
  stays firing re-notifies its sinks. Omitted uses a one-minute default; `0s`
  disables repeats for that rule.
- `max_alert_generation` (optional): a per-rule override of the alerts-on-alerts
  generation circuit breaker.

Validation is strict and happens once at startup, not every tick: an unknown
field, a rule naming neither or both query languages, an unparseable `for` or
`repeat_interval`, a condition that cannot apply to its query shape, or two
rules in one tenant that would produce the same alert identity all fail the
process at load time.

A SQL detection rule reads the same tables the `POST /api/v1/sql` endpoint
serves (`samples`, `logs`, `spans`, `audit`), under the same
one-signal-per-query rule. See the [query guide](query.md) for the query
languages themselves.

A rule that reads `alerts`, so that an alert fires on other alerts, is not
usable yet. The table is queryable from the endpoint, but the evaluator passes
no consumed generations to the recursion guard, so every record such a rule
produced would sit at generation 1 and the `max_alert_generation` circuit
breaker could never trip. Until that is wired, write rules against the other
four tables.

## Evaluation cadence and the SQL lookback

- `--alert-eval-interval-secs` (default `60`): how often each tenant's evaluator
  wakes to evaluate every rule configured for that tenant.
- `--alert-sql-lookback` (default `5m`): the event-time window a SQL detection
  rule's query resolves over, ending at the tick's clock reading. It bounds only
  which segments the query lists; the statement's own `WHERE` still applies above
  the scan. A PromQL rule evaluates as an instant query and does not use this
  window.

## Notification sinks

A sink is where a transition is delivered. Every sink flag is repeatable, and a
transition is posted to every configured sink after its record is durably
written. There are four kinds, in two pairs.

- Unauthenticated webhook: `--alert-webhook-url URL`. Each transition is POSTed
  as JSON to every configured URL.
- Unauthenticated Alertmanager: `--alertmanager-url URL`. The URL is either an
  Alertmanager base URL (`http://alertmanager:9093`) or its full
  `/api/v2/alerts` endpoint; the well-known path is appended when it is missing.
- Authenticated webhook: `--alert-webhook SPEC`, a comma-separated `key=value`
  spec. `url=...` is required, plus exactly one credential, given as either
  `bearer-file=PATH` or `basic-user=NAME,basic-pass-file=PATH`. The secret is
  read from a file, never inline, so it never appears in a process listing.
- Authenticated Alertmanager: `--alertmanager SPEC`, the same spec as
  `--alert-webhook`; its `url` may be a base URL or the full `/api/v2/alerts`
  endpoint.

For a webhook that needs a bearer token whose value lives in `/etc/ravel/hook.token`:

```sh
ravel-server --mode all \
  --alert-rules-file /etc/ravel/alerts.json \
  --alert-webhook 'url=https://hooks.example.com/ravel,bearer-file=/etc/ravel/hook.token' \
  ...
```

The full flag list, with defaults and help, is in
[ravel-server-flags.md](../reference/ravel-server-flags.md).

## Querying alert history

Every transition an evaluator writes is a row in the `alerts` table, served by
`POST /api/v1/sql` and by Flight SQL. It is one of the five tables the SQL
surface exposes (`samples`, `logs`, `spans`, `alerts`, `audit`), under the same
one-signal-per-query rule as the rest: a query names exactly one of them, and a
query that names two is rejected with a 400 before any listing.

The table has these columns:

| column         | type                | notes                                              |
|----------------|---------------------|----------------------------------------------------|
| `ts_ns`        | `Timestamp(ns)`     | the transition's event time, never null             |
| `alert_id`     | `Utf8`              | 32-character hex identity of the alert, nullable    |
| `rule_id`      | `Utf8`              | the rule that produced the transition, nullable     |
| `state`        | `Utf8`              | `pending`, `firing`, `resolved`, `suppressed`; nullable |
| `generation`   | `Int64`             | the alerts-on-alerts generation counter, nullable   |
| `writer_id`    | `Utf8`              | the evaluator that wrote the record, never null     |
| `writer_epoch` | `UInt64`            | write identity from the record's commit record      |
| `writer_seq`   | `UInt64`            | write identity from the record's commit record      |
| `attrs`        | `Map(Utf8, Utf8)`   | every attribute of the record, merged into one map  |

`alert_id` is the stable hash of the rule id and the alert's label set, so every
record for one alerting condition carries the same value across restarts and
across rule reloads. The record's severity mirrors `state` (firing at the ERROR
level, pending at WARN, resolved and suppressed at INFO), but the table exposes
no severity column: filter on `state` itself.

`attrs` carries the four promoted keys above plus everything a rule attached:
one entry per rule label under `label.<name>`, and one per annotation under
`annotation.<name>`. Read a single one with a subscript, for example
`attrs['label.severity'] = 'page'` or `attrs['annotation.summary']`. The label
and annotation key sets are per-rule and open-ended, which is why they are a map
and not columns.

### One row per transition, and how to fold it

A record is written when an alert changes state, never on a tick that changes
nothing, and each transition is one immutable object. So the table is history:
it holds what happened and when, and it never holds a folded "current state"
row. You compute current state with a query.

Each row also carries the identity of the write that produced it: `writer_id`,
`writer_epoch`, and `writer_seq`, stamped from the object's commit record. They
are there because `ts_ns` alone is not a total order. Two evaluators can overlap
briefly at a lease handover and write the same `alert_id` at the same `ts_ns`,
and ordering by timestamp alone would leave two rows tied for "latest". Ordering
by `ts_ns DESC, writer_epoch DESC, writer_seq DESC, writer_id DESC` is a total
order, so the fold below returns exactly one row per alert.

One caveat on what that order means. `writer_epoch` is a constant today, not a
lease term, and each evaluator's `writer_seq` restarts at 1, so across a
handover the key picks the departing evaluator's record rather than the later
write. The result is still exactly one current row per alert, and it is the
same row the evaluator's own fold picks, so the table agrees with the writer.
Do not read the order as causal ordering between evaluators.

```sql
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY alert_id
           ORDER BY ts_ns DESC, writer_epoch DESC, writer_seq DESC,
                    writer_id DESC
         ) AS rn
  FROM alerts
)
WHERE rn = 1;
```

That row carries its transition's `state`, `generation`, labels, and
annotations together, so filtering it by `state` answers "what is true now"
rather than "what changed recently".

### Which predicates prune

All pushdown is widen-only. DataFusion re-applies the original `WHERE`
predicate above the scan, so a query never returns a wrong row; pruning only
decides how much is read.

- `ts_ns` range comparisons (`>=`, `>`, `<`, `<=`, `=`, and `BETWEEN`) fold
  into one time window that prunes objects and blocks.
- `alert_id = '<hex>'` and `rule_id = '<name>'` equality against a string
  literal push into the reader as exact per-record attribute equalities. The
  reader skips blocks whose attribute bloom proves the value absent and
  re-checks every surviving row.

Every other predicate, including any `attrs['k'] = 'v'` subscript, prunes
nothing and is evaluated exactly above the scan. The pruning shapes must be
top-level `AND` conjuncts: an `OR` inside a conjunct drops that conjunct from
pruning.

### Worked queries

Which alerts are firing right now for one rule:

```sql
SELECT alert_id, ts_ns, generation, attrs
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY alert_id
           ORDER BY ts_ns DESC, writer_epoch DESC, writer_seq DESC,
                    writer_id DESC
         ) AS rn
  FROM alerts
  WHERE rule_id = 'cpu-hot'
)
WHERE rn = 1 AND state = 'firing';
```

The `rule_id` equality is inside the subquery on purpose: it prunes the scan,
and the fold then runs over that rule's records only.

Every transition one alert went through, oldest first:

```sql
SELECT ts_ns, state, generation, writer_id
FROM alerts
WHERE alert_id = '5f2b9c0a1d4e6f8091a2b3c4d5e6f708'
ORDER BY ts_ns, writer_epoch, writer_seq, writer_id;
```

How often each rule changed state in a window, the flapping check:

```sql
SELECT rule_id, count(*) AS transitions
FROM alerts
WHERE ts_ns >= TIMESTAMP '2026-08-19T00:00:00'
  AND ts_ns <  TIMESTAMP '2026-08-20T00:00:00'
GROUP BY rule_id
ORDER BY transitions DESC;
```

### Cost and retention

An `alerts` query reads through the same fetcher the `logs` table reads
through, so its bytes are cached by the same tiers that fetcher runs, the RAM
tier always and the local-disk tier when `--cache-dir` is set, and accounted
through the same funnel. Alert records are not folded into the catalog and not
compacted, so a query lists the tenant's alert commit records for its window on
every call, one bounded listing per shard. One object per transition keeps that
listing small.

No retention rule covers the alerts signal today. A transition record is
written once and is never swept, so alert history grows with the number of
transitions and nothing trims it. A future retention rule that covers the
signal would change that; until then, plan for the records to stay.
