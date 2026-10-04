# Alerting

Ravel evaluates alert rules on a schedule. It compares the result of each
rule's query against a condition. When a rule changes state, Ravel posts a
notification to a sink. One rule engine serves two kinds of rule:

- An observability alert is a PromQL query plus a numeric threshold.
- A security detection is a SQL query plus a returns-any-row condition.

## Turn evaluation on

Alert evaluation is off by default. To turn it on, pass `--alert-rules-file`
with a JSON file that holds at least one rule. If the flag is absent or the
file holds no rules, no evaluator runs and no alert can fire.

Put the rules file on the process that answers queries. Evaluation runs only
in `--mode all` or `--mode query`, the modes that build a query engine.

A `gateway` or `maintain` process ignores the rules file and does not fail.
At startup it logs a warning that names the mode. The warning states that the
rules will never be evaluated and that no alert will ever fire.

Ravel writes each alert state transition durably to object storage, under its
own signal prefix. The sinks deliver each transition as it happens. The
`alerts` SQL table serves the same transitions afterwards. See
[Querying alert history](#querying-alert-history).

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

A rule has these fields:

- `tenant` (required): the tenant id that the rule belongs to. It matches a
  `--tenant-token` or `--tenant-token-file` tenant.
- `rule_id` (required): a stable identifier that the operator chooses, unique
  within a tenant. The `rule_id` and the label set of an alert form the
  identity of that alert, so keep the `rule_id` stable across restarts.
- `promql` or `sql` (required): the query text. Name one of the two. A rule
  that names both, or neither, fails startup.
- `condition` (required): a tagged object.
  - `{"type": "threshold", "op": "gt", "value": 0.9}` for a PromQL rule. Each
    series whose value satisfies `value <op> threshold` raises its own alert.
    See [One alert per matching series](#one-alert-per-matching-series). `op`
    is one of `gt`, `ge`, `lt`, `le`, `eq`, `ne`.
  - `{"type": "non_empty_result"}` for a SQL rule. The rule fires when the
    query returns at least one row. Write the query so that it returns no
    rows when nothing matches. A bare aggregate such as `count(*)` always
    returns one row, so it fires on every tick.
  - A PromQL query takes a `threshold` condition. A SQL query takes a
    `non_empty_result` condition. The other pairing fails startup.
- `labels` (optional): a string map attached to every alert that the rule
  produces. A rule label replaces a series label of the same name. The labels
  are part of the identity of every alert.
- `annotations` (optional): a string map that the notification carries, for
  example a summary or a runbook link. Annotations are not part of the
  identity.
- `for` (optional): a humantime duration (`5m`, `30s`). The condition must
  hold continuously for that long before the rule fires. If `for` is omitted,
  the rule fires on the first tick that its condition holds.
- `repeat_interval` (optional): a humantime duration. It sets how often an
  alert that stays firing notifies its sinks again. The default is one minute.
  `0s` disables repeats for that rule. The interval applies to each alert of
  the rule separately, counted from the firing record of that alert. A rule
  with 500 firing series sends 500 repeat notifications per interval to every
  sink.
- `max_alert_generation` (optional): a per-rule override of the
  alerts-on-alerts generation circuit breaker.

Ravel validates the rules file once, at startup. Each of these fails the
process at load time:

- an unknown field
- a rule that names neither or both query languages
- a `for` or `repeat_interval` that does not parse
- a condition that cannot apply to its query shape
- two rules in one tenant that share a `rule_id`

A `rule_id` must be unique within its tenant even when the rules carry
different labels. The evaluator resolves every alert of a `rule_id` that the
rule's query no longer matches. With a shared `rule_id`, each rule resolves
the alerts of the other on every tick.

## One alert per matching series

A PromQL rule raises one alert for each series that satisfies its condition.
The example rule `max by (instance) (cpu_usage)` raises one alert per hot
instance. Each alert moves through pending, firing, and resolved separately.

Two cases raise one alert that carries the rule labels alone:

- A PromQL query that returns a scalar, which has no series labels.
- A SQL rule, which has no series.

### Alert identity

The label set of an alert is the series labels without `__name__`, overlaid
by the rule's `labels`. The rule label wins when both name the same label.
The `alert_id` is the hash of the `rule_id` and that label set. The
notification carries the same label set, so Alertmanager grouping and
silences match on the series labels.

The Alertmanager sink sets `alertname` to the `rule_id`, or to the rule's own
`alertname` label when the rule sets one.

A series label named `alertname` (for example, the output of a recording
rule) stays in the identity of the alert and in the webhook payload. It never
replaces the Alertmanager `alertname`:

- If the rule sets no `alertname`, the Alertmanager payload carries the
  series value as `exported_alertname`. Two series that differ only in that
  label stay two alerts in Alertmanager.
- If the series already has an `exported_alertname`, the payload carries the
  value as `exported_exported_alertname`. This is Prometheus' conflict rule.
- If the rule sets `alertname`, the rule label replaces the series label and
  nothing is exported.

### Duplicate identity

If two matched series produce the same label set, the rule fails that tick
with `DuplicateAlertIdentity`. One alert cannot hide the other. An example is
two metric names that differ only in the dropped `__name__`. To correct the
rule, aggregate, or add a label that tells the series apart.

### Resolution

An alert resolves on the next tick after its series stops matching. The
series stops matching when its value no longer satisfies the condition, or
when the series stops reporting. The other alerts of the rule do not change.

### The alert cap

A rule can raise at most 1000 alerts per evaluation. The cap counts matching
series, not the size of the query result. The cap is fixed and has no flag.

A rule that matches more series fails the tick with `TooManyAlerts`:

- The rule writes no record.
- Its existing alerts keep their state.
- `ravel_alert_rules_failed_total` rises.
- The warning names the count and the limit.

To correct the rule, narrow the selector or aggregate.

### Churning labels

The evaluator keeps one state entry per alert identity that it has ever seen.
Nothing prunes those entries yet. A rule over a label whose values churn,
such as a pod name or a request id, adds an entry for every series that comes
and goes. Per-series rules over churning label sets wait on alert state
pruning (see [Background](#background)). Until it lands, aggregate the
churning label away.

A sink that keeps failing grows the in-memory retry queue of the evaluator
the same way. The queue gains one notification per identity that fires or
resolves, until the sink accepts them.
`ravel_alert_undelivered_notifications` reports the size of that queue.

### The delivery deadline

On each tick, delivery to the sinks is bounded to half the evaluation
interval. A slow or unresponsive sink cannot make the delivery phase grow
with the queue behind it.

How the deadline applies:

- The evaluator checks the deadline before each attempt, not during one.
- The first attempt of a tick is unconditional.
- One whole attempt is the number of configured sinks times the 10-second
  sink HTTP timeout.
- The delivery phase ends at the latest at
  `max(tick start + half the interval, start of delivery)` plus one whole
  attempt.

The half interval starts at the start of the tick, not at the start of
delivery. The work that precedes delivery in the same tick spends it too:

- the history read
- the lease acquire
- on the lease holder, rule evaluation, the repeat pass, and the alert state
  memo write

On a tick whose history read failed, delivery follows that store read
directly, with no rule evaluation. The same applies after the lease acquire
when another replica holds the lease.

The deadline does not bound that earlier work, which runs as long as its
queries and store calls take. So the deadline does not bound the whole tick.
A tick that overruns its interval delays the next tick and does not overlap
it. The evaluator does not run on a fixed schedule. After a tick returns, it
sleeps for a jittered interval, up to 10% longer than the configured one.

Notifications that the evaluator did not attempt before the deadline stay
queued and keep their place at the front.
`ravel_alert_notifications_deferred_total` counts them once per notification
per tick. A notification deferred on several consecutive ticks is counted on
each. A rising value has two causes:

- A sink is too slow to drain the queue within a tick.
- The work before delivery already ran past the deadline. Then every
  notification after the first is deferred, even when every sink answers at
  once. Healthy sinks receive one notification per tick until the ticks get
  faster. A slow rule query or a slow store looks like a slow sink here.

A tick both raises new alerts and resolves alerts that stopped matching. So
the worst case for one rule in one tick is twice the cap: up to 1000 new
transitions plus up to 1000 resolutions. One tick publishes up to 2000
records and queues up to 2000 notifications for each sink. That bound is on
what one tick publishes and queues, not on what one tick delivers.

### Delivery order

The evaluator serves the queue in the order that notifications were queued,
not by the age of the transition that they carry.

- A notification that some sink refused goes to the back of the queue after
  the attempt.
- A notification that the deadline never reached keeps its place.
- A notification leaves the queue only when *every* configured sink has
  accepted it.

The pass therefore rotates over the whole queue and does not spend the budget
of every tick on the same few entries. A healthy sink receives every
notification eventually.

While one sink is blackholed, the queue never drains. Total delivery for
*every* sink, healthy ones included, is then throttled to what fits in the
budget of one tick. Watch `ravel_alert_undelivered_notifications`. A queue
that only grows means that one sink refuses notifications and slows every
other sink. Remove a sink that is down from the configuration.

### Retained labels

Every alert record keeps the labels that the rule's query returns, and the
rule's own labels. No erasure path reaches alert history today.

### Upgrade from per-rule alerts

Releases before per-series evaluation raised one alert per rule, with the
rule labels only. Four things change for an existing rules file on upgrade:

- **A notification burst on the first tick.** A PromQL rule gets a new
  identity for each matching series that carries a label, other than
  `__name__`, that the rule labels do not override. If the single rule-level
  alert is pending or firing at upgrade, that tick writes one Resolved
  transition for it and one new transition per matching series. The webhook
  sink receives a notification for every one of them. The Alertmanager sink
  receives every one except a pending transition. A rule with a nonzero `for`
  starts its new alerts pending.
- **Rules that fire today can start failing every tick.** A rule fails with
  `DuplicateAlertIdentity` when two matching series merge to one label set.
  One example is a selector over several metric names (`{__name__=~"a|b"}`)
  whose series differ only in `__name__`. Another is a rule label that
  overrides the series label that told them apart. A rule fails with
  `TooManyAlerts` when more than 1000 series match. On every tick that it
  fails, `ravel_alert_rules_failed_total` rises. After the upgrade, read that
  counter and the warnings of the evaluator.
- **Duplicate rule ids fail startup.** Two rules of one tenant that share a
  `rule_id` stop the process at load with `rule id "<id>" is used by more than
  one rule in tenant "<tenant>"`. Before the upgrade, give each rule its own
  `rule_id`.
- **Repeat notifications multiply by the firing series.** `repeat_interval`
  now applies to each alert, not to the rule. A rule sends one repeat per
  firing series per interval to every sink, where it sent one before. On
  rules that match many series, raise `repeat_interval` or set it to `0s`.

## SQL detection rules

A SQL detection rule reads the same tables that the `POST /api/v1/sql`
endpoint serves (`samples`, `logs`, `spans`, `audit`), under the same
one-signal-per-query rule. See the [query guide](query.md) for the query
languages.

A rule that reads `alerts`, so that an alert fires on other alerts, is not
usable yet. The endpoint can query the table. But the evaluator passes no
consumed generations to the recursion guard. Every record from such a rule
stays at generation 1, and the `max_alert_generation` circuit breaker can
never trip. Until that is wired, write rules against the other four tables.

## Cadence and SQL lookback

- `--alert-eval-interval-secs` (default `60`): how often the evaluator of
  each tenant wakes and evaluates every rule configured for that tenant.
- `--alert-sql-lookback` (default `5m`): the event-time window that the query
  of a SQL detection rule resolves over. The window ends at the clock reading
  of the tick. It bounds only which segments the query lists. The statement's
  own `WHERE` still applies above the scan. A PromQL rule evaluates as an
  instant query and does not use this window.

## Reading back the loaded rules

`GET /api/v1/rules` returns the rules that the process loaded for the calling
tenant, in the shape that Prometheus's rules API uses. It reads
configuration, not data. Use it to confirm that the process parsed the rules
file that you started it with:

```sh
curl -s -H "Authorization: Bearer $RAVEL_TOKEN" \
  http://localhost:4318/api/v1/rules
```

```json
{
  "status": "success",
  "data": {
    "groups": [
      {
        "name": "ravel-alert-rules",
        "file": "",
        "interval": 60.0,
        "rules": [
          {
            "type": "alerting",
            "name": "cpu-hot",
            "query": "max by (instance) (cpu_usage) > 0.9",
            "duration": 300.0,
            "labels": {"severity": "page"},
            "annotations": {"summary": "CPU over 90% for five minutes"},
            "health": "unknown",
            "state": "unknown"
          }
        ]
      }
    ]
  }
}
```

How to read the response:

- The tenant comes from the credential. A token for a tenant with no rules
  gets `{"status": "success", "data": {"groups": []}}`.
- Every rule in the file for one tenant renders in one group named
  `ravel-alert-rules`. Its `interval` is `--alert-eval-interval-secs`. The
  rules file has no group blocks to take a name or a `file` from.
- The `query` of a rule is its whole firing expression. A PromQL rule shows
  its threshold comparison appended to the query text. A SQL detection rule
  shows its statement alone.
- `health` and `state` are `unknown`, and a rule carries no `alerts` array.
  The endpoint serves the loaded rule set and reads no evaluation outcome.
  For what the rules did, query the `alerts` table. See
  [Querying alert history](#querying-alert-history).
- Prometheus's companion `/api/v1/alerts` endpoint is not served.

## Notification sinks

A sink is the destination of a transition. Ravel posts each transition to
every configured sink after it writes the record durably. Every sink flag is
repeatable. There are four kinds of sink:

| Sink | Flag | Value |
|---|---|---|
| Unauthenticated webhook | `--alert-webhook-url URL` | Ravel POSTs each transition as JSON to every configured URL. |
| Unauthenticated Alertmanager | `--alertmanager-url URL` | An Alertmanager base URL (`http://alertmanager:9093`) or its full `/api/v2/alerts` endpoint. Ravel appends the well-known path when it is missing. |
| Authenticated webhook | `--alert-webhook SPEC` | A comma-separated `key=value` spec. |
| Authenticated Alertmanager | `--alertmanager SPEC` | The same spec as `--alert-webhook`. Its `url` can be a base URL or the full `/api/v2/alerts` endpoint. |

A spec requires `url=...` and one credential, no more. The credential is
either `bearer-file=PATH` or `basic-user=NAME,basic-pass-file=PATH`. Ravel
reads the secret from a file, never inline, so the secret never appears in a
process listing.

For a webhook that needs a bearer token whose value is in
`/etc/ravel/hook.token`:

```sh
ravel-server --mode all \
  --alert-rules-file /etc/ravel/alerts.json \
  --alert-webhook 'url=https://hooks.example.com/ravel,bearer-file=/etc/ravel/hook.token' \
  ...
```

The full flag list, with defaults and help, is in
[ravel-server-flags.md](../reference/ravel-server-flags.md).

## Evaluator metrics

The evaluator exports its own figures on `/metrics`. They show a pipeline
that evaluates nothing, writes nothing, or delivers nothing. Without them,
the first sign of a broken evaluator is an alert that never arrived. That
looks the same as a condition that never occurred.

| Metric | Meaning |
|---|---|
| `ravel_alert_rules_evaluated_total` | Rules whose query ran and whose condition was decided. |
| `ravel_alert_rules_failed_total` | Rules skipped because the query, the condition, or the write failed, including a rule over the 1000-alert cap (`TooManyAlerts`) or with two series sharing one alert identity (`DuplicateAlertIdentity`). Each is logged with its `rule_id` and retried next tick. |
| `ravel_alert_records_written_total` | Transition records durably written. |
| `ravel_alert_repeats_queued_total` | Repeat notifications queued for a still-firing alert. A repeat writes no new record. |
| `ravel_alert_notifications_delivered_total` | Notifications accepted by every configured sink. |
| `ravel_alert_notifications_failed_total` | Notifications attempted but not accepted by every sink, counted once per tick while they are retried. |
| `ravel_alert_notifications_deferred_total` | Notifications not attempted in a tick because the per-tick delivery deadline (half the evaluation interval) elapsed first. Counted once per notification per tick: a notification deferred again on the next tick is counted again. They keep their place at the front of the queue. |
| `ravel_alert_undelivered_notifications` | Gauge. Notifications not yet accepted by every configured sink, at most one per alert identity. While a sink keeps failing it grows by one for every identity that transitions, without bound. |
| `ravel_alert_ticks_total` | Evaluation ticks, split by an `outcome` label: `evaluated`, `lease_not_held`, `lease_unavailable`, `history_unavailable`. |
| `ravel_alert_last_tick_completed_timestamp_seconds` | Unix time this process last completed a tick. Its age is the liveness signal. |

`outcome="lease_not_held"` is healthy. Only the replica that holds the alert
lease of a tenant evaluates rules. Every other replica ticks, skips
evaluation, and reports this outcome forever. The two store-failure outcomes
(`lease_unavailable`, `history_unavailable`) are separate from it, so an
alert rule can ignore the steady state.

`ravel_alert_last_tick_completed_timestamp_seconds` is the only figure that
shows a stopped loop. Every counter in the table is cumulative. A dead
evaluator freezes the counters at values that look like a healthy deployment
whose rules never fire. A tick that skipped evaluation because a peer held
the lease still stamps this gauge, because that replica is alive.

A process that built no evaluator (no `--alert-rules-file`, or a file with no
rules) exports none of these metrics, and no row of zeros. The
[observability guide](observability.md#alert-evaluation-ravel_alert_) has
ready-made `for:`-guarded PromQL rules over these series. They include a
dead-loop rule and a notifications-failing-to-every-sink rule.

## Querying alert history

Every transition that an evaluator writes is a row in the `alerts` table.
`POST /api/v1/sql` and Flight SQL serve the table. It is one of the five
tables that the SQL surface exposes (`samples`, `logs`, `spans`, `alerts`,
`audit`). The one-signal-per-query rule applies: a query names one table. A
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

`alert_id` is the stable hash of the rule id and the label set of the alert.
Every record for one alert (one matching series of one rule) carries the same
value across restarts and across rule reloads.

The severity of a record mirrors `state`: firing at the ERROR level, pending
at WARN, resolved and suppressed at INFO. The table exposes no severity
column, so filter on `state`.

`attrs` carries the four promoted keys above, plus the labels of the alert
and the annotations of the rule:

- one entry per alert label under `label.<name>`. These are the series labels
  and the rule labels, merged as
  [One alert per matching series](#one-alert-per-matching-series) describes.
- one entry per annotation under `annotation.<name>`.

Read one entry with a subscript, for example
`attrs['label.instance'] = 'host-1'` or `attrs['annotation.summary']`. The
label and annotation key sets are per-rule and open-ended, so they are a map
and not columns.

### One row per transition

The evaluator writes a record when an alert changes state, never on a tick
that changes nothing. Each transition is one immutable object. The table is
history: it holds what happened and when, and it never holds a current-state
row. Compute the current state with a query.

Each row carries the identity of the write that produced it: `writer_id`,
`writer_epoch`, and `writer_seq`, from the commit record of the object.
`ts_ns` alone is not a total order. Two evaluators can overlap briefly at a
lease handover and write the same `alert_id` at the same `ts_ns`. An order by
`ts_ns DESC, writer_epoch DESC, writer_seq DESC, writer_id DESC` is a total
order, so the query below returns one row per alert.

Do not read that order as causal ordering between evaluators. `writer_epoch`
is a constant today, not a lease term, and the `writer_seq` of each evaluator
restarts at 1. Across a handover, the key picks the record of the departing
evaluator, not the later write. The result is still one current row per
alert. It is the same row that the evaluator's own fold picks, so the table
agrees with the writer.

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

That row carries the `state`, `generation`, labels, and annotations of its
transition together. Filter it by `state` to answer what is true now.

### Which predicates prune

All pushdown is widen-only. DataFusion applies the original `WHERE` predicate
again above the scan, so a query never returns a wrong row. Pruning only
decides how much the query reads.

- `ts_ns` range comparisons (`>=`, `>`, `<`, `<=`, `=`, and `BETWEEN`) fold
  into one time window that prunes objects and blocks.
- `alert_id = '<hex>'` and `rule_id = '<name>'` equality against a string
  literal push into the reader as exact per-record attribute equalities. The
  reader skips blocks whose attribute bloom proves the value absent and
  re-checks every surviving row.

Every other predicate, including any `attrs['k'] = 'v'` subscript, prunes
nothing. DataFusion evaluates it above the scan. The pruning shapes must be
top-level `AND` conjuncts. An `OR` inside a conjunct drops that conjunct from
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

The `rule_id` equality is inside the subquery so that it prunes the scan. The
fold then runs over the records of that rule only.

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

An `alerts` query reads through the same fetcher as the `logs` table. The
tiers of that fetcher cache its bytes: the RAM tier always, and the
local-disk tier when `--cache-dir` is set. The same funnel accounts for them.

Alert records are not folded into the catalog and not compacted. A query
lists the alert commit records of the tenant for its window on every call,
one bounded listing per shard. One object per transition keeps that listing
small.

The maintenance loop sweeps alert history. It deletes a transition older than
`--alert-retention`, 90 days by default: both its commit record and its
object. There is one exception. The current-state record of each alert
identity is kept whatever its age:

- A rule that fires for a year keeps the one record that says so.
- A rule deleted a year ago keeps the one `resolved` record that carries its
  last generation.

An `alerts` query therefore answers for the retention window plus the current
state of every identity. The prefix that a query lists holds the transitions
of one window plus one record per identity, not the whole life of the
deployment.

The sweep runs on the process that owns the alert unit of the tenant, on the
ordinary maintenance tick. It learns which record is the current state of
each identity from the alert state memo of the tenant. The evaluator rewrites
that memo on every tick that it runs. If the memo of a tenant is missing,
unreadable, or too far behind the window, the sweep skips that tenant for
that tick. It never sweeps without that protection.
`ravel_alert_retention_skipped_total` counts those ticks by `reason`. The
[observability guide](observability.md#alert-retention-skips-ravel_alert_retention_skipped_total)
lists each reason and where to look for its remedy.

To change the window, set `--alert-retention`:

- If you need more history, set a longer window before you upgrade.
- To keep every transition forever, set `--alert-retention 0`. Deployments
  did this before the sweep existed.
- Startup refuses a nonzero window shorter than one hour plus the seal margin
  of the memo. The seal margin is three evaluation intervals plus the query
  deadline, so the minimum is 1 h 3 m 30 s at the defaults. Under a shorter
  window the sweep can never run, and every tick reports a skip.

`0` turns off the retention sweep and its memo read only. The orphan sweep of
the alerts shard still runs on the same tick, whatever the window. The
evaluator abandons a transition whose write outlived the `max_flush_lifetime`
of the ingest writers. The orphan sweep reclaims the data object that the
abandoned transition leaves behind. A mass-orphan breaker trip on the alerts
shard counts under
`ravel_maintain_orphan_breaker_tripped_total{signal="alerts"}`, the same
family and alert as the trips of every other signal.

## Background

Per-series evaluation, the identity rule, and the 1000-alert cap are
[ADR-0117](../adrs/0117-per-series-alert-evaluation.md). Pruning the alert
state memo, which per-series rules over churning label sets wait on, is issue
#1438.
