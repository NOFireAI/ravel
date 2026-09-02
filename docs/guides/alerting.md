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

**Alert state transitions are written durably to object storage under their own
signal prefix, and no shipped query surface can read them back.** Neither the
PromQL endpoints nor the SQL tables (`samples`, `logs`, `spans`) expose alert
history. An operator who plans to build a dashboard on Ravel's own alert
history cannot: the transitions are stored for the sinks and for restart
recovery, not for query. Alerts reach you through the sinks below; they do not
become queryable data.

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
      "sql": "select count(*) from logs where has_word(body, 'denied')",
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
    returns at least one row.
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
serves (`samples`, `logs`, `spans`), under the same one-signal-per-query rule.
See the [query guide](query.md) for the query languages themselves.

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
