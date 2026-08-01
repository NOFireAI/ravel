# ADR-0043: Unified alerting engine - observability alerts and detection rules, stored as data

## Context

Epic 3 (program #333), scoped by the user explicitly to cover both
observability alerting (recording/alert rules over PromQL, the
Grafana-adjacent workflow) and security detection (Sigma-style rules
over the `logs` SQL table), with alert state itself stored as durable
data so a rule can read alert history - including a rule that fires on
other alerts. This depends on ADR-0040 (`Signal::Alerts`, RLOG-format
reuse, fold-derived state, the generation-based recursion guard) and
benefits from, but does not require, Epic 2's aggregation operators
(already on main).

No rule-evaluation machinery exists in Ravel today. The closest
structural precedent is `services/ravel-server/src/maintain.rs`'s
`Mode::Maintain`: one background tokio task spawned per tenant
(`spawn`/`run_loop`), ticking on a configurable interval, with a
graceful-shutdown channel per task. This is the pattern a rule
evaluator scheduler mirrors, not a new scheduling mechanism.

## Decision

1. **Rule definition is generic, not PromQL-specific or Sigma-specific.**
   A rule is `{rule_id, query: {lang: "promql"|"sql", text}, condition
   (how the query result maps to firing - e.g. "any series > threshold"
   for PromQL, "row count > 0" for SQL), labels, annotations,
   for_duration (optional pending-before-firing delay, PromQL
   alerting's usual semantics), max_alert_generation override
   (optional, defaults to ADR-0040's global default)}`. This one shape
   covers observability alert rules (PromQL query + threshold),
   recording rules (PromQL query, no condition, always "fires" to
   record a derived series - out of this ADR's v1, see deferred list),
   and detection rules (SQL query over `logs`, condition = nonempty
   result). A Sigma-to-this-shape compiler is additive later work, not
   a second rule engine.
2. **Rules are per-tenant static config for v1**, following the exact
   pattern `--tenant-token`/`--retention-tenant` already use (a CLI
   flag or config file, loaded at startup). A rules-management HTTP API
   (create/update/delete rules at runtime) is explicitly deferred - v1
   proves the evaluation and alerts-as-data model; dynamic rule
   management is a follow-up epic once the eval loop is trusted.
3. **One background evaluator task per tenant**, mirroring
   `maintain.rs::spawn`/`run_loop` exactly: `--alert-eval-interval-secs`
   ticks, each tick evaluates every configured rule for that tenant.
   Ravel's compute processes are disposable (core architecture
   invariant), so per-rule "how long has this been pending" state is
   never held in the evaluator's memory - each tick queries the most
   recent `Signal::Alerts` record for the rule's `alert_id` (fold to
   current state, per ADR-0040) and derives pending-duration from that
   record's timestamp, not from an in-process timer. A restarted
   evaluator resumes correctly from the last written record.
4. **A new record is written only on state transition** (pending ->
   firing, firing -> resolved, firing -> suppressed), not on every tick
   a condition continues to hold - avoiding a heartbeat record per rule
   per tick. "Still firing since Tn" is answered by folding to the last
   transition record, the same way "is this bucket retained" is
   answered by folding retention tombstones rather than a per-tick
   liveness record.
5. **Alerts-on-alerts**: a rule's `query` may target the `alerts` SQL
   table (ADR-0040) the same as `samples` or `logs`. The evaluator
   computes each produced record's `generation` per ADR-0040's formula
   and rejects (typed error, logged, not silently dropped) exceeding
   `max_alert_generation`. This is the only rule type where the query
   result directly names other alert records as input, so it is also
   the only place the generation computation applies; ordinary
   PromQL/SQL rules over metrics/logs always compute `generation = 0`.
6. **Sinks**: a webhook sink (HTTP POST of the transition record as
   JSON) and an Alertmanager-compatible sink (POSTs in Alertmanager's
   own `/api/v2/alerts` payload shape) fire on the same tick a
   transition is written, at-least-once (sinks are expected to be
   idempotent consumers, the same delivery guarantee the rest of
   Ravel's strict-mode acknowledgement already assumes downstream of
   itself). Sink failures are logged and retried next tick from the
   latest record, never block the record from being written - the
   durable record is the source of truth, the sink is a notification,
   not a second commit path.
7. **Query surface**: `alerts` and (Epic 4's) `audit` SQL tables follow
   the exact `logs` table provider/pushdown/scan pattern (ADR-0033),
   reusing `LogSegmentFetcher`'s shape against the shared RLOG-format
   reader (ADR-0040).

## Deferred (explicitly out of this epic's v1)

- **Recording rules** (a PromQL query that always writes a derived
  series back as new metric data, rather than conditionally firing an
  alert). Same evaluator loop, different output signal (`Metrics`, not
  `Alerts`) and a different ingest path (through the existing shard
  actor, not a new record kind) - different enough to be its own
  follow-up rather than bundled into the alert-condition engine.
- **Sigma DSL compiler.** V1 rules are hand-authored SQL/PromQL. A
  compiler translating Sigma YAML into this ADR's generic rule shape
  is additive, high-value, and independently shippable once the
  generic shape is proven - not a precondition for it.
- **Dynamic rules-management API.** Static per-tenant config for v1
  (point 2).
- **Live tail (`/api/v1/tail`)** for hunt workflows. Independent of the
  alerting engine's data model; can be built against `logs` or `alerts`
  once either exists, as its own smaller ticket.

## Rejected alternatives

- **In-memory pending/firing state, checkpointed periodically.**
  Rejected: violates "every compute process is disposable" - an
  evaluator crash between checkpoints would either lose pending-state
  progress or double-fire depending on checkpoint timing. Deriving
  state by fold from durable records has no such window.
- **A record per tick regardless of transition (heartbeat model).**
  Rejected: multiplies storage and compaction load by the tick count
  for no query benefit "is this still firing" already answers by fold;
  Prometheus-alertmanager users do not expect per-tick alert records
  either.
- **Two separate engines for observability alerts and security
  detections**, matching how most vendors ship them as separate
  products. Rejected per the user's explicit direction and because the
  generic rule shape (point 1) already covers both without
  duplicating the scheduler, the fold logic, or the sink code.
- **Exactly-once sink delivery via a second durable outbox table.**
  Rejected: real added complexity (an outbox needs its own retry/dedup
  state) for a guarantee downstream sinks do not need - webhook/
  Alertmanager consumers are already expected to be idempotent, the
  same assumption the rest of the system makes about its own
  strict-mode acknowledgement.

## Consequences

- No new frozen format beyond ADR-0040's `Signal::Alerts` (already
  decided); this ADR is entirely about the evaluation loop, rule
  shape, and sinks built on top of it.
- The evaluator reuses `QueryEngine` (PromQL) and `ravel-sql`'s executor
  (SQL) as libraries, not services it calls over the network - same
  in-process reuse `POST /api/v1/analytics` already established for
  sharing the query engine with a non-query-endpoint feature.
- Rule config lives alongside tenant-token/retention config for v1;
  moving it to a dynamic API later is additive, not a breaking change,
  since the evaluator's internal rule shape does not change.
- Alerts-on-alerts is real but bounded by `max_alert_generation`; an
  operator who needs deeper chains adjusts the config knob, but the
  default protects against an accidental infinite loop from day one.
