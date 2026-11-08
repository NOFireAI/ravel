# Operating Ravel Agents

This page is for the person who runs `ravel-server --agents`. It covers the
budgets and quotas, the capabilities a token needs, where projected history
goes, what the sweep costs, the clock assumptions, how long one agent can
live, how to reconcile an effect that is in doubt, and how to destroy an
agent. The
[Ravel Agents guide](../agents.md) introduces the feature for an application
developer, and the [storage spec](../../agents.md) holds the key layout, the
wire shapes and the table of every guarantee with its test.

- [Turning it on](#turning-it-on)
- [Budgets and quotas](#budgets-and-quotas)
- [Long-lived agents](#long-lived-agents)
- [Capabilities and tokens](#capabilities-and-tokens)
- [Projection](#projection)
- [Sweep cost](#sweep-cost)
- [Several server processes](#several-server-processes)
- [Clock assumptions](#clock-assumptions)
- [Reconciling an effect in doubt](#reconciling-an-effect-in-doubt)
- [Destroying an agent](#destroying-an-agent)

## Turning it on

The published `ravel-server` image is built with the `agents` feature. The
routes mount only when the process also runs with `--agents`, and only in
`--mode all` or `--mode query`.
`GET /api/v1/agents/health` needs no token and reports the feature, the mode
and every setting below as the process parsed it. The flag spellings,
environment variables and defaults are generated into the
[server flag reference](../../reference/ravel-server-flags.md); the routes
are in the [HTTP API reference](../../reference/http-api.md).

## Budgets and quotas

Every flag below is reported on the health route and as a settings gauge on
`/metrics`. The [metric families](../observability.md) are listed in the
observability guide.

| Flag | Default | What it bounds | At the bound | Metric |
|---|---|---|---|---|
| `--agents-activation-concurrency` | 64 | agents requests other than the three long-poll routes that this process serves at once | 503 `store_unavailable`, "agents admission limit reached"; the request takes no query permit | `ravel_agents_activation_concurrency_limit` |
| `--agents-poll-concurrency` | 1024 | waiting `call`, `result` and `worker/poll` requests, a lane of its own so long polls do not hold activation permits | 503 `store_unavailable` | `ravel_agents_poll_concurrency_limit` |
| `--agents-max-mailbox` | 256 | accepted inputs waiting in one agent's mailbox | 429 `mailbox_full`, retry with backoff, `cap` `mailbox` | `ravel_agents_max_mailbox`, `ravel_agents_budget_refusals_total{cap="mailbox"}` |
| `--agents-max-receipts` | 1024 | send receipts one agent incarnation keeps; see [Long-lived agents](#long-lived-agents) | 413 `budget_exceeded`, retry never, `cap` `receipts` | `ravel_agents_max_receipts`, `ravel_agents_budget_refusals_total{cap="receipts"}` |
| `--agents-max-timers` | 64 | armed timers held in one agent's HEAD | 413 `budget_exceeded`, `cap` `timers` | `ravel_agents_max_timers`, `ravel_agents_budget_refusals_total{cap="timers"}` |
| `--agents-max-agents` | 100000 | parsed and reported only | nothing is refused | `ravel_agents_max_agents` |
| `--agents-max-state-bytes` | 67108864 | parsed and reported only | nothing is refused | `ravel_agents_max_state_bytes` |
| `--agents-memory-budget` | 268435456 | parsed and reported only | nothing is refused | `ravel_agents_memory_budget_bytes` |

Two caps have no flag. Each list kept in pages (results, events, timers) holds
at most 512 sealed pages of 64 entries, refused with `cap` `index_pages`, and
HEAD holds at most 256 KiB, refused with `cap` `head_bytes`. A refusal names
its cap in the error body's `cap` field and moves
`ravel_agents_budget_refusals_total{cap}` by exactly one. No cap drops data: a
refused write leaves HEAD as it was.

No route enforces `--agents-max-agents`, `--agents-max-state-bytes` or
`--agents-memory-budget`. Size a deployment against them as targets, and
watch the request and memory metrics of the process instead. The
[storage spec](../../agents.md#guarantees-and-limitations) carries this as a
deferred row.

`wait_ms` on `call`, `result` and `worker/poll` is capped at 20 s, so a
waiting request holds a poll permit for at most that long.

## Long-lived agents

Agent lifetime limit. An agent accepts at most `--agents-max-receipts`
messages per incarnation (1,024 by default), because receipts are kept for
idempotency and do not expire. The next send fails
`budget_exceeded` (HTTP 413, retry never): about 17 hours after creation at
one message a minute, or 17 minutes at one a second. Raising the flag helps
only up to about 23,000 sends at one event a turn, where the 256 KiB HEAD cap
binds (an estimate from the HEAD byte budget; past it the refusal names cap
`head_bytes`).
Separately, the results, events and timers lists stop at 32,768 entries each
(512 pages of 64), reached by timer-fired inputs, by 32 events a turn after
about 1,024 turns, or by 32,768 distinct timer keys: the timers list holds
one entry per distinct timer key, and a rescheduled key replaces its earlier
generation rather than adding one. Behind one long-lived scheduled timer, the
limit is about 3,000 distinct timer keys, because those timers stay in HEAD
(an estimate from the HEAD byte budget; past it the refusal names cap
`head_bytes`). Nothing trims these lists. An agent at a cap stays there until
an `agents_admin` caller destroys it with `POST /api/v1/agents/destroy` and it
is created again under a new incarnation, which starts every list empty; see
[Destroying an agent](#destroying-an-agent).

Signals, from the earliest to the most direct:

- `ravel_agents_index_fill_max_ratio{list}` (`list` is `receipts`,
  `results`, `events` or `timers`) and `ravel_agents_head_bytes_max_ratio`:
  the highest fill of any agent this process has seen, from 0 to 1.
- `ravel_agents_agents_near_cap{list}`: how many agents are at 75% of a cap
  or more.
- `ravel_agents_budget_refusals_total{cap}` and the error body's `cap` field.
- The state route's `usage` object: `receipts`, `max_receipts`,
  `results_pages`, `events_pages`, `timers_pages`, `max_index_pages`,
  `head_bytes` and `max_head_bytes` for one agent.

The fill gauges are published once per directory cycle of the sweep, so they
can be up to about two cycles old, and the near-cap count can arrive after the
refusal. The refusal counter and `usage` are updated on the request itself.

Advice:

- Key an agent by something with a bounded lifetime: a conversation, a
  job, a session. An agent keyed by a user or a device lives as long as the
  user does and reaches the receipt cap.
- An agent at the receipt cap still runs the inputs already in its mailbox,
  and its state, results and events stay readable. At a list or HEAD byte
  cap a completion that would grow a list is refused `budget_exceeded`, and
  the handler can still fail the turn.
- A retry with the same request id uses no receipt, because the duplicate is
  answered before the cap is checked. A timer firing uses no receipt.

## Capabilities and tokens

Three capabilities gate the routes:

| Capability | Routes | Holder |
|---|---|---|
| `agents` | `create`, `send`, `call`, `operation`, `result`, `resolve`, `cancel`, `state`, `events`, `timers`, `timers/cancel`, `effects`, `effects/{effect}`, `inspector` | applications |
| `agents_worker` | every `worker/*` route | the worker processes that run handlers |
| `agents_admin` | `reconcile`, `sweep`, `destroy` | operators |

A token lacking the route's capability gets 403 naming it. Grant them as
suffixes on a static token, for example `--tenant-token 'TOKEN=acme;agents'`,
or map OIDC claims with `--oidc-agents-claim`, `--oidc-agents-worker-claim`
and `--oidc-agents-admin-claim`. Give `agents_admin` to people, and keep it
off application and worker tokens: it decides what an effect in doubt did,
and it destroys agents.

Object store grants: the gateway and query roles list, read and write
`t/*/ag/*` and delete `t/*/ag/_wake/*` only; the maintain role lists and
reads; no other role deletes there.
The policy statements are in the
[IAM templates](../../../deploy/iam/README.md#ravel-agents-tag). `ag` is a
reserved Parquet table name, so no tenant table can collide with the agents
prefix.

## Projection

Committed agent history is projected into the tenant's logs and traces under
a stable event id, redacted by default.
`--agents-projection-payload-tenant` (repeatable) opts a tenant into payload
text. The [storage spec](../../agents.md#projection) has the record shape.

Choose the mode by where the process can deliver records:

| Process | `--agents-projection` | `--agents-projection-endpoint` | Records go to | Health `mode` |
|---|---|---|---|---|
| `--mode all` | `auto` (default) | unset | this process's own ingest | `in_process` |
| any | `auto` | plain `http://` on a loopback host | that gateway over OTLP gRPC | `endpoint` |
| `--mode query` | `auto` | unset | refuses to start, naming both ways out | none |
| any | `off` | unset | nowhere | `off` |

An endpoint on another host, any `https://` endpoint, and `off` with an
endpoint each refuse to start.

The endpoint mode sends no credential: each request names its tenant in the
`x-ravel-tenant` header only, so the receiving gateway must run
`--dev-insecure-tenant-header`. Startup accepts that flag only when both
`--listen-http` and `--listen-grpc` bind a loopback address, which makes the
endpoint mode a same-host development topology. In production run the agents
process in `--mode all`, or set `--agents-projection off`.

The projector only reads committed history, off the commit path. Its
backlog holds at most `--agents-projection-backlog` records per tenant
(default 4096). At the bound it stops reading and counts, the agents fall
behind, and commits proceed. A failed delivery retries after one second under
the same ids.

Watch `ravel_agents_projection_lag_max` (committed minus projected events,
the largest over the agents this process has seen) with
`ravel_agents_projection_backlog`, `ravel_agents_projection_backlog_bound`,
`ravel_agents_projection_backlog_full_total`,
`ravel_agents_projection_sink_failures_total`,
`ravel_agents_projection_read_failures_total` and
`ravel_agents_projection_records_rejected_total`. The same figures are under
`projection` on the health route. A growing lag with a rising
`backlog_full_total` is backpressure from the sink; a rising
`records_rejected_total` is the log ingest window refusing old records, which
is permanent for those records.

The projected logs and traces are a view of agent history, not its record.
The record is the agent's committed history in the bucket: the events route
and the inspector read it in full, including events on sealed pages, and
nothing trims it. Reconcile decisions are written to the audit pipeline, not
to the projection. So a record the log ingest window refuses leaves a counted
gap in the logs, never in the history. To fill such a gap after an outage,
raise `--max-ingest-lag` past the outage's length and restart the process:
the restart re-projects each agent's history under the same ids, so records
the window now admits arrive and the ones already stored are duplicates you
drop on `agent.event_id`. `--max-ingest-lag` is a process setting: it widens
the window for every tenant's telemetry this process ingests, not only for
agents, so lower it again once the gap is filled.

A restart re-projects history from the start under the same ids: up to
32,768 duplicate records per agent per process start, at about 520 GETs for
an agent with a full history. Consumers deduplicate on the `agent.event_id`
attribute.

## Sweep cost

The sweep fires due timers and finds agents with work. Each pass is bounded
whatever the number of agents:

- at most 8 LIST pages and 64 HEAD reads;
- the wake half (minute-bucket hints under `_wake/`) gets 32 of the HEAD
  reads, and the directory half (`_dir/`) gets the rest plus whatever the
  wake half left;
- each half resumes after its cursor and a pass does not wrap, so each
  agent's HEAD is read at most once a pass;
- a timer firing adds its own GET and CAS on top of the cap, and a repair of a
  missing directory entry is at most one PUT per wake HEAD read.

The interval task runs one pass every 5 s for each tenant this process has
served. A waiting poll runs a pass at most once per interval, and its only
listing is that pass.

So the per-pass cost is constant: a quiet pass costs 64 HEAD GETs and 2 LIST
pages. What grows with the number of agents N is the number of passes in one
directory cycle, ceil(N/64) when quiet, and so the time to reach every agent
once; the per-request cost does not grow
([benchmark report](../../../benchmarks/agents/REPORT.md), "Discovery").

Metrics: `ravel_agents_sweep_passes_total`,
`ravel_agents_sweep_list_pages_total`, `ravel_agents_sweep_head_gets_total`,
`ravel_agents_sweep_hints_read_total`, `ravel_agents_sweep_hints_deleted_total`,
`ravel_agents_sweep_timers_fired_total`,
`ravel_agents_sweep_agents_loaded_total`,
`ravel_agents_sweep_dir_entries_repaired_total`,
`ravel_agents_sweep_directory_cycles_total`,
`ravel_agents_sweep_last_directory_cycle_ms` and
`ravel_agents_sweep_interval_ms`. `POST /api/v1/agents/sweep` (needs
`agents_admin`) runs one pass now and returns the same counts for it.

## Several server processes

A single process wakes a waiting worker as soon as a send lands, with no
sweep delay.

With several `--agents` processes on one tenant, a send writes no wake hint.
The process that accepts a send loads the agent and wakes only its own
waiting polls. When the matching worker polls another process, that process
finds the input only when its sweep's directory cursor reaches the agent. The
guaranteed bound is ceil(N/32) passes at the 5 s interval, for N agents in
the tenant. Measured on a cold store
([benchmark report](../../../benchmarks/agents/REPORT.md), "Discovery"):

| Agents in the tenant | Guaranteed (wake half saturated) | Quiet |
|---|---|---|
| 100 | 11.5 s | 5.5 s |
| 1,000 | 2.5 min | 75.5 s |
| 10,000 | 26.0 min | 13.0 min |

So the delay stays under 1 minute up to 384 agents per tenant and under 10
minutes up to 3,840. The sweep interval and the per-pass budgets have no
flag, so there is nothing to tune: keep a tenant's agents below the envelope
you need, or route a tenant's sends and workers to one process. Watch
`ravel_agents_sweep_directory_cycles_total` and
`ravel_agents_sweep_last_directory_cycle_ms`: the latter is the time the last
full directory cycle took, the delay a send can wait.

## Clock assumptions

Safety rests on the store's conditional writes alone: a fenced command with
a stale fence is refused, and a commit after a takeover is
`ownership_lost`. Clocks affect timeliness only:

- timer accuracy and takeover speed assume monotonic local clocks and
  bounded skew between processes;
- wake hints are minute buckets, so a hint written by a process whose clock
  runs ahead is kept until it is due, and a timer whose hint was lost still
  fires when the directory half reaches it;
- the owner's lease time is advisory; only an epoch advance revokes an owner.

The [storage spec](../../agents.md#clocks) has the detail.

## Reconciling an effect in doubt

A step with the `manual` policy whose worker died after the tool may have run
goes in doubt, and its turn blocks until an operator decides. This runbook
covers effects a worker records through `worker/step`. The TypeScript SDK
records every step through that route, so its in-doubt steps appear on the
effects route, in the inspector and to reconcile like any other, and the
operation reads `blocked`. The SDK's client reconciles with an `agents_admin`
token (`client.reconcile`); see the [SDK guide](../agents-sdk.md).

How it looks:

- The owner's next `worker/step` for that key gets 409 with `code`
  `effect_in_doubt` and retry `reconcile`.
- `GET /api/v1/agents/operation?operation=OP` reads `status` `blocked` with
  `blocked_effect` set to the effect id.
- `GET /api/v1/agents/effects/{effect}` reads `status` `in_doubt` at
  `attempt` 1, with `attempts` `[{"attempt":1,"status":"in_doubt"}]`.
  `GET /api/v1/agents/effects?kind=K&key=KEY` lists the agent's effects.
- The inspector (`GET /api/v1/agents/inspector`) shows the effect as
  EffectInDoubt with the two reconcile bodies ready to copy.

The two decisions, each `POST /api/v1/agents/reconcile` with an
`agents_admin` token:

- `record_result`: you checked the outside system and the tool did run.
  Record what it returned; the step replays that result and the turn goes on.
- `retry`: the tool did not run. A new attempt is authorized and the owner's
  next step dispatches it once.

A token without `agents_admin` gets 403 "requires the agents_admin
capability". With `--audit-mode` on, each decision that changes an effect
writes one `agents_reconcile` audit record naming the tenant and the
capabilities of the caller; in `required` mode a failed audit write refuses
the reconcile with 503. Repeating a decision answers the same body and writes
no second record. The record names `agents.principal.subject` and
`agents.principal.subject_source` when the caller's token or claim carries a
subject (a `subject=LABEL` entry on the admin's `--tenant-token` pair, or the
claim `--oidc-subject-claim` names); two admins whose tokens carry none are
not told apart.

Retry across a restart. The hand-over of a `retry` lives in the memory of the
process that answered it. If that process restarts, or the owner's next step
reaches another process, the effect reads in doubt again; reconcile it again.
A takeover by a new worker on the same process keeps the hand-over.

After a 500 `outcome_unknown` from reconcile. The server could not tell
whether its own write took effect, so it hands the attempt to nobody.
An effect whose `status` reads `authorized` in
`GET /api/v1/agents/effects/{effect}` does not tell you whether any process
holds the attempt: another reconcile's process may, or none may. Either way
the attempt is dispatched at most once. Wait for the owner's next step. If
the effect then reads `in_doubt` again, the attempt may or may not have run:
a process that held it may have dispatched it to a worker that lost the turn
before reporting. Treat it as any effect in doubt: check the outside system,
then `record_result` if the tool ran or `retry` if it did not.

Worked example, with `EFF` the effect id from `blocked_effect`:

```sh
# 1. Confirm the block.
curl -s -H "Authorization: Bearer $AGENTS_TOKEN" \
  "http://127.0.0.1:4318/api/v1/agents/effects/EFF"
# {"effect":"EFF","status":"in_doubt","attempt":1,
#  "attempts":[{"attempt":1,"status":"in_doubt"}], ...}

# 2a. The charge went through: record its result.
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"effect":"EFF","attempt":1,"decision":"record_result","result":{"cents":1500}}' \
  http://127.0.0.1:4318/api/v1/agents/reconcile
# {"effect":"EFF","status":"completed","attempt":1}

# 2b. Or the charge did not go through: authorize a second attempt.
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"effect":"EFF","attempt":1,"decision":"retry"}' \
  http://127.0.0.1:4318/api/v1/agents/reconcile
# {"effect":"EFF","status":"authorized","attempt":2}
```

How to verify:

- After `record_result`, the owner's next step for the key answers the step
  `completed` with the recorded output, and the turn commits.
- After `retry`, the operation reads `running`, the owner's next step gets
  `attempt` 2 with step status `started`, and the effect's `attempts` read
  `[{"attempt":1,"status":"superseded"},{"attempt":2,"status":"open"}]`.
- A second `retry` naming attempt 2 while it is open is 409
  `ownership_lost`, and attempt 0 is 409 `idempotency_conflict`.

`ravel-cli agents reconcile` posts the same body with the admin token. A
refusal exits non-zero and its stderr line names the ids the error carries
after the retry hint, for example `agents request refused: CODE (REASON),
retry HINT; ids: agent_id=AGENT_ID, effect_id=EFF_ID`, where `EFF_ID` is the
effect's id rather than the handle you passed. The error carries no attempt;
the attempt in question is the `--attempt` you sent. With `--json` the error
body itself is printed on stdout; a 401 or 403 is not a refusal body and
prints no JSON on stdout under `--json`. The ids appear in wire field order.

## Destroying an agent

When to use it: an agent has reached a lifetime cap (see
[Long-lived agents](#long-lived-agents)), or its state is wrong beyond
repair and the application can start it over. Destroying ends the current
incarnation; a later `create` under the same kind and key starts a new one
with nothing carried over.

What it does. `POST /api/v1/agents/destroy` with an `agents_admin` token
tombstones the agent's HEAD in one write. From then on the agent and every
handle minted under that incarnation answer 404 `agent_gone`: `state`,
`operation`, `result` and `reconcile`, and for the worker holding the turn,
`worker/heartbeat`, effect results and `worker/commit`. A client waiting
in `result` on one of its operations is answered `agent_gone` at once
rather than at its deadline.

What it does not do. It deletes no object: a HEAD is never deleted, and the
tombstone is a new HEAD state. It does not reach outside systems: a tool
call a worker already started may still finish there, and its result is
refused `agent_gone`. Before destroying an agent with effects in doubt or
authorized, list them with `GET /api/v1/agents/effects?kind=K&key=KEY` and
check the outside system for each, since nothing will reconcile them
afterwards.

The answer counts what the tombstone abandoned: `inputs` accepted and never
consumed (the running turn's input among them), whether a turn was open,
the effects in doubt and authorized, and the timers still scheduled.

```sh
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"agent":{"kind":"order","key":"o-1"},"incarnation":1}' \
  http://127.0.0.1:4318/api/v1/agents/destroy
# {"agent_id":"AGENT_ID","incarnation":1,"destroyed":true,
#  "abandoned":{"inputs":2,"active_turn":true,"effects_in_doubt":0,
#               "effects_authorized":1,"timers_scheduled":1}}
```

`incarnation` is optional. Name the incarnation you inspected (the state
route's `incarnation`) so that a destroy sent late cannot end an incarnation
someone created since.

`ravel-cli agents destroy --kind K --key KEY --incarnation N --url URL
--token T` posts the same body with the admin token and prints the
abandoned counts, or the response itself with `--json`.

Repeats and refusals:

- Destroying an agent that is already destroyed answers 200 with
  `destroyed` false and the same counts. Nothing is written and no audit
  record is added.
- An agent that was never created, or an `incarnation` that is not the
  current one, is 404 `agent_gone`; for a pinned incarnation, `ids.incarnation`
  names it. Nothing is written and nothing is audited.
- A 500 `outcome_unknown` with retry `backoff` means the server could not
  tell whether the tombstone landed. Send the same destroy again with the
  `incarnation` from the error's `ids.incarnation`. If the first write
  landed, the retry answers 200 with `destroyed` false.
  `ravel-cli agents destroy` exits non-zero and prints the ids at the end
  of its stderr line, so pass the `incarnation=N` from `agents request
  refused: outcome_unknown (...), retry backoff; ids: agent_id=AGENT_ID,
  incarnation=N` as `--incarnation N`. With `--json` the error body is
  printed on stdout and `ids.incarnation` is in it.
- A token without `agents_admin` gets 403 "requires the agents_admin
  capability".

Audit. With `--audit-mode` on, a destroy that tombstones the agent first
writes one audit record with `kind` `agents_destroy` naming the tenant, the
agent id, the incarnation, the `abandoned` counts as read before the write
(fields `agents.abandoned.inputs`, `agents.abandoned.active_turn`,
`agents.abandoned.effects_in_doubt`, `agents.abandoned.effects_authorized`
and `agents.abandoned.timers_scheduled`), and the caller under
`agents.principal.tenant` and `agents.principal.capabilities`. The record
names `agents.principal.subject` and `agents.principal.subject_source` when
the caller's token or claim carries a subject; two admins whose tokens carry
none are not told apart, as for reconcile. In `required` mode a failed audit
write refuses the destroy with 503 `store_unavailable` and the agent stays
active; in `best-effort` mode the destroy goes ahead without its record.
Without `--audit-mode` no record is written.

Recreate. `POST /api/v1/agents/create` with the same kind and key answers
`incarnation` 2 and `created` true. The new incarnation starts with no
state, no mailbox and no receipts, so a `request_id` used before the
destroy is accepted as a new send; handles from incarnation 1 stay
`agent_gone`.
