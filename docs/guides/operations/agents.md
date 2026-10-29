# Operating Ravel Agents

This page is for the person who runs `ravel-server --agents`. It covers the
budgets and quotas, the capabilities a token needs, where projected history
goes, what the sweep costs, the clock assumptions, how long one agent can
live, and how to reconcile an effect that is in doubt. The
[Ravel Agents guide](../agents.md) introduces the feature for an application
developer, and the [storage spec](../../agents.md) holds the key layout, the
wire shapes and the table of every guarantee with its test. Each statement
below names the test or spec row that backs it.

- [Turning it on](#turning-it-on)
- [Budgets and quotas](#budgets-and-quotas)
- [Long-lived agents](#long-lived-agents)
- [Capabilities and tokens](#capabilities-and-tokens)
- [Projection](#projection)
- [Sweep cost](#sweep-cost)
- [Several server processes](#several-server-processes)
- [Clock assumptions](#clock-assumptions)
- [Reconciling an effect in doubt](#reconciling-an-effect-in-doubt)

## Turning it on

The published `ravel-server` image is built with the `agents` feature. The
routes mount only when the process also runs with `--agents`, and only in
`--mode all` or `--mode query` (`agents_in_a_non_query_mode_is_refused`,
`agents_router_mounts_on_both_listeners_only_with_feature_and_flag`).
`GET /api/v1/agents/health` needs no token and reports the feature, the mode
and every setting below as the process parsed it
(`health_reports_feature_mode_and_every_setting`). The flag spellings,
environment variables and defaults are generated into the
[server flag reference](../../reference/ravel-server-flags.md); the routes
are in the [HTTP API reference](../../reference/http-api.md).

## Budgets and quotas

Every flag below is parsed into the agents budgets
(`agents_flags_parse_and_reach_query_budgets`) and reported on the health
route and as a settings gauge on `/metrics`
(`health_reports_feature_mode_and_every_setting`). The [metric
families](../observability.md) are listed in the observability guide.

| Flag | Default | What it bounds | At the bound | Metric |
|---|---|---|---|---|
| `--agents-activation-concurrency` | 64 | agents requests other than the three long-poll routes that this process serves at once | 503 `store_unavailable`, "agents admission limit reached"; the request takes no query permit (`admission_refusal_is_503_and_takes_no_query_permit`) | `ravel_agents_activation_concurrency_limit` |
| `--agents-poll-concurrency` | 1024 | waiting `call`, `result` and `worker/poll` requests, a lane of its own so long polls do not hold activation permits | 503 `store_unavailable`, same test | `ravel_agents_poll_concurrency_limit` |
| `--agents-max-mailbox` | 256 | accepted inputs waiting in one agent's mailbox | 429 `mailbox_full`, retry with backoff, `cap` `mailbox` (`a_full_mailbox_is_refused_before_any_write`) | `ravel_agents_max_mailbox`, `ravel_agents_budget_refusals_total{cap="mailbox"}` |
| `--agents-max-receipts` | 1024 | send receipts one agent incarnation keeps; see [Long-lived agents](#long-lived-agents) | 413 `budget_exceeded`, retry never, `cap` `receipts` (`a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`) | `ravel_agents_max_receipts`, `ravel_agents_budget_refusals_total{cap="receipts"}` |
| `--agents-max-timers` | 64 | armed timers held in one agent's HEAD | 413 `budget_exceeded`, `cap` `timers` (`logical_caps_and_the_byte_cap_are_typed`) | `ravel_agents_max_timers`, `ravel_agents_budget_refusals_total{cap="timers"}` |
| `--agents-max-agents` | 100000 | parsed and reported only | nothing is refused | `ravel_agents_max_agents` |
| `--agents-max-state-bytes` | 67108864 | parsed and reported only | nothing is refused | `ravel_agents_max_state_bytes` |
| `--agents-memory-budget` | 268435456 | parsed and reported only | nothing is refused | `ravel_agents_memory_budget_bytes` |

Two caps have no flag. Each list kept in pages (results, events, timers) holds
at most 512 sealed pages of 64 entries, refused with `cap` `index_pages`, and
HEAD holds at most 256 KiB, refused with `cap` `head_bytes`
(`logical_caps_and_the_byte_cap_are_typed`,
`head_stays_under_the_cap_with_every_list_at_max_pages`). A refusal names its
cap in the error body's `cap` field and moves
`ravel_agents_budget_refusals_total{cap}` by exactly one
(`each_cap_refusal_moves_its_counter_by_exactly_one`). No cap drops data: a
refused write leaves HEAD as it was.

The last three rows are deferred: `--agents-max-agents`,
`--agents-max-state-bytes` and `--agents-memory-budget` are parsed and
reported, and no route enforces them. Size a deployment against them as
targets, and watch the request and memory metrics of the process instead.
The [storage spec](../../agents.md#guarantees-and-limitations) carries this
as a deferred row.

`wait_ms` on `call`, `result` and `worker/poll` is capped at 20 s
(`a_waiting_poll_wakes_on_send_and_the_wait_is_capped`), so a waiting
request holds a poll permit for at most that long.

## Long-lived agents

Agent lifetime limit. An agent accepts at most `--agents-max-receipts`
messages per incarnation (1,024 by default), because receipts are kept for
idempotency and do not expire in this release. The next send fails
`budget_exceeded` (HTTP 413, retry never): about 17 hours after creation at
one message a minute, or 17 minutes at one a second
(`a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter` sets the
cap to 4 and is refused at the fifth send). Raising the flag helps only up to
about 23,000 sends at one event a turn, where the 256 KiB HEAD cap binds.
Separately, the results, events and timers lists stop at 32,768 entries each
(512 pages of 64), reached by timer-fired inputs, by 32 events a turn after
about 1,024 turns, or by 32,768 distinct timer keys: the timers list holds
one entry per distinct timer key, and a rescheduled key replaces its earlier
generation rather than adding one
(`a_reschedule_supersedes_the_earlier_generation`). Behind one long-lived
scheduled timer, the limit is about 3,000 distinct timer keys, because those
timers stay in HEAD. Nothing trims these lists; the only recovery is destroy
then create, which starts a new incarnation with empty state and history
(`a_tombstoned_agent_is_gone_until_recreated_under_a_new_incarnation`).

Signals, from the earliest to the most direct:

- `ravel_agents_index_fill_max_ratio{list}` (`list` is `receipts`,
  `results`, `events` or `timers`) and `ravel_agents_head_bytes_max_ratio`:
  the highest fill of any agent this process has seen, from 0 to 1.
- `ravel_agents_agents_near_cap{list}`: how many agents are at 75% of a cap
  or more (`a_directory_cycle_publishes_the_highest_fill_and_the_near_cap_counts`).
- `ravel_agents_budget_refusals_total{cap}` and the error body's `cap` field.
- The state route's `usage` object: `receipts`, `max_receipts`,
  `results_pages`, `events_pages`, `timers_pages`, `max_index_pages`,
  `head_bytes` and `max_head_bytes` for one agent.

The fill gauges are published once per directory cycle of the sweep
(`the_fill_is_published_only_when_a_directory_cycle_completes`), so they can
be up to about two cycles old, and the near-cap count can arrive after the
refusal. The refusal counter and `usage` are updated on the request itself.

Advice:

- Key an agent by something with a bounded lifetime: a conversation, a
  ticket, a day. An agent keyed by a user or a device lives as long as the
  user does and reaches the receipt cap.
- A capped agent still drains its queued work, and its state, results and
  events stay readable, so read what you need before you destroy it.
- A retry with the same request id uses no receipt, because the duplicate is
  answered before the cap is checked
  (`retries_are_idempotent_and_conflicts_are_typed`), and a timer firing uses
  none either.

## Capabilities and tokens

Three capabilities gate the routes
(`every_route_refuses_a_token_lacking_its_capability`,
`capabilities_are_enforced_on_both_listeners`):

| Capability | Routes | Holder |
|---|---|---|
| `agents` | `create`, `send`, `call`, `operation`, `result`, `resolve`, `cancel`, `state`, `events`, `timers`, `timers/cancel`, `effects`, `effects/{effect}`, `inspector` | applications |
| `agents_worker` | every `worker/*` route | the worker processes that run handlers |
| `agents_admin` | `reconcile`, `sweep` | operators |

A token lacking the route's capability gets 403 naming it. Grant them as
suffixes on a static token, for example `--tenant-token 'TOKEN=acme;agents'`,
or map OIDC claims with `--oidc-agents-claim`, `--oidc-agents-worker-claim`
and `--oidc-agents-admin-claim`. Give `agents_admin` to people, and keep it
off application and worker tokens: it decides what an effect in doubt did.

Object store grants: the gateway and query roles list, read and write
`t/*/ag/*` and delete `t/*/ag/_wake/*` only; the maintain role lists and
reads; no other role deletes there
(`agents_namespace_is_granted_to_gateway_and_query_roles_and_read_only_to_maintain`).
The policy statements are in the
[IAM templates](../../../deploy/iam/README.md#ravel-agents-tag). `ag` is a
reserved Parquet table name, so no tenant table can collide with the agents
prefix.

## Projection

Committed agent history is projected into the tenant's logs and traces under
a stable event id, redacted by default
(`committed_turn_is_queryable_as_a_log_with_its_stable_event_id_and_a_projection_outage_changes_no_state`,
`payload_text_is_projected_only_for_an_opted_in_tenant`).
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
endpoint each refuse to start
(`a_process_that_cannot_project_refuses_to_start_and_off_is_reported`,
`agents_projection_target_resolves_or_refuses_naming_both_ways_out`,
`agents_projection_endpoint_is_loopback_http_only`).

The endpoint mode sends no credential: each request names its tenant in the
`x-ravel-tenant` header only, so the receiving gateway must run
`--dev-insecure-tenant-header`. Startup accepts that flag only when both
`--listen-http` and `--listen-grpc` bind a loopback address
(`dev_insecure_tenant_header_on_non_loopback_fails_validate`,
`dev_insecure_tenant_header_on_non_loopback_grpc_fails_validate`), which
makes the endpoint mode a same-host development topology
(`a_query_mode_process_projects_to_a_loopback_gateway`). In production run
the agents process in `--mode all`, or set `--agents-projection off`.

The projector only reads committed history, off the commit path. Its
backlog holds at most `--agents-projection-backlog` records per tenant
(default 4096). At the bound it stops reading and counts, the agents fall
behind, and commits proceed
(`a_full_backlog_counts_and_leaves_the_coordinator_untouched`). A failed
delivery retries after one second under the same ids
(`a_failing_sink_changes_no_agent_state_and_retries_the_same_ids`).

Watch `ravel_agents_projection_lag_max` (committed minus projected events,
the largest over the agents this process has seen) with
`ravel_agents_projection_backlog`, `ravel_agents_projection_backlog_bound`,
`ravel_agents_projection_backlog_full_total`,
`ravel_agents_projection_sink_failures_total`,
`ravel_agents_projection_read_failures_total` and
`ravel_agents_projection_records_rejected_total`. The same figures are under
`projection` on the health route
(`projection_metrics_scrape_matches_health_after_an_outage`). A growing lag
with a rising `backlog_full_total` is backpressure from the sink; a rising
`records_rejected_total` is the log ingest window refusing old records, which
is permanent for those records.

A restart re-projects history from the start under the same ids
(`a_restart_repeats_history_under_the_same_ids`): up to 32,768 duplicate
records per agent per process start, at about 520 GETs for an agent with a
full history. Consumers deduplicate on the `agent.event_id` attribute.

## Sweep cost

The sweep fires due timers and finds agents with work. Each pass is bounded
whatever the number of agents
(`a_bounded_pass_walks_the_directory_in_list_order_across_passes`):

- at most 8 LIST pages and 64 HEAD reads;
- the wake half (minute-bucket hints under `_wake/`) gets 32 of the HEAD
  reads, and the directory half (`_dir/`) gets the rest plus whatever the
  wake half left;
- each half resumes after its cursor and a pass does not wrap, so each
  agent's HEAD is read at most once a pass;
- a timer firing adds its own GET and CAS on top of the cap, and a repair of a
  missing directory entry is at most one PUT per wake HEAD read
  (`a_hint_reached_agent_gets_its_missing_dir_entry_back`).

The interval task runs one pass every 5 s for each tenant this process has
served (`the_interval_sweep_runs_a_pass_per_interval_with_no_poll`). A
waiting poll runs a pass at most once per interval, and its only listing is
that pass (`a_poll_issues_no_unbounded_list`,
`interval_zero_runs_no_periodic_pass_and_polls_sweep_at_the_floor`).

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
`agents_admin`) runs one pass now and returns the same counts for it
(`sweep_route_counts_match_the_store_and_needs_agents_admin`).

## Several server processes

A single process wakes a waiting worker as soon as a send lands, with no
sweep delay (`a_waiting_poll_wakes_on_send_and_the_wait_is_capped`).

With several `--agents` processes on one tenant, a send writes no wake hint.
A send that one process accepts for an agent no process holds is found only
when the sweep's directory cursor reaches that agent on some process. The
guaranteed bound is ceil(N/32) passes at the 5 s interval, for N agents in
the tenant. Measured on a cold store
([benchmark report](../../../benchmarks/agents/REPORT.md), "Discovery"):

| Agents in the tenant | Discovery delay |
|---|---|
| 100 | 11.5 s |
| 1,000 | 2.5 min |
| 10,000 | 26.0 min guaranteed, 13.0 min quiet |

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
`ownership_lost` (`every_fenced_command_refuses_a_stale_fence`,
`a_commit_after_takeover_is_ownership_lost`). Clocks affect timeliness only:

- timer accuracy and takeover speed assume monotonic local clocks and
  bounded skew between processes;
- wake hints are minute buckets, so a hint written by a process whose clock
  runs ahead is kept until it is due (`a_premature_hint_fires_nothing_and_is_kept`),
  and a timer whose hint was lost still fires when the directory half reaches
  it (`a_schedule_whose_hint_was_never_written_still_fires_when_due`);
- the owner's lease time is advisory; only an epoch advance revokes an owner.

The [storage spec](../../agents.md#clocks) has the detail.

## Reconciling an effect in doubt

A step with the `manual` policy whose worker died after the tool may have run
goes in doubt, and its turn blocks until an operator decides
(`manual_blocks_until_reconciled_and_audits_the_decision_once`).

How it looks:

- The owner's next `worker/step` for that key gets 409 with `code`
  `effect_in_doubt` and retry `reconcile`.
- `GET /api/v1/agents/operation?operation=OP` reads `status` `blocked` with
  `blocked_effect` set to the effect id.
- `GET /api/v1/agents/effects/{effect}` reads `status` `in_doubt` at
  `attempt` 1, with `attempts` `[{"attempt":1,"status":"in_doubt"}]`.
  `GET /api/v1/agents/effects?kind=K&key=KEY` lists the agent's effects.
- The inspector (`GET /api/v1/agents/inspector`) shows the effect as
  EffectInDoubt with the two reconcile bodies ready to copy
  (`inspector_is_served_to_agents_tokens_with_every_section`).

The two decisions, each `POST /api/v1/agents/reconcile` with an
`agents_admin` token:

- `record_result`: you checked the outside system and the tool did run.
  Record what it returned; the step replays that result and the turn goes on.
- `retry`: the tool did not run. A new attempt is authorized and the owner's
  next step dispatches it once
  (`a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`).

A token without `agents_admin` gets 403 "requires the agents_admin
capability". With `--audit-mode` on, each decision that changes an effect
writes one `agents_reconcile` audit record naming the tenant and the
capabilities of the caller; in `required` mode a failed audit write refuses
the reconcile with 503. Repeating a decision answers the same body and writes
no second record.

Retry across a restart. The hand-over of a `retry` lives in the memory of the
process that answered it. If that process restarts, or the owner's next step
reaches another process, the effect reads in doubt again; reconcile it again
(`manual_latches_in_doubt_after_a_coordinator_restart_and_is_never_redispatched`).
A takeover by a new worker on the same process keeps the hand-over
(`a_reconcile_retry_is_handed_to_the_owner_after_a_takeover`). Two admins of
one tenant are not told apart in the audit record, so agree on one person per
effect.

Worked example, with `EFF` the effect id from `blocked_effect`:

```sh
# 1. Confirm the block.
curl -s -H "Authorization: Bearer $AGENTS_TOKEN" \
  "http://127.0.0.1:8080/api/v1/agents/effects/EFF"
# {"effect":"EFF","status":"in_doubt","attempt":1,
#  "attempts":[{"attempt":1,"status":"in_doubt"}], ...}

# 2a. The charge went through: record its result.
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"effect":"EFF","attempt":1,"decision":"record_result","result":{"cents":1500}}' \
  http://127.0.0.1:8080/api/v1/agents/reconcile
# {"effect":"EFF","status":"completed","attempt":1}

# 2b. Or the charge did not go through: authorize a second attempt.
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"effect":"EFF","attempt":1,"decision":"retry"}' \
  http://127.0.0.1:8080/api/v1/agents/reconcile
# {"effect":"EFF","status":"authorized","attempt":2}
```

How to verify:

- After `record_result`, the owner's next step for the key answers the step
  `completed` with the recorded output, and the turn commits.
- After `retry`, the operation reads `running`, the owner's next step gets
  `attempt` 2 with step status `started`, and the effect's `attempts` read
  `[{"attempt":1,"status":"superseded"},{"attempt":2,"status":"open"}]`
  (`a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`).
- A second `retry` naming attempt 2 while it is open is 409
  `ownership_lost`, and attempt 0 is 409 `idempotency_conflict`
  (`a_reconcile_retry_of_an_attempt_a_takeover_raised_is_ownership_lost`).

`ravel-cli agents reconcile` posts the same body with the admin token
(`reconcile_posts_the_decision_with_the_admin_token`).
