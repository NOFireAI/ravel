# Ravel Agents

Ravel Agents is a durable agent runtime that keeps every agent's state in
the same object storage bucket as your telemetry. It is distinct from the
[MCP server for AI agents](mcp-server.md), which lets an AI agent host query
Ravel. This page says what an agent is, how to turn the runtime on, and what
works in this release. The [storage protocol](../agents.md) is the normative
reference for every guarantee below.

## What an agent is

An agent is a named, long-lived unit of state that handles messages one at a
time:

- **One durable HEAD per agent.** An agent is named by a kind and a key,
  each a non-empty UTF-8 string of at most 256 bytes. Its whole state hangs off one
  record in the bucket, its HEAD, which is changed only by a conditional
  write. Two writers cannot both win; the loser re-reads and tries again or
  gets a typed error. See [the HEAD record](../agents.md#the-head-record).
- **Messages with receipts.** Sending a message records a receipt keyed by
  your request id. Sending the same request id with the same payload again
  returns the same operation handle instead of a second message; sending it
  with a different payload is refused as `IdempotencyConflict`. See
  [the publication rule](../agents.md#the-publication-rule).
- **Turns.** The agent's handler runs one accepted message at a time, in
  the order the messages were accepted. A turn's new state, its result and
  its events are published together in one write.
- **Steps.** Inside a turn, a handler calls tools through durable steps.
  A completed step replays from its record when the turn is retried, and
  each tool declares what happens after an ambiguous attempt
  (`provider_idempotency`, `at_least_once` or `manual`).
- **Timers.** An agent can schedule a timer that later delivers a message
  to itself. A timer fires once; cancelling it after it fired reports
  `AlreadyFired`.

When a write's outcome is unknown (the store did not answer after the
write), the runtime returns `OutcomeUnknown` with the operation handle, and
you resolve that handle instead of sending again. Every error is one of
eleven [typed errors](../agents.md#typed-errors), each with a retry hint.

## Turning it on

Ravel Agents is off by default and needs two switches:

1. Build `ravel-server` with the `agents` cargo feature:

   ```sh
   cargo build -p ravel-server --features agents
   ```

   The published image does not include it.

2. Run the server with `--agents`, under `--mode all` or `--mode query`. A
   build without the feature refuses `--agents` at startup rather than
   ignoring it, and any other mode refuses it too.

The related flags, all listed in the
[server flag reference](../reference/ravel-server-flags.md):
`--agents-activation-concurrency`, `--agents-poll-concurrency`,
`--agents-memory-budget`, `--agents-max-agents`, `--agents-max-mailbox`,
`--agents-max-receipts`, `--agents-max-state-bytes` and
`--agents-max-timers`. All of them are reported on
`GET /api/v1/agents/health` and `/metrics`. The two concurrency flags bound
the routes' admission pools: a request past its pool answers 503
`store_unavailable`. `--agents-max-mailbox`, `--agents-max-receipts` and
`--agents-max-timers` are the caps every agent's mailbox, receipts and
timers are held to. `--agents-max-agents`, `--agents-max-state-bytes` and
`--agents-memory-budget` are reported only: no route enforces them in this
release.

Access is by three capabilities: `agents` for client operations,
`agents_worker` for workers, and `agents_admin` for reconciliation. A
static token grants them through its suffix (`--tenant-token
'TOKEN=acme;agents'`), and OIDC through `--oidc-agents-claim`,
`--oidc-agents-worker-claim` and `--oidc-agents-admin-claim`. Every client
route requires `agents` and every `worker/` route requires `agents_worker`,
answering 403 without it. No route needs `agents_admin` yet, because
reconciliation has no route.

The runtime stores objects under `t/<tenant>/ag/` in the bucket. The IAM
templates in [deploy/iam](../../deploy/iam/README.md#ravel-agents-tag) grant
that prefix to the gateway and query roles. Because of that grant, `ag` is a
reserved Parquet table name: a table already named `ag` stops resolving,
and `ravel-cli parquet repair --stray` reports its manifests.

## What works today

Ravel Agents arrives in seven milestones. This release has the
contracts, the storage layer and the coordinator, served over HTTP under
`/api/v1/agents/`. Clients use `create`, `send`, `call`, `operation`,
`result`, `resolve`, `cancel`, `state` and `events`. Workers use
`worker/register`, `worker/poll`, `worker/heartbeat` and `worker/commit`.
The [HTTP API reference](../reference/http-api.md#agents-routes) gives each
route's body and status codes. The server's `agents_e2e` tests run these
routes against a real server, for example
`send_then_result_through_real_server_survives_worker_replacement` and
`call_heartbeat_and_cancel_through_real_server`.

| Milestone | What it delivers | In this release |
|---|---|---|
| 0 Contracts | key layout, HEAD format, typed errors, the `agents` feature and `--agents` flag | yes |
| 1 Durable kernel | create, send, read, resolve, turn commit, receipts, quotas | yes, over the client routes; the agent-count, state-size and memory quotas are reported but not enforced |
| 2 Runtime and SDK | workers, ownership and takeover, turn dispatch, the TypeScript SDK | the worker routes, leases, heartbeats and takeover are served; the SDK has no HTTP worker transport yet and runs against its in-process test harness |
| 3 Steps and tools | step journal, effect intents, recovery policies, cancellation | cancellation is served (`cancel`); the step journal, effect intents and recovery policies have no route and are in the transition kernel's tests only |
| 4 Timers and discovery | timer firing, wake hints, the reconciliation sweep | timer firing in the kernel's tests only; no sweep |
| 5 Developer experience | `ravel-cli agents`, inspector page, MCP tools, Rust client, projection into logs and traces | no |
| 6 Release hardening | paged indexes, benchmarks, operator guide, the feature in the published image | no |

The [guarantee table](../agents.md#guarantees-and-limitations) lists each
property with the test that pins it or the milestone that delivers it.

## The TypeScript SDK

The SDK lives in `sdk/typescript/packages/agents`; its
[README](../../sdk/typescript/packages/agents/README.md) shows how to define
an agent and a tool and drive them through the test harness. It is not
published, and its worker has no HTTP transport for the agents routes yet.
