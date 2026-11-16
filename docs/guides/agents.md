# Ravel Agents

Ravel Agents runs long-lived agents and keeps each agent's state in the same
object storage bucket as your telemetry. It is separate from the
[MCP server for AI agents](mcp-server.md), which lets an AI agent host query
Ravel. The [storage protocol](../agents.md) is the normative reference for
every guarantee on this page.

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

   The published `ravel-server` image is built with it; this step is for a
   source build.

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
`--agents-memory-budget` are reported only: no route enforces them.

Access is by three capabilities: `agents` for client operations,
`agents_worker` for workers, and `agents_admin` for reconciliation and
destroying an agent. A
static token grants them through its suffix (`--tenant-token
'TOKEN=acme;agents'`), and OIDC through `--oidc-agents-claim`,
`--oidc-agents-worker-claim` and `--oidc-agents-admin-claim`. Every client
route requires `agents` and every `worker/` route requires `agents_worker`,
answering 403 without it. The `reconcile`, `sweep` and `destroy` routes
require `agents_admin`, answering 403 without it.

The runtime stores objects under `t/<tenant>/ag/` in the bucket. The IAM
templates in [deploy/iam](../../deploy/iam/README.md#ravel-agents-tag) grant
that prefix to the gateway and query roles. Because of that grant, `ag` is a
reserved Parquet table name: a table already named `ag` stops resolving,
and `ravel-cli parquet repair --stray` reports its manifests.

## What you can use

All routes are under `/api/v1/agents/`. The
[HTTP API reference](../reference/http-api.md#agents-routes) gives each
route's body and status codes.

| Surface | What it does |
|---|---|
| Client routes | `create`, `send`, `call`, `operation`, `result`, `resolve`, `cancel`, `state`, `events`, `timers`, `effects` |
| Worker routes | `worker/register`, `worker/poll`, `worker/heartbeat`, `worker/commit`, `worker/step`, `worker/effects/complete`, `worker/effects/fail` |
| Admin routes | `reconcile` settles an effect in doubt, `sweep` runs one discovery pass now, `destroy` ends an agent's incarnation |
| Inspector | an embedded HTML page that shows one agent's state, timers, effects and events to an `agents` token |
| `ravel-cli agents` | `dev`, `scaffold`, `list`, `inspect`, `operation`, `events`, `effects`, `reconcile`, `destroy` |
| Rust client | `ravel-agents-client`: one method per client route, with typed failures; it never retries on its own |
| MCP tools | agent tools on the MCP server, in a build with the `mcp` feature and only with `--mcp --agents` |
| Projection | committed agent history appears in the tenant's logs and traces, redacted by default |
| TypeScript SDK | define agents and tools, serve them from a worker, send them messages; see [below](#the-typescript-sdk) |

Current limits:

- **Agent lifetime.** One incarnation accepts at most `--agents-max-receipts`
  sends, and its results, events and timers lists stop at a fixed size.
  Nothing trims them. An agent at a cap stays there until an `agents_admin`
  caller destroys it and it is created again. See
  [Long-lived agents](operations/agents.md#long-lived-agents).
- **Discovery across processes.** A send writes no wake hint. With several
  server processes on one tenant, a worker polling a different process from
  the one that accepted the send waits for that process's sweep to reach the
  agent. See the [operator guide](operations/agents.md#several-server-processes).
- **Quotas.** The agent-count, state-size and memory flags are reported but
  not enforced.

The [guarantees and limitations matrix](../agents.md#guarantees-and-limitations)
lists each behaviour with its status and tests. The
[operator guide](operations/agents.md) covers budgets, tokens, projection,
reconciling and destroying.

## The TypeScript SDK

The SDK lives in `sdk/typescript/packages/agents` and is not published to
npm: a project links it from a checkout of this repository. Its worker
connects to a server with `createHttpTransport`, which records every step and
effect result on the server, so a worker that replaces a killed one in
another process replays the same steps.
[The SDK guide](agents-sdk.md) takes a first project from a scaffold to a
run against a server, and the package
[README](../../sdk/typescript/packages/agents/README.md) is the API
reference.
