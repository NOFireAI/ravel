# The Ravel Agents TypeScript SDK

The SDK in `sdk/typescript/packages/agents` (package `@ravel/agents`, not
published) defines agents and tools, serves them from a worker, and sends
them messages from a client. This page takes a first project from an empty
directory to a passing test suite, then to a running server, and lists what
the SDK covers and what it does not. Every statement about behaviour names
the test that pins it; the tests live under
`sdk/typescript/packages/agents/src/` unless another path is given, and the
package [README](../../sdk/typescript/packages/agents/README.md) lists the
rest.

## First run

You need node 22.15 or later 22.x and a checkout of this repository.

1. Install the SDK's dependencies and write a starter project into a new
   directory:

   ```sh
   cd sdk/typescript
   npm ci
   node packages/agents/bin/ravel-agents-scaffold.js ../../../my-agent
   ```

   Run the bin by its path. `npm ci` does not link it into
   `node_modules/.bin`, because the lockfile does not list a bin for this
   workspace package, and `npx ravel-agents-scaffold` would look the name
   up in the public npm registry instead, so never use the bare name.

   The project holds one agent, `order`, whose `checkout` handler prices a
   line through a durable step against a local price-list fixture, the
   client types generated from its handler schemas, a client typed by them,
   a vitest suite and its `package.json`, `tsconfig.json` and README. It
   links `@ravel/agents` from your checkout through a relative `file:`
   dependency. A directory that is not empty is refused and nothing is
   written. Tests: `the bin writes a project, exits 1 on a non-empty
   directory and 64 on bad usage`, `a directory holding any other file is
   refused and nothing is written` (`scaffold/index.test.ts`).

2. Run its tests:

   ```sh
   cd ../../../my-agent
   npm install
   npm test
   ```

   They run the agent against the SDK's in-process fake coordinator, so they
   need no server, no network and no API key. They cover a checkout, an
   unknown SKU, a worker killed inside the price step, and the generated
   client types matching the agent. Test: `scaffold output typechecks and
   its tests pass without an API key` (`scaffold/index.test.ts`), which
   typechecks a generated project and runs its suite with every `*_API_KEY`
   variable removed.

3. Run it against a server. Turn the runtime on as
   [the Ravel Agents guide](agents.md#turning-it-on) describes, with one
   token for the client capability `agents` and one for the worker
   capability `agents_worker`:

   ```sh
   cargo run -p ravel-server --features agents -- \
     --store memory --agents --tenant-hash-unkeyed \
     --tenant-token 'clienttoken=acme;agents' \
     --tenant-token 'workertoken=acme;agents_worker'
   ```

   Then, in the project:

   ```sh
   RAVEL_AGENTS_TOKEN=clienttoken RAVEL_WORKER_TOKEN=workertoken npm start -- A-1 2
   ```

   It serves the agent with a worker over the HTTP transport, sends
   `checkout` to a new order, and prints the operation id, the line and the
   total. No test runs `npm start`; the SDK's end-to-end test
   `worker killed inside an at_least_once step recovers the same operation
   on restart, replaying steps from the in-memory journal`
   (`sdk/typescript/packages/agents/e2e/recover.test.ts`) drives the same
   client and worker calls against a built server.

After changing a payload or result schema, `npm run contract` regenerates
the client types. The generated test `contract.ts is the generator's output
for the agent` fails until you do. The generator is `generateContract` in
`@ravel/agents/contract`; a contract generated from the repository's example
assistant typechecks against the agent's own types, and one that disagrees
fails. Tests: `the contract generated from the example assistant typechecks
against the agent's own types`, `a contract that disagrees with the agent
fails that typecheck` (`scaffold/contract.test.ts`).

## Typed errors

Every error the SDK raises from the wire subclasses `RavelAgentsError` and
carries a `code` equal to its class name, the safe ids of what it concerns
(never a payload), and a `retry` hint. The client maps each wire code to its
class. Test: `maps every ErrorBody code to its typed error`
(`client/client.test.ts`). The Test column names, first, the test that pins
the class's code and hint (`errors.test.ts`), then one that raises it.

| Wire code | Class | `retry` | Tests |
|---|---|---|---|
| `agent_gone` | `AgentGone` | `never` | `AgentGone carries its code, retry hint and safe ids`; `nothing outlives close: a new harness has never seen the first one's agent` |
| `idempotency_conflict` | `IdempotencyConflict` | `never` | `IdempotencyConflict carries its code, retry hint and safe ids`; `the same request id with a different payload fails IdempotencyConflict` |
| `outcome_unknown` | `OutcomeUnknown` | `resolve` | `OutcomeUnknown carries its code, retry hint and safe ids`; `a result() deadline throws OutcomeUnknown carrying the handle` |
| `ownership_lost` | `OwnershipLost` | `never` | `OwnershipLost carries its code, retry hint and safe ids`; `leases carry the recorded time and seed, and heartbeats extend a held lease only` |
| `incompatible_handler` | `IncompatibleHandler` | `never` | `IncompatibleHandler carries its code, retry hint and safe ids`; `a worker without the leased handler version abandons the turn with IncompatibleHandler` |
| `non_deterministic_replay` | `NonDeterministicReplay` | `never` | `NonDeterministicReplay carries its code, retry hint and safe ids`; `reused step key with a different input hash fails NonDeterministicReplay` |
| `effect_in_doubt` | `EffectInDoubt` | `reconcile` | `EffectInDoubt carries its code, retry hint and safe ids`; `a manual tool interrupted by a crash blocks the operation with EffectInDoubt` |
| `mailbox_full` | `MailboxFull` | `backoff` | `MailboxFull carries its code, retry hint and safe ids`; `maps every ErrorBody code to its typed error` |
| `budget_exceeded` | `BudgetExceeded` | `never` | `BudgetExceeded carries its code, retry hint and safe ids`; `maps every ErrorBody code to its typed error` |
| `store_unavailable` | `StoreUnavailable` | `backoff` | `StoreUnavailable carries its code, retry hint and safe ids`; `retries are bounded: an exhausted store_unavailable surfaces as StoreUnavailable` |
| `unsupported_format` | `UnsupportedFormat` | `never` | `UnsupportedFormat carries its code, retry hint and safe ids`; `an unknown error code or an off-schema response reads as UnsupportedFormat` |

`Forbidden` has no wire code: the server refuses a credential with HTTP 401
or 403, which the SDK maps to `Forbidden` with `retry` `never`, and a worker
whose poll is refused stops. Tests: `the auth refusal is typed, carries its
HTTP status and is never retried` (`errors.test.ts`), `a 401 or 403 with the server's plain
error body is Forbidden, not retried` (`client/client.test.ts`), `a
Forbidden poll stops the worker and rejects done with it`
(`worker/serve.test.ts`). `ConcurrentStepError` and `InvalidStepInput` are
raised in the worker and never cross the wire. Test: `the SDK-local errors
are typed and never retried` (`errors.test.ts`).

## The testing export

`createTestHarness` in `@ravel/agents/testing` runs a fake coordinator, a
client over it and one worker, all in process memory. Unless a file is
named, the tests below are in `testing/harness.test.ts`.

What the fake honours:

- Leases carry the turn's recorded time and seed, a heartbeat extends a
  held lease, and a heartbeat on a revoked lease fails `OwnershipLost`.
  Test: `leases carry the recorded time and seed, and heartbeats extend a
  held lease only` (`testing/transport.test.ts`).
- After `crashWorker()`, completed steps replay from the fake's journal
  without calling the tool, and the turn commits once. Test: `replay after
  crashWorker with two completed steps replays both without calling run and
  commits once` (`worker/replay.test.ts`).
- A `manual` tool interrupted by a crash blocks the operation with
  `EffectInDoubt`. Test: `a manual tool interrupted by a crash blocks the
  operation with EffectInDoubt` (`worker/steps.test.ts`).
- `cancel` on a running operation applies when its turn commits, and the
  cancelled operation rejects `result()`. Test: `cancel is idempotent and a
  cancelled operation rejects result()` (`client/client.test.ts`).
- `advanceTime(ms)` moves the fake's clock, and the next lease records the
  moved time. Test: `leases carry the recorded time and seed, and
  heartbeats extend a held lease only` (`testing/transport.test.ts`).

What it does not do:

- It has no timers, and its clock never expires a lease. Test: `advancing
  the fake's clock past a lease's expiry neither takes the turn away nor
  runs anything`.
- It has no reconcile route, so an operation in doubt stays blocked. Test:
  `an operation blocked with EffectInDoubt stays blocked: no clock advance
  or new worker reconciles it`.
- It keeps nothing past `close()`. Test: `nothing outlives close: a new
  harness has never seen the first one's agent`.

To drive a kill and a recovery, hold a tool's run open on a promise the
test controls, wait until it has started, call `crashWorker()`, then
release it: the killed worker's turn is reported as abandoned and a fresh
worker takes the turn over. Tests: `an operation blocked with EffectInDoubt
stays blocked: no clock advance or new worker reconciles it`, and in a
scaffolded project `a worker killed inside the price step is replaced and
the turn commits once`, which `scaffold output typechecks and its tests
pass without an API key` runs.

A test that passes against the harness shows how the handler behaves under
replay. It shows nothing about what Ravel stores.

## HTTP transport limits

`createHttpTransport` in `@ravel/agents/worker` connects a worker to a
server. Its tests are in `transport/http.test.ts`.

- The poll wait is clamped to 20 s. Test: `clamps the poll wait to 20 s and
  leaves a shorter one alone`.
- The lease is heartbeated once before each step and never while a tool
  runs. Test: `heartbeats once before each step and sends nothing while a
  tool runs`. So one tool run, or handler code between two steps, that
  outlasts the server's lease loses the turn to another worker. The lease
  is 30 s by default: the agents runtime's `RuntimeConfig::default` sets
  `lease_ms` to 30000.
- The server serves no step or effect route yet, so steps are journaled in
  the worker's own memory and lost with its process. Test: `authorizes a
  new attempt at an unsettled at_least_once step and replays a settled
  one`.
- A manual step left without a result is in doubt until a reconcile through
  that in-memory journal records one. Test: `puts an unsettled manual step
  in doubt until a reconcile records its result`.
- An untyped 5xx, a request with no answer, or a body that fails mid-read
  is `OutcomeUnknown`. Tests: `maps an untyped 5xx and a request with no
  answer to OutcomeUnknown`, `maps a response body that fails mid-read to
  OutcomeUnknown`.

## What the SDK does not do yet

- `ravel-cli` has no scaffold command; the package's bin, run by its path
  as in the first run above, is the entry point. Test: `the bin writes a
  project, exits 1 on a non-empty directory and 64 on bad usage`.
- Steps and effects are not stored on the server, so a worker restart
  loses the step journal and a kill across processes cannot replay a step.
  Test: `authorizes a new attempt at an unsettled at_least_once step and
  replays a settled one` (`transport/http.test.ts`).
- No route reconciles an effect in doubt, and while one is in doubt the
  server reports the operation as running. Test: `worker killed inside a
  manual step blocks as EffectInDoubt in the in-memory journal until
  reconciled there` (`e2e/recover.test.ts`).
- The fake coordinator has no timers. Test: `advancing the fake's clock
  past a lease's expiry neither takes the turn away nor runs anything`.
- The package is not published; a project links it from a checkout. Test:
  `the generated package.json links this package and pins the workspace's
  versions` (`scaffold/index.test.ts`).
