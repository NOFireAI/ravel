# @ravel/agents

TypeScript SDK for Ravel Agents, the durable agent runtime described in
ADR-3331 (decision 11). The package name is provisional and the package is
never published (`"private": true` in `package.json`).

## Status and reachability

The SDK runs end to end against the in-process fake coordinator in
`@ravel/agents/testing`, and nowhere else yet:

- No Ravel server serves the agents routes to this client. The HTTP routes
  under `/api/v1/agents/` and the worker transport arrive with tasks T11
  and T12b; until then `createClient` has nothing live to talk to, and
  `serveAgents` has no HTTP `Transport` implementation, only the fake.
- The only way in is the package's own exports; nothing in Ravel imports
  it.
- No CI job runs it yet (task T2 adds the `sdk` job). Run the checks by
  hand with `make test-sdk` and `make lint-sdk` from the repository root.

## Entry points

| Import | Contents |
|---|---|
| `@ravel/agents` | The typed errors and shared types |
| `@ravel/agents/client` | `createClient`: send messages to agents and follow operations |
| `@ravel/agents/worker` | `defineAgent`, `message`, `serveAgents` and the step runner |
| `@ravel/agents/tools` | `defineTool` |
| `@ravel/agents/testing` | `createTestHarness`: in-process agents for tests |

The client entry imports no server-only module: nothing under `worker/`,
`tools/` or `testing/`, and none of `node:child_process`, `node:fs` or
`node:net`. The test `client entry imports no server-only module` in
`src/client/index.test.ts` walks the client entry's import graph and
fails on any of them.

## First run

Define a tool, an agent that calls it through a durable step, and drive
both through the test harness:

```ts
import { z } from "zod";
import { defineTool } from "@ravel/agents/tools";
import { defineAgent } from "@ravel/agents/worker";
import { createTestHarness } from "@ravel/agents/testing";

const price = defineTool({
  name: "catalog.price",
  version: "1",
  input: z.object({ sku: z.string() }),
  output: z.object({ cents: z.number() }),
  recovery: "at_least_once",
  run: async (input, { effectId, attempt, signal }) => ({ cents: 250 }),
});

const order = defineAgent({
  name: "order",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    checkout: message({
      payload: z.object({ sku: z.string(), qty: z.number() }),
      result: z.number(),
      handler: async (ctx, payload) => {
        const quoted = await ctx.step("price", price, { sku: payload.sku });
        const total = quoted.cents * payload.qty;
        ctx.setState({ total: ctx.state.total + total });
        return total;
      },
    }),
  }),
});

const h = createTestHarness({ agents: [order], tools: [price] });
const op = await h.agent(order, "o-1").send("checkout", { sku: "A-1", qty: 2 });
console.log(await op.result()); // 500
await h.close();
```

Against a server, the client side is the same with
`createClient({ url, token }).agent<typeof order>("order", "o-1")`.

## Behaviour and the tests that pin it

Worker and steps (`src/worker/`):

- A handler's state is a draft. Neither its state changes nor its emitted
  events are visible until the turn commits, and a failed turn publishes
  neither. Tests: `state is a draft published only in the turn's commit`,
  `a failing handler publishes neither state nor events, and dead-letters
  after its attempts` (`steps.test.ts`).
- `ctx.step(key, tool, input)` journals each step before running it. After
  a crash, a completed step is replayed from the journal without calling
  the tool again, and the turn commits once. Test: `replay after
  crashWorker with two completed steps replays both without calling run
  and commits once` (`replay.test.ts`).
- The runner decides from the journal's current record, not from the
  transcript the lease carried: a step the lease saw as started but whose
  result landed since then replays that result. Test: `P1 a manual tool
  completed by a dead worker after takeover is not run again`
  (`review.test.ts`).
- A tool's result or failure that arrives after another attempt settled
  the step is kept as evidence only, and the step returns the journal's
  result. Test: `a late tool result is kept as evidence and the step
  returns the journal's result`.
- A tool that throws is journaled as the step's failure before the turn
  commits as failed, and a retried turn gets that failure back as
  `StepFailed` instead of running the tool again. Test: `P11 a tool failure
  is journaled as the step's result before the failed commit, and
  replayed` (`review.test.ts`).
- An error a tool throws, typed or not, reaches the handler and does not
  block the operation; only the runner's own `NonDeterministicReplay` and
  `EffectInDoubt`, and a coordinator refusal, block it. Test: `P9 a tool
  that throws a typed RavelAgentsError does not wedge the operation`
  (`review.test.ts`).
- A step key reused with a different tool name, tool version or input hash
  fails `NonDeterministicReplay` and blocks the operation; nothing commits.
  Tests: `reused step key with a different input hash fails
  NonDeterministicReplay` (`replay.test.ts`), `P3 a tool version bump under
  a reused step key fails NonDeterministicReplay` (`review.test.ts`).
- The input hash covers the input's JSON form (`toJSON` applied, as
  `JSON.stringify` does), so a different `Date` is a different input. Any
  value that has no plain JSON form (a `Map`, `Set`, `bigint`, `undefined`,
  function, non-finite number or class instance) is refused with
  `InvalidStepInput` before the tool runs. Tests: `P10 a different Date
  under the same step key is detected` (`review.test.ts`), `canonical JSON
  refuses %s` (`identity.test.ts`), `a step input that is not plain JSON is
  refused with InvalidStepInput before its tool runs, and fails terminally
  uncaught`.
- One step at a time per turn: a second `ctx.step` while one is in flight
  rejects with `ConcurrentStepError`, which fails the operation terminally
  if the handler does not catch it. Tests: `a second step started while one
  is in flight is refused with ConcurrentStepError`, `an uncaught
  ConcurrentStepError fails the operation terminally, without retries`.
- `ctx.now()` and `ctx.random()` are the same on every attempt at one
  input. Test: `recorded time and seeded random are the same on every
  attempt at one input`.
- A turn whose handler version or handler digest the worker does not match
  is abandoned with `IncompatibleHandler`. Tests: `a worker without the
  leased handler version abandons the turn with IncompatibleHandler`, `a
  worker whose handler digest differs from the leased one abandons the turn
  with IncompatibleHandler`.

Unless a test file is named, the tests above are in `steps.test.ts`.

Tools (`src/tools/`) take one of three recovery policies, and any other
value is a `TypeError` (test: `refuses a recovery policy outside the
three`). Every run receives the effect id, the attempt number and an abort
signal (test: `a tool receives the effect id, attempt and signal`).

| Policy | After a crash mid-run | Test |
|---|---|---|
| `at_least_once` | Runs again under the same effect id, attempt + 1 | `an at_least_once tool interrupted by a crash runs again under the same effect id` |
| `provider_idempotency` | Runs again under the same effect id; the provider dedupes on it | `a tool receives the effect id, attempt and signal` (the id it would dedupe on) |
| `manual` | Does not run again: a result recorded since replays, and an attempt with no result blocks the operation with `EffectInDoubt` | `a manual tool interrupted by a crash blocks the operation with EffectInDoubt`, `P1 a manual tool completed by a dead worker after takeover is not run again` |

Client (`src/client/`):

- Every retry of one request reuses its request id and request hash, so the
  coordinator answers it as the same request. Retried: network failures,
  untyped 5xx answers, and typed errors whose hint is `backoff`. Tests: `a
  retry after a transient 503 reuses the same request id and hash`, `an
  untyped 5xx is retried the same way`, `retries are bounded: an exhausted
  store_unavailable surfaces as StoreUnavailable`.
- A send whose outcome is unknown throws `OutcomeUnknown` carrying a
  handle. `handle.result()` first asks the coordinator to resolve the
  request id and resends the same body only when it never landed. Tests:
  `a send whose response misses its deadline resolves to the operation it
  created`, `a send that never landed is resent under the same request id
  once resolve finds nothing`.
- A `result()` deadline throws `OutcomeUnknown` carrying the handle; an
  aborted wait does not cancel the operation. Tests: `a result() deadline
  throws OutcomeUnknown carrying the handle`, `an aborted result() wait
  leaves the operation running`.
- A failed or cancelled operation rejects `result()` and `call()` with
  `OperationFailed`. Tests: `a failed operation rejects result() and call()
  with OperationFailed`, `cancel is idempotent and a cancelled operation
  rejects result()`.
- `handle.events()` yields only that operation's events, waits while it
  runs, ends once it is terminal and the first page past it has been read,
  and resumes from any event's `cursor`. A cursor names the last delivered
  event sequence, so resuming with a different page size repeats nothing.
  Tests in `events.test.ts`, among them `a cursor is the last delivered
  event seq, so a different page size resumes without duplicates` and `a
  terminal operation's events end at the first page past it, not at an
  empty page`.

Wire (`src/wire/`): every golden fixture under `sdk/fixtures/` decodes and
re-encodes to the same bytes, and every body schema has a fixture
(`wire.test.ts`). The identity derivations match the fixtures and the
Rust kernel's `identity.rs` (`identity.test.ts`).

## Errors

The eleven errors of ADR-3331 decision 11 subclass `RavelAgentsError`. Each
carries a `code` equal to its class name, the safe ids `ids` (agent,
incarnation, request, operation, turn, step, effect, timer, epoch; never a
payload), and a `retry` hint. `src/errors.test.ts` asserts the set of
classes and each class's code and hint; `maps every ErrorBody code to its
typed error` (`client.test.ts`) asserts the wire mapping.

| Wire code | Class | `retry` |
|---|---|---|
| `agent_gone` | `AgentGone` | `never` |
| `idempotency_conflict` | `IdempotencyConflict` | `never` |
| `outcome_unknown` | `OutcomeUnknown` | `resolve` |
| `ownership_lost` | `OwnershipLost` | `never` |
| `incompatible_handler` | `IncompatibleHandler` | `never` |
| `non_deterministic_replay` | `NonDeterministicReplay` | `never` |
| `effect_in_doubt` | `EffectInDoubt` | `reconcile` |
| `mailbox_full` | `MailboxFull` | `backoff` |
| `budget_exceeded` | `BudgetExceeded` | `never` |
| `store_unavailable` | `StoreUnavailable` | `backoff` |
| `unsupported_format` | `UnsupportedFormat` | `never` |

An unknown code, or a 2xx body that does not match its schema, reads as
`UnsupportedFormat` (test: `an unknown error code or an off-schema response
reads as UnsupportedFormat`).

Two more errors are local to the SDK and have no wire code: they are raised
in the worker and never cross the wire. Both subclass `RavelAgentsError`
with `retry` `never` (test: `the SDK-local errors are typed and never
retried`), and an uncaught one fails the operation terminally (the
`ConcurrentStepError` and `InvalidStepInput` tests in `steps.test.ts`).

| Class | Raised when |
|---|---|
| `ConcurrentStepError` | A handler starts a step while another is in flight |
| `InvalidStepInput` | A step input has no plain JSON form |

A handler that catches a failed step's error sees `StepFailed`, on the run
that recorded the failure and on every retry that replays it; it carries
the step key and the recorded error text (test: `P11 a tool failure is
journaled as the step's result before the failed commit, and replayed`).

## The test harness is not durable

`createTestHarness` runs a fake coordinator in process memory. It models the
coordinator's protocol (leases, epochs, step journal, retries, dead letters,
blocked operations), not its storage: every agent, operation, step and
event is lost on `close()` or when the process exits. `crashWorker()`
revokes the worker's leases and starts a fresh worker against the same
fake; it simulates a worker crash, not a coordinator or store crash.
`advanceTime(ms)` is the only thing that moves the fake's clock. The fake
reads wall-clock time only to bound the long-poll waits in `poll` and
`waitTerminal`, and `delayNext` holds a response back with a timer; none of
these changes state. The fake refuses a failed commit while a step's
attempt is authorized without a result, as the kernel does (test: `P11 the
fake refuses FailTurn while a step's attempt is authorized without a
result`, `review.test.ts`). A test that passes against the harness says the
handler behaves under replay; it says nothing about what Ravel persists.

## Wire gaps

The wire contract this package was written against has gaps, filled
provisionally on the SDK side:

- No route records a step, so `Transport.recordStep` has no HTTP mapping.
  Its answer carries a `failure` field, and `Transport.failEffect` records
  a tool's failure; both are SDK-local shapes. The wire `StepState` has
  only `started` and `completed`, so a failed step reads as `started` on
  the wire.
- No recorded turn time or random seed travels on the wire. `ctx.now()`
  reads the poll's `startedUnixNs`, local to the SDK, and `ctx.random()` is
  seeded from the agent id, incarnation and accept sequence.
- The canonical encoding of step input is unspecified; the SDK hashes
  sorted-key JSON of the input's JSON form and refuses anything that is not
  plain JSON.
- u64 counters are numbers capped at `Number.MAX_SAFE_INTEGER` and refused
  past it (test: `a u64 counter past the safe-integer range is refused,
  not rounded`). Nanosecond timestamps are strings refused outside the i64
  range (test: `a timestamp outside i64 is refused on decode and encode`).

## Checks

Run from `sdk/typescript/` after `npm ci`:

- `npm run lint`: ESLint with the `typescript-eslint` strict type-checked
  rules.
- `npm run typecheck`: `tsc` in strict mode over `src/` and `typetests/`.
- `npm test`: the vitest suites under `src/`.

`typetests/` holds compile-time negative tests. Each `// @ts-expect-error`
case (an invalid handler signature, an invalid payload type, a step input
that does not match its tool, and others) must fail to compile. If a type
change lets one compile, tsc reports the unused directive as error TS2578,
and `npm run typecheck` fails.
