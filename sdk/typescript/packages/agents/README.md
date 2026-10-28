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
- A step key reused with a different tool, version or input hash fails
  `NonDeterministicReplay` and blocks the operation; nothing commits. Test:
  `reused step key with a different input hash fails
  NonDeterministicReplay` (`replay.test.ts`).
- One step at a time per turn: a second `ctx.step` while one is in flight
  rejects with `ConcurrentStepError`. Test: `a second step started while
  one is in flight is refused with ConcurrentStepError`.
- `ctx.now()` and `ctx.random()` are the same on every attempt at one
  input. Test: `recorded time and seeded random are the same on every
  attempt at one input`.
- A turn whose handler version the worker lacks is abandoned with
  `IncompatibleHandler`. Test: `a worker without the leased handler
  version abandons the turn with IncompatibleHandler`.

Tools (`src/tools/`) take one of three recovery policies, and any other
value is a `TypeError` (test: `refuses a recovery policy outside the
three`). Every run receives the effect id, the attempt number and an abort
signal (test: `a tool receives the effect id, attempt and signal`).

| Policy | After a crash mid-run | Test |
|---|---|---|
| `at_least_once` | Runs again under the same effect id, attempt + 1 | `an at_least_once tool interrupted by a crash runs again under the same effect id` |
| `provider_idempotency` | Runs again under the same effect id; the provider dedupes on it | `a tool receives the effect id, attempt and signal` (the id it would dedupe on) |
| `manual` | Does not run again; the operation blocks with `EffectInDoubt` | `a manual tool interrupted by a crash blocks the operation with EffectInDoubt` |

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
  runs, ends once it is terminal, and resumes from any event's `cursor`.
  Tests in `events.test.ts`.

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

## The test harness is not durable

`createTestHarness` runs a fake coordinator in process memory. It models the
coordinator's protocol (leases, epochs, step journal, retries, dead letters,
blocked operations), not its storage: every agent, operation, step and
event is lost on `close()` or when the process exits. `crashWorker()`
revokes the worker's leases and starts a fresh worker against the same
fake; it simulates a worker crash, not a coordinator or store crash.
`advanceTime(ms)` is the only thing that moves the fake's clock; the fake
reads no wall-clock time (`delayNext` holds a response back with a timer
but changes no state). A test that passes against the harness says the
handler behaves under replay; it says nothing about what Ravel persists.

## Wire gaps

The wire contract this package was written against has gaps, filled
provisionally on the SDK side:

- No route records a step, so `Transport.recordStep` has no HTTP mapping.
- No recorded turn time or random seed travels on the wire. `ctx.now()`
  reads the poll's `startedUnixNs`, local to the SDK, and `ctx.random()` is
  seeded from the agent id, incarnation and accept sequence.
- The canonical encoding of step input is unspecified; the SDK hashes
  sorted-key JSON.
- u64 counters are numbers capped at `Number.MAX_SAFE_INTEGER` and refused
  past it (test: `a u64 counter past the safe-integer range is refused,
  not rounded`).

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
