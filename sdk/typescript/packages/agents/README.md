# @ravel/agents

TypeScript SDK for Ravel Agents, the durable agent runtime described in
ADR-3331 (decision 11). The package name is provisional and the package is
never published (`"private": true` in `package.json`).

## Status and reachability

The SDK runs against the in-process fake coordinator in
`@ravel/agents/testing` and against a live `ravel-server --agents`:

- `ravel-server --agents` serves the client and worker routes under
  `/api/v1/agents/`, and the wire schemas here match their bodies (the
  golden fixtures in `sdk/fixtures/` are written by the Rust side).
  `createClient` maps a 401 or 403 to `Forbidden`.
- `createHttpTransport` (`@ravel/agents/worker`) is the `Transport` that
  `serveAgents` uses against a server. The server mounts no step or effect
  route yet, so steps go to an in-memory step journal (see "Wire gaps").
  Its behaviour is pinned under "HTTP transport" below.
- The end-to-end tests in `e2e/` start a built `ravel-server` and kill a
  worker inside a step (see "End-to-end tests").
- The only way in is the package's own exports; nothing in Ravel imports
  it.
- CI's `sdk` job (`scripts/gates-sdk.sh`) runs `npm ci`, `npm run lint`,
  `npm run typecheck` and `npm test`, and nothing else: it builds no
  server, so `npm run test:e2e` runs locally only. No nightly workflow runs
  it either. `make test-sdk` and `make lint-sdk` from the repository root
  run the unit checks.

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
- The input hash covers the input's JSON form (`toJSON` applied, an
  `undefined` member dropped and an `undefined` array item written as
  `null`, as `JSON.stringify` does), so a different `Date` is a different
  input and `{ cursor: undefined }` is the same input as `{}`. Any value
  that has no plain JSON form (a `Map`, `Set`, `bigint`, function,
  non-finite number or class instance, or `undefined` as the whole input)
  is refused with `InvalidStepInput` before the tool runs. Tests: `P10 a
  different Date under the same step key is detected` (`review.test.ts`),
  `the JSON form drops undefined members and writes undefined items as
  null, as JSON.stringify does` and `canonical JSON refuses %s`
  (`identity.test.ts`), `a step input that is not plain JSON is
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

HTTP transport (`src/transport/http.ts`; tests in `http.test.ts`):

- Each worker route is posted with the worker's bearer token, and the poll
  wait is clamped to 20 s. Tests: `posts each worker route with the bearer
  token`, `clamps the poll wait to 20 s and leaves a shorter one alone`.
- An `ErrorBody` maps to the class its code names, a typed 503 to
  `StoreUnavailable`, an untyped 5xx or a request with no answer to
  `OutcomeUnknown`, and an off-schema 2xx or an untyped 4xx to
  `UnsupportedFormat`. Tests: `maps an ErrorBody to the class its code
  names, with its safe ids`, `maps a typed 503 to StoreUnavailable rather
  than OutcomeUnknown`, `maps an untyped 5xx and a request with no answer
  to OutcomeUnknown`, `maps an off-schema 2xx and an untyped 4xx to
  UnsupportedFormat`.
- A step heartbeats the lease before it is journaled, and is refused for a
  fenced lease or a turn the transport never polled. Tests: `heartbeats the
  lease before journaling a step and derives ids from incarnation and
  accept_seq`, `refuses a step for a fenced lease without touching the
  journal`, `refuses a step for a turn it never polled`.
- That per-step heartbeat is the only one in a turn: nothing is sent while a
  tool runs. Test: `heartbeats once before each step and sends nothing while
  a tool runs`.
- A 401 or 403 in the server's plain body maps to `Forbidden` with its
  status. Test: `maps the plain %i body to Forbidden with its status`.
- The server polls an agent that has never committed with a null state and
  `base_state_version` 0; the turn starts from the agent's `initialState`.
  Test: `starts a never-committed agent's turn from initialState when the
  server polls a null state`.

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

- Pending routes (task T16): the server mounts no step, effect complete,
  effect fail, effect list or reconcile route, and no client or
  `agents_admin` route reconciles an effect. Until they land,
  `createHttpTransport` serves `recordStep`, `completeEffect` and
  `failEffect` from a `MemoryStepJournal`, which lives in one process and
  is lost with it. This is why the end-to-end kill is an in-process abort.
  Tests: `authorizes a new attempt at an unsettled at_least_once step and
  replays a settled one`, `puts an unsettled manual step in doubt until a
  reconcile records its result`, `hands a reconciled retry out exactly
  once` (`http.test.ts`).
- While an effect is in doubt the server still reports the operation as
  `running`, since no route tells it the operation is blocked (asserted in
  `worker killed inside a manual step blocks as EffectInDoubt in the
  in-memory journal until reconciled there`, `e2e/recover.test.ts`).
- Over `createHttpTransport` the lease is heartbeated only before each step
  (see "HTTP transport" above), so a single tool run, or the handler code
  between two steps, longer than the 30 s lease loses the turn to another
  worker.
- `agent.state()` answers `state: null` for an agent that has never
  committed, which its type `S` does not admit. The golden
  `worker_poll_response` fixture carries a non-null state at
  `base_state_version` 0, while the server polls such an agent with a null
  one.
- The wire `StepState` has only `started` and `completed`, so a failed step
  would read as `started` on the wire.
- A handler pin on the wire (`HandlerPinBody`) names the handler, version
  and digest but no agent kind, and the kernel matches a pin by handler
  name alone. Two agents at one version sharing a handler name are told
  apart only by the digest, which the fake coordinator matches on (test:
  `two agents at one version sharing a handler name are each leased their
  own turn`, `steps.test.ts`); whether the server matches on the digest
  too is not pinned by any server test.
- The polled lease carries the turn's `recorded_unix_ns` and `random_seed`.
  `ctx.now()` reads the recorded time and `ctx.random()` is seeded from the
  random seed, so a retried turn sees the same values (test: `turn random
  seed matches the worker_poll_response fixture's lease`,
  `identity.test.ts`).
- The canonical encoding of step input is unspecified; the SDK hashes
  sorted-key JSON of the input's JSON form and refuses anything that is not
  plain JSON.
- u64 counters are numbers capped at `Number.MAX_SAFE_INTEGER` and refused
  past it (test: `a u64 counter past the safe-integer range is refused,
  not rounded`). Nanosecond timestamps are strings refused outside the i64
  range (test: `a timestamp outside i64 is refused on decode and encode`).

## End-to-end tests

`npm run test:e2e` (from `sdk/typescript/`) runs `e2e/recover.test.ts`
against a `ravel-server` it starts itself, with `--agents` on free local
ports. `npm test` never runs it. Build the server first:

```sh
cargo build --locked -p ravel-server --features agents
```

The binary is `RAVEL_SERVER_BIN`, else
`$CARGO_TARGET_DIR/debug/ravel-server`, else `target/debug/ravel-server`
under the repository root. The store is `RAVEL_E2E_STORE`: `memory`
(default, `MemoryStore`) or `rustfs` (the compose RustFS store, which
needs `docker compose`; the run stops the RustFS container afterwards
only if it started it). Each test waits out one 30 s lease and its 10 s
grace before worker B can take the turn over. Tests for the path and
store choice, which need no server and run under `npm test`: `resolves
the repository root to the directory holding the workspace Cargo.toml`,
`looks for the server under the repository's target unless told
otherwise`, `needs docker compose only for the rustfs store`
(`e2e/plan.unit.test.ts`).

Each test kills worker A inside a step, after the tool's side effect and
before its result is recorded, resends under the same request id, and
lets worker B take the turn over. The server shows that the resend
resolves to the same operation id, that a second worker takes the turn
over, and that the turn commits once. Step replay, effect attempts and the
`EffectInDoubt` block come from the SDK's in-memory step journal (see
"Wire gaps"), and the test titles say so:

- `worker killed inside an at_least_once step recovers the same operation
  on restart, replaying steps from the in-memory journal`: the resend
  resolves to the same operation id, `kb.search` runs once and is
  replayed, `kb.draft` runs twice under one effect id (attempts 1 and 2),
  and the operation completes with the drafted answer.
- `worker killed inside a manual step blocks as EffectInDoubt in the
  in-memory journal until reconciled there`: worker B abandons the turn
  with `EffectInDoubt` without running `desk.ticket` again, and the
  operation completes with ticket `T-1001` once the journal's effect is
  reconciled with the ticket the killed run filed. The reconcile goes
  through the journal, not an `agents_admin` route.

A missing prerequisite skips both tests and names it, both in a warning at
the top of the run and in the suite title the verbose reporter prints:

```
e2e skipped: ravel-server binary not found at <path>; build it with `cargo build --locked -p ravel-server --features agents` or set RAVEL_SERVER_BIN
e2e skipped: RAVEL_E2E_STORE=rustfs needs `docker compose`, which is not installed or not running
```

A skipped run exits 0. With `RAVEL_E2E_REQUIRE=1` a missing prerequisite
fails the run instead, with the same reason, so a job that must exercise
the server cannot pass with nothing run (test: `skips on a missing
prerequisite, and fails instead under RAVEL_E2E_REQUIRE=1`, and test:
`names the resolved binary path when it is missing`).

## Demo

`demo/agents-kill-and-recover.sh`, from the repository root, starts the
compose RustFS store (or `MemoryStore` with
`RAVEL_AGENTS_DEMO_STORE=memory`) and a built `ravel-server --agents`, runs
the same two scenarios through `examples/assistant/kill-and-recover.ts`,
asserts each printed figure against an exact value or a stated band, and
exits 1 on any miss. It needs `npm ci` in `sdk/typescript/` and node 22.15
or later 22.x: the example's loader hook (`examples/ts-hooks.js`) uses
`module.registerHooks`, added in Node 22.15.0, which is why the `engines`
floor in `sdk/typescript/package.json` is 22.15.0. See
`examples/assistant/README.md`.

## Checks

Run from `sdk/typescript/` after `npm ci`:

- `npm run lint`: ESLint with the `typescript-eslint` strict type-checked
  rules.
- `npm run typecheck`: `tsc` in strict mode over `src/`, `typetests/`,
  `e2e/` and `examples/`.
- `npm test`: the vitest suites under `src/`, and `e2e/*.unit.test.ts`,
  which need no server.
- `npm run test:e2e`: the end-to-end tests above.

`typetests/` holds compile-time negative tests. Each `// @ts-expect-error`
case (an invalid handler signature, an invalid payload type, a step input
that does not match its tool, and others) must fail to compile. If a type
change lets one compile, tsc reports the unused directive as error TS2578,
and `npm run typecheck` fails.
