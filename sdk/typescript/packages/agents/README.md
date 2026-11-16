# @ravel/agents

TypeScript SDK for Ravel Agents, the durable agent runtime in `ravel-server`.
Define agents and tools, serve them from a worker, and send them messages
from a client. Run the same code against the in-process fake coordinator in
`@ravel/agents/testing` or against a live `ravel-server --agents`. The
[SDK guide](../../../../docs/guides/agents-sdk.md) is the tutorial; this
page is the API reference.

The package is not published to npm (`"private": true` in `package.json`),
and its name is provisional. A project links it from a checkout of this
repository.

## Install

From `sdk/typescript/` in a checkout (node 22.15 or later 22.x):

```sh
npm ci
node packages/agents/bin/ravel-agents-scaffold.js <dir>
```

The bin writes a starter project that links `@ravel/agents` with a relative
`file:` dependency. Run the bin by its path, never by its bare name; [the SDK
guide](../../../../docs/guides/agents-sdk.md#first-run) says why. From the
repository root, `ravel-cli agents scaffold <dir>` runs this bin with node
and passes `<dir>` through; the by-path form needs no Rust build.

## First example

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
`createClient({ url, token }).agent<typeof order>("order", "o-1")`. A worker
connects with `createHttpTransport`; see [HTTP transport](#http-transport).

## Entry points

| Import | Contents |
|---|---|
| `@ravel/agents` | The typed errors and shared types |
| `@ravel/agents/client` | `createClient`: send messages to agents and follow operations |
| `@ravel/agents/worker` | `defineAgent`, `message`, `serveAgents`, `createHttpTransport` and the step runner |
| `@ravel/agents/tools` | `defineTool` |
| `@ravel/agents/testing` | `createTestHarness`: in-process agents for tests |
| `@ravel/agents/contract` | `generateContract`: client types from an agent's handler schemas |
| `@ravel/agents/scaffold` | `scaffold`: write a starter project into an empty directory |

The package's bin, `bin/ravel-agents-scaffold.js <dir>`, runs `scaffold`.

The client entry imports no server-only module: nothing under `worker/`,
`tools/` or `testing/`, and none of `node:child_process`, `node:fs` or
`node:net`.

## Scaffold

The starter project holds one agent, `order`, whose `checkout` handler
prices a line through the durable step `price` against a local price-list
fixture (no network, no API key); the client types generated from its
handler schemas; a client typed by them; a vitest suite against
`@ravel/agents/testing`; and its `package.json`, `tsconfig.json` and README.

- The output typechecks and its suite passes with no `*_API_KEY` variable
  set. No generated file names an API key, and the generated test and the
  files it imports do not read `process.env`. The file list is exactly
  `SCAFFOLD_FILES`.
- A directory that is not empty, even one holding only an empty
  subdirectory, is refused with `ScaffoldError` code `target_not_empty`
  before anything is written, and a path that is a file with
  `target_not_a_directory`. Files are opened with `wx`, so none is
  overwritten.
- The bin exits 0 with the file list, 1 with `<code>: <message>` on stderr
  for a refused directory, and 64 on bad usage.
- The project pins zod, typescript, vitest and `@types/node` to the
  versions this workspace pins.
- The template's checked-in `contract.ts` is the generator's output.

## Contract generator

`generateContract(agent)` (`@ravel/agents/contract`) returns a TypeScript
module of client types for one agent: `AGENT_KIND`, `AGENT_VERSION`,
`HANDLER_NAMES`, `HandlerName`, and `InputOf<K>`, `OutputOf<K>` and
`OperationOf<K>` for each handler. Inputs are the payload schemas' input side
and outputs the result schemas' output side, as `PayloadOf` and `ResultOf`.
A generated contract typechecks against the agent's own `PayloadOf`,
`ResultOf` and `OperationHandle` types, and one that disagrees with the
agent fails that check.

How schemas map:

| Schema | Generated type |
|---|---|
| Optional member, defaulted enum, literal, nullable member | their TypeScript forms |
| `z.record` keyed by an enum or a literal | `Record<"x" \| "y", V>`; a payload missing a key, or holding a key outside the enum, fails to typecheck as zod fails to parse it |
| `z.partialRecord` keyed by an enum or a literal | `Partial<Record<"x" \| "y", V>>` |
| Loose object | its members `& Record<string, unknown>` |
| Empty union (`z.union([])`) | `never` |

Refused with `ContractError` code `unsupported_schema`, naming the handler
side:

- A `bigint`, a `Date`, a transform in a result schema, or a recursive
  schema.
- A record whose keys carry any other constraint (a pattern, a length),
  naming the keyword, since a `Record<string, V>` would let a client send
  keys zod rejects.
- An object with named properties and a catchall typed other than
  `unknown`: intersecting the members with `Record<string, V>` would type
  them as never, and an index signature wide enough for them would accept
  extra keys zod rejects.

## Worker and steps

- A handler's state is a draft. Neither its state changes nor its emitted
  events are visible until the turn commits, and a failed turn publishes
  neither. A failing handler dead-letters after its attempts.
- `ctx.step(key, tool, input)` journals each step before running it. After
  a crash, a completed step is replayed from the journal without calling
  the tool again, and the turn commits once.
- The runner decides from the journal's current record, not from the
  transcript the lease carried: a step the lease saw as started but whose
  result landed since then replays that result.
- A tool's result or failure that arrives after another attempt settled
  the step is kept as evidence only, and the step returns the journal's
  result.
- A tool that throws is journaled as the step's failure before the turn
  commits as failed, and a retried turn gets that failure back as
  `StepFailed` instead of running the tool again.
- An error a tool throws, typed or not, reaches the handler and does not
  block the operation; only the runner's own `NonDeterministicReplay` and
  `EffectInDoubt`, and a coordinator refusal, block it.
- A step key reused with a different tool name, tool version or input hash
  fails `NonDeterministicReplay` and blocks the operation; nothing commits.
- The input hash covers the input's JSON form (`toJSON` applied, an
  `undefined` member dropped and an `undefined` array item written as
  `null`, as `JSON.stringify` does), so a different `Date` is a different
  input and `{ cursor: undefined }` is the same input as `{}`. Any value
  that has no plain JSON form (a `Map`, `Set`, `bigint`, function,
  non-finite number or class instance, or `undefined` as the whole input)
  is refused with `InvalidStepInput` before the tool runs.
- One step at a time per turn: a second `ctx.step` while one is in flight
  rejects with `ConcurrentStepError`.
- `ctx.now()` and `ctx.random()` are the same on every attempt at one
  input: `ctx.now()` reads the lease's `recorded_unix_ns` and `ctx.random()`
  is seeded from its `random_seed`.
- A turn whose handler version or handler digest the worker does not match
  is abandoned with `IncompatibleHandler`.
- `AgentGone` from any coordinator call in a turn (step record, effect
  result or commit) ends the turn as abandoned with that error, as
  `OwnershipLost` does: the commit is attempted once and never retried, and
  the worker keeps polling.

Pauses after an abandoned turn. A turn still leased to the worker is handed
straight back to it, so the worker pauses before polling again:

| Abandoned with `retry` | Pause |
|---|---|
| `reconcile` (`EffectInDoubt`), `backoff` (`StoreUnavailable`, `MailboxFull`) or `resolve` (`OutcomeUnknown`) | doubling from `abandonDelayMs` (250 ms) up to `maxBackoffMs` (30 s) |
| `never`, such as `IncompatibleHandler` | the fixed `abandonDelayMs` |

The doubling delay is kept per operation: an operation's commit resets its
own delay, and another operation's abandonment starts at `abandonDelayMs`
whatever the first one's delay is. The pause still holds the whole poll
loop, not only the operation. The worker keeps a delay for at most 1024
operations and forgets the least recently used past that.

Poll errors. Only a `Forbidden` poll error, a refused token, stops the
worker: it goes to `onError`, or without one rejects the handle's `done`.
Any other poll error, `UnsupportedFormat` included, goes to `onError` and is
retried after a pause doubling from 50 ms, since a proxy can answer one poll
with a page the SDK does not read and the next with a turn.

## Tools

`defineTool` takes one of three recovery policies; any other value is a
`TypeError`. Every run receives the effect id, the attempt number and an
abort signal.

| Policy | After a crash mid-run |
|---|---|
| `at_least_once` | Runs again under the same effect id, attempt + 1 |
| `provider_idempotency` | Runs again under the same effect id; the provider dedupes on it |
| `manual` | Does not run again: a result recorded since replays, and an attempt with no result blocks the operation with `EffectInDoubt` |

## Client

- Every retry of one request reuses its request id and request hash, so the
  coordinator answers it as the same request. Retried: network failures,
  untyped 5xx answers, and typed errors whose hint is `backoff`. Retries are
  bounded: an exhausted `store_unavailable` surfaces as `StoreUnavailable`.
- A send whose outcome is unknown throws `OutcomeUnknown` carrying a
  handle. `handle.result()` first asks the coordinator to resolve the
  request id and resends the same body only when it never landed.
- A `result()` deadline throws `OutcomeUnknown` carrying the handle; an
  aborted wait does not cancel the operation.
- A failed or cancelled operation rejects `result()` and `call()` with
  `OperationFailed`.
- `handle.events()` yields only that operation's events, waits while it
  runs, ends once it is terminal and the first page past it has been read,
  and resumes from any event's `cursor`. A cursor names the last delivered
  event sequence, so resuming with a different page size repeats nothing.
- `agent.state()` answers `state: null` for an agent that has never
  committed a turn, and `AgentState<S>.state` is typed `S | null`.
- `agent.effects()` lists the agent's effects (`GET /api/v1/agents/effects`)
  and `client.effect(handle)` inspects one effect by its handle, with its
  attempts and the evidence of late results (`GET
  /api/v1/agents/effects/{effect}`).
- `client.reconcile(request)` posts an operator's decision on an in-doubt
  effect, `retry` or `record_result`, to `/api/v1/agents/reconcile`.
- `agent.destroy({ incarnation })` posts the agent's kind and key, and the
  incarnation only when one is given, to `/api/v1/agents/destroy`, and
  answers whether it destroyed the agent and what the destruction
  abandoned.
- `reconcile` and `destroy` need an `agents_admin` token and are sent once,
  never retried: a network failure or an untyped 5xx is `OutcomeUnknown`,
  and the caller reads the effect or the agent to learn whether it landed.

## HTTP transport

`createHttpTransport` (`@ravel/agents/worker`) is the `Transport` that
`serveAgents` uses against a server. The wire schemas match the server's
bodies: every golden fixture under `sdk/fixtures/`, written by the Rust
side, decodes and re-encodes to the same bytes, and the identity derivations
match the fixtures and the Rust kernel's `identity.rs`.

- Each worker route is posted with the worker's bearer token, and the poll
  wait is clamped to 20 s.
- `recordStep` posts the step to `worker/step` and returns the server's
  record, a started step with its effect and attempt or a recorded result
  to replay. The tool runs under the effect id and attempt that record
  names.
- A tool's result or error is posted to `worker/effects/complete` or
  `worker/effects/fail` with its effect and attempt. The transport takes no
  `journal` option: the server holds the step journal.
- A failed commit while an attempt has no result is refused by the
  server's 409 `effect_in_doubt`, which maps to `EffectInDoubt` with the
  body's ids.
- The server polls an agent that has never committed with a null state and
  `base_state_version` 0; the turn starts from the agent's `initialState`.

How answers map to errors:

| Answer | Error |
|---|---|
| An `ErrorBody` | the class its code names, with its safe ids |
| A typed 503 | `StoreUnavailable` |
| An untyped 5xx, a request with no answer, or a body that fails mid-read | `OutcomeUnknown` |
| An off-schema 2xx or an untyped 4xx | `UnsupportedFormat` |
| A 404 `agent_gone` from any worker route | `AgentGone`, `retry` `never` |
| A 401 or 403 with the server's plain body | `Forbidden`, with its status |

The worker renews the lease in the background while a turn runs, and
aborts the turn when a renewal is refused or the lease runs out. The
`leaseMs` option sets the lease the worker assumes; keep it at or below
30000. The [SDK guide](../../../../docs/guides/agents-sdk.md#lease-renewal)
gives the rules.

## Errors

The eleven wire errors subclass `RavelAgentsError`, plus `Forbidden` for a
refused credential. The [SDK
guide](../../../../docs/guides/agents-sdk.md#typed-errors) lists each wire
code with its class and `retry` hint.

Two more errors are local to the SDK: they are raised in the worker and
never cross the wire. Both subclass `RavelAgentsError` with `retry` `never`,
and an uncaught one fails the operation terminally, without retries.

| Class | Raised when |
|---|---|
| `ConcurrentStepError` | A handler starts a step while another is in flight |
| `InvalidStepInput` | A step input has no plain JSON form |

A handler that catches a failed step's error sees `StepFailed`, on the run
that recorded the failure and on every retry that replays it; it carries
the step key and the recorded error text.

## Testing harness

`createTestHarness({ agents, tools })` returns a fake coordinator
(`coordinator`), a client over it (`client`, and `agent(definition, key)`
for a typed one), one worker serving `agents`, the list of turns its
workers finished or abandoned (`turns`), and `advanceTime`, `crashWorker`
and `close`. The [SDK
guide](../../../../docs/guides/agents-sdk.md#the-testing-export) lists what
the fake honours and what it does not do. A test that passes against the
harness says the handler behaves under replay; it says nothing about what
Ravel persists.

## Known limits

- The wire `StepState` has only `started` and `completed`, so a failed step
  reads as `started` on the wire.
- A handler pin on the wire (`HandlerPinBody`) names the handler, version
  and digest but no agent kind. The server starts a new turn with the first
  pin whose handler name matches, and checks version and digest only when a
  turn resumes. The fake coordinator tells two agents at one version that
  share a handler name apart by the digest.
- The canonical encoding of step input is unspecified; the SDK hashes
  sorted-key JSON of the input's JSON form and refuses anything that is not
  plain JSON.
- u64 counters are numbers capped at `Number.MAX_SAFE_INTEGER` and refused
  past it. Nanosecond timestamps are strings refused outside the i64 range.

## Development

Run from `sdk/typescript/` after `npm ci`:

| Command | What it runs |
|---|---|
| `npm run lint` | ESLint with the `typescript-eslint` strict type-checked rules |
| `npm run typecheck` | `tsc` in strict mode over `src/`, `typetests/`, `e2e/` and `examples/` |
| `npm test` | the vitest suites under `src/`, and `e2e/*.unit.test.ts`, which need no server |
| `npm run test:e2e` | the end-to-end tests against a built server |

`make test-sdk` and `make lint-sdk` from the repository root run the unit
checks. CI's `sdk` job (`scripts/gates-sdk.sh`) runs `npm ci`, `npm run
lint`, `npm run typecheck` and `npm test`. No CI job runs `npm run
test:e2e`; run it locally.

`npm test` excludes the scaffold template's own suite under
`src/scaffold/template/`; the scaffold test runs it inside a generated
project. `typetests/` holds compile-time negative tests: each
`// @ts-expect-error` case must fail to compile, and if a type change lets
one compile, tsc reports the unused directive as error TS2578 and
`npm run typecheck` fails.

### End-to-end tests

`npm run test:e2e` starts a `ravel-server` with `--agents` on free local
ports and kills a worker, run as a child process, with SIGKILL inside a
step. The recover tests check that the resent request resolves to the same
operation and that an `at_least_once` step runs again under the same effect
id, and that a `manual` step blocks as `EffectInDoubt` until
`client.reconcile` records its result. The destroy test checks that a
destroy fences the worker holding the turn and that a recreate starts
incarnation 2. Build the server first:

```sh
cargo build --locked -p ravel-server --features agents
```

| Variable | Meaning |
|---|---|
| `RAVEL_SERVER_BIN` | the server binary; else `$CARGO_TARGET_DIR/debug/ravel-server`, else `target/debug/ravel-server` under the repository root |
| `RAVEL_E2E_STORE` | `memory` (default, `MemoryStore`) or `rustfs` (the compose RustFS store, which needs `docker compose`; the run stops the RustFS container afterwards only if it started it) |
| `RAVEL_E2E_REQUIRE` | `1` fails the run on a missing prerequisite instead of skipping |

Each recover test waits out one 30 s lease and its 10 s grace before the
second worker can take the turn over.

A missing prerequisite skips every test and names it, both in a warning at
the top of the run and in the suite title the verbose reporter prints:

```
e2e skipped: ravel-server binary not found at <path>; build it with `cargo build --locked -p ravel-server --features agents` or set RAVEL_SERVER_BIN
e2e skipped: RAVEL_E2E_STORE=rustfs needs `docker compose`, which is not installed or not running
```

A skipped run exits 0. With `RAVEL_E2E_REQUIRE=1` a missing prerequisite
fails the run instead, with the same reason, so a job that must exercise
the server cannot pass with nothing run.

### Demo

`demo/agents-kill-and-recover.sh`, from the repository root, starts the
compose RustFS store (or `MemoryStore` with
`RAVEL_AGENTS_DEMO_STORE=memory`) and a built `ravel-server --agents` with a
client, a worker and an `agents_admin` development token, and runs the same
two recover scenarios through `examples/assistant/kill-and-recover.ts`. It
asserts each printed `KEY=VALUE` figure against an exact value or a stated
band and exits 1 on any miss: among them `OP_SAME=true`,
`WORKER_A_SIGNAL=SIGKILL`, `DRAFT_ATTEMPTS=2`,
`TICKET_RUNS_BEFORE_RECONCILE=1` and `STATUS_IN_DOUBT=blocked`, with
`RECOVERY_MS` within [35000, 60000]. It needs `npm ci` in `sdk/typescript/`
and node 22.15 or later 22.x: the example's loader hook
(`examples/ts-hooks.js`) uses `module.registerHooks`, added in Node 22.15.0,
which is why the `engines` floor in `sdk/typescript/package.json` is
22.15.0. See `examples/assistant/README.md`.
