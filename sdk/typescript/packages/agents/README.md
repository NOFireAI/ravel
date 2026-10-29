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
  `serveAgents` uses against a server. The SDK does not call the server's
  step or effect routes yet, so steps go to an in-memory step journal (see
  "Wire gaps").
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
| `@ravel/agents/contract` | `generateContract`: client types from an agent's handler schemas |
| `@ravel/agents/scaffold` | `scaffold`: write a starter project into an empty directory |

The package's bin, `bin/ravel-agents-scaffold.js <dir>`, runs `scaffold`.
Run it by its path, never by its bare name; [the SDK
guide](../../../../docs/guides/agents-sdk.md#first-run) says why.

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

## Scaffold and typed contract

From `sdk/typescript/`, after `npm ci`:

```sh
node packages/agents/bin/ravel-agents-scaffold.js <dir>
```

Run the bin by its path; [the SDK
guide](../../../../docs/guides/agents-sdk.md#first-run) says why.

It writes a starter project: one agent, `order`, whose `checkout` handler prices a line through the durable step `price`
against a local price-list fixture (no network, no API key); the client
types generated from its handler schemas; a client typed by them; a vitest
suite against `@ravel/agents/testing`; and its `package.json`, `tsconfig.json`
and README. Its suite needs no server and no API key. From the repository
root, `ravel-cli agents scaffold <dir>` runs this bin with node and passes
`<dir>` through; the by-path form above needs no Rust build.

- The output typechecks and its own suite passes, run as a child process
  with every `*_API_KEY` variable removed and again with poison values set,
  and no generated file names an API key; the generated test and the files
  it imports do not read `process.env`. The file list is exactly
  `SCAFFOLD_FILES`. Test: `scaffold output typechecks and its tests pass
  without an API key` (`src/scaffold/index.test.ts`). That test links the
  SDK's own `node_modules` into the output instead of running
  `npm install`, so npm resolving the generated `file:` dependency is not
  under test.
- A directory that is not empty, even one holding only an empty
  subdirectory, is refused with `ScaffoldError` code `target_not_empty`
  before anything is written, and a path that is a file with
  `target_not_a_directory`; files are opened with `wx`, so none is
  overwritten. Tests: `a second scaffold into the same directory is refused
  and changes nothing`, `a directory holding any other file is refused and
  nothing is written`, `a directory holding only an empty subdirectory is
  refused`, `a path that is a file is refused`.
- The bin exits 0 with the file list, 1 with `<code>: <message>` on stderr
  for a refused directory, and 64 on bad usage. Test: `the bin writes a
  project, exits 1 on a non-empty directory and 64 on bad usage`.
- The project links `@ravel/agents` with a relative `file:` dependency and
  pins zod, typescript, vitest and `@types/node` to the versions this
  workspace pins. Test: `the generated package.json links this package and
  pins the workspace's versions`.

`generateContract(agent)` (`@ravel/agents/contract`) returns a TypeScript
module of client types for one agent: `AGENT_KIND`, `AGENT_VERSION`,
`HANDLER_NAMES`, `HandlerName`, and `InputOf<K>`, `OutputOf<K>` and
`OperationOf<K>` for each handler. Inputs are the payload schemas' input side
and outputs the result schemas' output side, as `PayloadOf` and `ResultOf`.

- The contract generated from `examples/assistant` typechecks against the
  agent's own `PayloadOf`, `ResultOf` and `OperationHandle` types, and a
  contract that disagrees with the agent fails that check. Tests: `the
  contract generated from the example assistant typechecks against the
  agent's own types`, `a contract that disagrees with the agent fails that
  typecheck` (`src/scaffold/contract.test.ts`).
- The emitted text is pinned for a schema with an optional member, a
  defaulted enum, a literal and a nullable member. Test: `emits handler
  names, input and output types and the operation handle type`.
- A `bigint`, a `Date`, a transform in a result schema or a recursive
  schema is refused with `ContractError` code `unsupported_schema` naming
  the handler side. Test: `a schema with no TypeScript form is refused with
  a ContractError naming the handler side`.
- A record keyed by an enum or a literal is typed by its key union:
  `Record<"x" | "y", V>` for `z.record`, `Partial<Record<"x" | "y", V>>`
  for `z.partialRecord`. A payload missing a key, or holding a key outside
  the enum, fails to typecheck as zod fails to parse it. Test: `an
  enum-keyed record is typed by its key union and typechecks against the
  agent's own types`.
- A record whose keys carry any other constraint (a pattern, a length) is
  refused with `unsupported_schema` naming the keyword, since a
  `Record<string, V>` would let a client send keys zod rejects. Test: `a
  record whose keys are constrained other than by an enum is refused naming
  the keyword`.
- An object with named properties and a catchall typed other than
  `unknown` is refused with `unsupported_schema`: intersecting the members
  with `Record<string, V>` would type them as never, and an index signature
  wide enough for them would accept extra keys zod rejects. A loose object
  is typed as its members `& Record<string, unknown>`. Test: `an object
  with named properties and a typed catchall is refused; a loose object
  keeps its members`. An empty union (`z.union([])`) is typed `never`.
  Test: `an empty union is typed never`.
- The template's checked-in `contract.ts` is the generator's output. Test:
  `the template's contract.ts is the generator's output for the template
  agent`.

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
- A turn still leased to the worker is handed straight back to it, so
  after a turn abandoned with an error whose `retry` is `reconcile`
  (`EffectInDoubt`), `backoff` (`StoreUnavailable`, `MailboxFull`) or
  `resolve` (`OutcomeUnknown`) the worker pauses before polling again,
  doubling from `abandonDelayMs` (250 ms) up to `maxBackoffMs` (30 s). An
  abandonment whose `retry` is `never`, such as `IncompatibleHandler`,
  pauses the fixed `abandonDelayMs`. The doubling delay is kept per
  operation: an operation's commit resets its own delay, and another
  operation's abandonment starts at `abandonDelayMs` whatever the first
  one's delay is. The pause still holds the whole poll loop, not only the
  operation. The worker keeps a delay for at most 1024 operations and
  forgets the least recently used past that. Tests: `doubles one
  operation's pause after each EffectInDoubt abandonment up to the cap and
  resets it on its commit`, `doubles the pause after each StoreUnavailable
  heartbeat abandonment, and an IncompatibleHandler abandonment keeps the
  fixed pause`, `an operation at the backoff cap does not raise an
  unrelated operation's pause, and the unrelated commit does not reset
  it`, `OperationBackoffs keeps one delay per operation and forgets the
  least recently used past its capacity`, `a Backoff doubles from its
  start to its cap and starts over on reset` (`serve.test.ts`).
- Only a `Forbidden` poll error, a refused token, stops the worker: it goes
  to `onError`, or without one rejects the handle's `done`. Any other poll
  error, `UnsupportedFormat` included, goes to `onError` and is retried
  after a pause doubling from 50 ms, since a proxy can answer one poll with
  a page the SDK does not read and the next with a turn. Tests: `a
  Forbidden poll stops the worker and rejects done with it`, `a Forbidden
  poll goes to onError, stops the worker and fulfils done`, `a poll
  answered with UnsupportedFormat is retried and the next poll runs its
  turn`, `a transient OutcomeUnknown poll is retried after a doubling pause
  and the next poll runs its turn` (`serve.test.ts`).

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
- A response whose body fails mid-read is `OutcomeUnknown`, like a request
  with no answer. Test: `maps a response body that fails mid-read to
  OutcomeUnknown`.
- The transport holds a polled turn's step scope until the runner releases
  the turn, committed or abandoned, so a long-running worker keeps none for
  turns it gave up. Test: `an abandoned turn leaves no scope behind`.
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

`Forbidden` has no wire code: the server refuses a credential with HTTP 401
or 403 and a plain body, which the client and the worker transport map to
`Forbidden` with `retry` `never`. A worker whose poll is refused stops
instead of retrying. Every other poll failure is retried, including an
`UnsupportedFormat` answer, although its `retry` is also `never`. Tests:
`a Forbidden poll stops the worker and rejects done with it`, `a poll
answered with UnsupportedFormat is retried and the next poll runs its
turn`, `a transient OutcomeUnknown poll is retried after a doubling pause
and the next poll runs its turn` (`serve.test.ts`).

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

## The testing export (`@ravel/agents/testing`)

`createTestHarness({ agents, tools })` returns a fake coordinator
(`coordinator`), a client over it (`client`, and `agent(definition, key)`
for a typed one), one worker serving `agents`, the list of turns its
workers finished or abandoned (`turns`), and `advanceTime`, `crashWorker`
and `close`. The fake models the coordinator's protocol, not its storage.
Each statement below names the test that pins it; unless a file is named,
the test is in `src/testing/harness.test.ts`.

What the fake honours:

- Leases: a polled lease carries the turn's recorded time and random seed,
  a heartbeat extends a held lease to 30 s past the fake's clock, and a
  heartbeat on a revoked lease fails `OwnershipLost`. Test: `leases carry
  the recorded time and seed, and heartbeats extend a held lease only`
  (`testing/transport.test.ts`).
- Replay: after `crashWorker()`, the new worker replays completed steps from
  the fake's journal without calling the tool again, and the turn commits
  once. Test: `replay after crashWorker with two completed steps replays
  both without calling run and commits once` (`worker/replay.test.ts`).
- At-least-once retry: an `at_least_once` tool interrupted by a crash runs
  again under the same effect id. Test: `an at_least_once tool interrupted
  by a crash runs again under the same effect id`
  (`worker/steps.test.ts`).
- In-doubt: a `manual` tool interrupted by a crash blocks the operation with
  `EffectInDoubt` and names the effect in `blockedEffect`. Test: `a manual
  tool interrupted by a crash blocks the operation with EffectInDoubt`
  (`worker/steps.test.ts`). The fake refuses a failed commit while a step's
  attempt is authorized without a result, as the kernel does. Test: `P11
  the fake refuses FailTurn while a step's attempt is authorized without a
  result` (`worker/review.test.ts`).
- Cancellation: `cancel` on a running operation is recorded and applied
  when its turn commits, a cancelled operation rejects `result()` with
  `OperationFailed`, and a second `cancel` answers `cancelled`. Test:
  `cancel is idempotent and a cancelled operation rejects result()`
  (`client/client.test.ts`).
- Retries and dead letters: a failing handler publishes neither state nor
  events and dead-letters after its attempts. Test: `a failing handler
  publishes neither state nor events, and dead-letters after its attempts`
  (`worker/steps.test.ts`).
- Time: `advanceTime(ms)` moves the fake's clock, and the next lease
  records the moved time. Test: `leases carry the recorded time and seed,
  and heartbeats extend a held lease only` (`testing/transport.test.ts`).

What it does not do:

- Timers: the fake has none, and its clock never expires a lease. Advancing
  the clock an hour past a lease's expiry neither takes the turn away nor
  runs anything. Test: `advancing the fake's clock past a lease's expiry
  neither takes the turn away nor runs anything`.
- Reconciliation: the fake has no reconcile route, so an operation blocked
  with `EffectInDoubt` stays blocked through a day of clock advance and a
  further worker. Test: `an operation blocked with EffectInDoubt stays
  blocked: no clock advance or new worker reconciles it`.
- Durability: every agent, operation, step and event lives in the fake's
  memory, and a new harness has never seen the agents of one that closed.
  Test: `nothing outlives close: a new harness has never seen the first
  one's agent`.

Kill and recover: hold a tool's run open (a promise the test resolves),
wait until it has started, call `crashWorker()`, then release it.
`crashWorker()` revokes the current leases and starts a fresh worker
(`harness-worker-2`) against the same fake; it simulates a worker crash,
not a coordinator or store crash. The killed worker's turn is reported in
`turns` as abandoned. Tests: `replay after crashWorker with two completed
steps replays both without calling run and commits once`
(`worker/replay.test.ts`), `an operation blocked with EffectInDoubt stays
blocked: no clock advance or new worker reconciles it` (the abandoned
turn), and in a scaffolded project `a worker killed
inside the price step is replaced and the turn commits once`, which the
scaffold acceptance test runs. A test that passes against the harness says
the handler behaves under replay; it says nothing about what Ravel persists.

## Wire gaps

The wire contract this package was written against has gaps, filled
provisionally on the SDK side:

- Unused routes: the server serves `worker/step`,
  `worker/effects/complete`, `worker/effects/fail`, `effects` and the
  `agents_admin` reconcile route, but `createHttpTransport` does not call
  them yet. Until it does, `createHttpTransport` serves `recordStep`, `completeEffect` and
  `failEffect` from a `MemoryStepJournal`, which lives in one process and
  is lost with it. This is why the end-to-end kill is an in-process abort.
  Tests: `authorizes a new attempt at an unsettled at_least_once step and
  replays a settled one`, `puts an unsettled manual step in doubt until a
  reconcile records its result`, `hands a reconciled retry out exactly
  once` (`http.test.ts`). Pairing each step with its effect record on the
  server, so the SDK can drop the in-memory journal, is tracked in #3416.
- While an SDK step is in doubt the server reports the operation as
  `running`, because the step lives only in the SDK's journal and the server
  holds no effect record for it (asserted in
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
  which need no server. The scaffold template's own suite under
  `src/scaffold/template/` is excluded here; the scaffold acceptance test
  runs it inside a generated project.
- `npm run test:e2e`: the end-to-end tests above.

`typetests/` holds compile-time negative tests. Each `// @ts-expect-error`
case (an invalid handler signature, an invalid payload type, a step input
that does not match its tool, and others) must fail to compile. If a type
change lets one compile, tsc reports the unused directive as error TS2578,
and `npm run typecheck` fails.
