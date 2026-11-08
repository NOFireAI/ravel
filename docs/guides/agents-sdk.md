# The Ravel Agents TypeScript SDK

The SDK in `sdk/typescript/packages/agents` (package `@ravel/agents`)
defines agents and tools, serves them from a worker, and sends them messages
from a client. The package is not published to npm: a project links it from
a checkout of this repository. This page takes a first project from an empty
directory to a passing test suite, then to a running server. The package
[README](../../sdk/typescript/packages/agents/README.md) is the API
reference.

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

   With `ravel-cli` built, `ravel-cli agents scaffold DIR` runs the same
   bin with node: from the repository root it finds the bin at its path in
   the checkout (`--scaffold-bin` names another, `--node` another node),
   passes DIR through as given, refuses a DIR that exists and is not empty
   before starting node, and reports the bin's exit code and stderr when it
   fails. `agents dev` prints this command once its server is ready.

   The project holds one agent, `order`, whose `checkout` handler prices a
   line through a durable step against a local price-list fixture, the
   client types generated from its handler schemas, a client typed by them,
   a vitest suite and its `package.json`, `tsconfig.json` and README. It
   links `@ravel/agents` from your checkout through a relative `file:`
   dependency. A directory that is not empty is refused and nothing is
   written.

2. Run its tests:

   ```sh
   cd ../../../my-agent
   npm install
   npm test
   ```

   They run the agent against the SDK's in-process fake coordinator, so they
   need no server, no network and no API key. They cover a checkout, an
   unknown SKU, a worker killed inside the price step, and the generated
   client types matching the agent.

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
   total.

After changing a payload or result schema, run `npm run contract` to
regenerate the client types. The project's own test fails until you do. The
generator is `generateContract` in `@ravel/agents/contract`.

The SDK's automated checks do not run `npm start`, and they link the SDK's
own `node_modules` into a scaffolded project instead of running
`npm install`, so npm resolving the `file:` dependency is not exercised. The
SDK's end-to-end tests drive the same client and worker calls as `npm start`
against a built server.

## Typed errors

Every error the SDK raises from the wire subclasses `RavelAgentsError` and
carries a `code` equal to its class name, the safe ids of what it concerns
in `ids` (agent, incarnation, request, operation, turn, step, effect, timer,
epoch; never a payload), and a `retry` hint. The client and the worker
transport map each wire code to its class.

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
or 403, which the SDK maps to `Forbidden` with `retry` `never`, and a worker
whose poll is refused stops. An unknown code, or a 2xx body that does not
match its schema, reads as `UnsupportedFormat`. `ConcurrentStepError` and
`InvalidStepInput` are raised in the worker and never cross the wire.

## The testing export

`createTestHarness` in `@ravel/agents/testing` runs a fake coordinator, a
client over it and one worker, all in process memory. The fake models the
coordinator's protocol, not its storage.

What the fake honours:

- Leases carry the turn's recorded time and seed, a heartbeat extends a
  held lease to 30 s past the fake's clock, and a heartbeat on a revoked
  lease fails `OwnershipLost`.
- After `crashWorker()`, completed steps replay from the fake's journal
  without calling the tool, and the turn commits once.
- An `at_least_once` tool interrupted by a crash runs again under the same
  effect id.
- A `manual` tool interrupted by a crash blocks the operation with
  `EffectInDoubt` and names the effect in `blockedEffect`. The fake refuses
  a failed commit while a step's attempt is authorized without a result, as
  the server does.
- `cancel` on a running operation applies when its turn commits, the
  cancelled operation rejects `result()` with `OperationFailed`, and a
  second `cancel` answers `cancelled`.
- A failing handler publishes neither state nor events and dead-letters
  after its attempts.
- An agent's state reads `null` until its first committed turn, as on the
  server.
- `advanceTime(ms)` moves the fake's clock, and the next lease records the
  moved time.

What it does not do:

- It has no timers, and its clock never expires a lease.
- It has no reconcile route, so an operation in doubt stays blocked.
- It keeps nothing past `close()`.

To drive a kill and a recovery, hold a tool's run open on a promise the
test controls, wait until it has started, call `crashWorker()`, then
release it. `crashWorker()` revokes the current leases and starts a fresh
worker (`harness-worker-2`) against the same fake: it simulates a worker
crash, not a coordinator or store crash. The killed worker's turn is
reported in `turns` as abandoned, and the fresh worker takes the turn over.

A test that passes against the harness shows how the handler behaves under
replay. It shows nothing about what Ravel stores. Step 3 of
[First run](#first-run) runs the project against a server.

## Effects, reconcile and destroy

The client reads and settles effects and destroys agents:

- `agent.effects()` lists an agent's effects.
- `client.effect(handle)` inspects one effect by its handle, with its
  attempts and the evidence of late results.
- `client.reconcile(request)` records an operator's decision on an in-doubt
  effect.
- `agent.destroy({ incarnation })` destroys an agent, pinned to an
  incarnation when one is given.

`reconcile` and `destroy` need a token holding `agents_admin`; a token
without it is refused with `Forbidden`. Both are sent once and never
retried, so a lost answer is `OutcomeUnknown`: read `client.effect(handle)`
or the agent to learn whether it landed.

After a destroy, the worker holding the turn abandons it with `AgentGone`
and sends no commit. A destroyed agent's worker routes answer `AgentGone`,
and the worker abandons the turn without retry. A second destroy answers
`destroyed: false` with the same abandoned counts, and `create` starts
incarnation 2 under the same agent id. The
[operator guide](operations/agents.md#destroying-an-agent) covers when to
destroy an agent.

## HTTP transport limits

`createHttpTransport` in `@ravel/agents/worker` connects a worker to a
server.

- The poll wait is clamped to 20 s.
- Steps and effect results are recorded on the server. The transport has no
  `journal` option.
- An untyped 5xx, a request with no answer, or a body that fails mid-read
  is `OutcomeUnknown`.

### Lease renewal

The worker renews the lease in the background while a turn runs: a
`worker/heartbeat` when the turn is assigned, then one each third of the
lease left by the last answered expiry, never sooner than 1 s apart, one at
a time, until the commit is sent or the turn is abandoned. No heartbeat goes
before a step. The interval is read against the worker's clock, so a clock
ahead of the server's renews at the 1 s floor.

- A renewal refused with `OwnershipLost` or `AgentGone` aborts the
  handler's signal and abandons the turn without a commit.
- Any other renewal failure is retried at the next tick. One inside the
  lease does not abort the turn.
- Once renewals have failed past the lease, the worker aborts the handler's
  signal at the expiry and abandons the turn without a commit, with
  `LeaseExpired`.

After either abort:

- A step the handler calls rejects with the abort's reason and sends no
  step record, so a handler that never reads `ctx.signal` still cannot
  start an effect on a turn another worker may own.
- A step whose record was sent inside the lease and answered after the abort
  runs no tool and reports no effect. An answer that authorizes an attempt
  rejects with the abort's reason; one that comes back completed or failed
  returns the replayed output or rejects with `StepFailed`.
- A tool already running that returns still has its result reported, so a
  takeover replays it rather than running the effect again. When the server
  answers that result as late, the worker sends nothing more and the step
  rejects with the abort's reason.
- A tool that throws reports no failure: no `worker/effects/fail` is sent.

The worker measures the expiry on its own timers, `leaseMs` (a
`serveAgents` option, default 30000) after it received the assignment or
sent the last renewal that was answered, because no answer carries the lease
length and the two wall clocks may disagree. The server counts a renewal's
lease from when it processed the request, no earlier than the sending, and
the assignment's lease from when it answered the poll, so the worker's first
expiry, counted from when that answer arrived, can trail the server's by the
poll answer's travel time.

The server's agents lease is fixed at 30000 with a grace of 10000
(`DEFAULT_AGENTS_LEASE_MS` and `DEFAULT_AGENTS_HEARTBEAT_GRACE_MS`, which no
flag sets; the agents runtime's `RuntimeConfig::default` uses the same two
values). Keep `leaseMs` at or below 30000, the server's fixed lease: the
abort then lands before the server's grace lets another worker take the turn
over, as long as the poll answer travels for less than the grace. A longer
`leaseMs` lets a turn run past its lease.

`leaseMs` outside 1000 to 2147483647, or not an integer, rejects
`serveAgents` with `InvalidWorkerOption` before any request.

Renewals live in the memory of the coordinator that answered them, so with
two coordinators a worker polling the other one can still take a turn over
once the lease and grace run out after its takeover.
