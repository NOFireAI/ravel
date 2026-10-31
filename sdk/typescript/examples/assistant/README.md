# Assistant example

A support assistant built on `@ravel/agents`. One `ask` message runs three
durable steps:

| Step | Tool | Recovery | Does |
|---|---|---|---|
| `search` | `kb.search` | `at_least_once` | Finds the knowledge-base articles whose keywords appear in the question |
| `draft` | `kb.draft` | `at_least_once` | Joins the found articles' text into the answer |
| `ticket` | `desk.ticket` | `manual` | Files a support ticket, only when the message sets `escalate` |

The tools run against `fixture.ts`: a fixed three-article knowledge base and
an in-memory ticket desk. Nothing there touches the network, reads a clock or
draws a random number, so the same question always gets the same answer and
every tool run is counted. Given a counters file, the fixture also appends
each run to it as one JSON line, so a run in a process that was killed is
still counted.

## Run it against a server

Start a server with `--agents` and a client and a worker token, then from
`sdk/typescript/` after `npm ci`:

```sh
RAVEL_URL=http://127.0.0.1:4318 \
RAVEL_AGENTS_TOKEN=<agents token> RAVEL_WORKER_TOKEN=<agents_worker token> \
npm run example:assistant
```

`RAVEL_URL` defaults to `http://127.0.0.1:4318`, and the question to "How
does a worker recover a step after a crash?" (pass another as arguments
after `--`). It prints the operation id, the articles found, the answer and
the tools that ran. A refused worker token stops the worker and rejects its
`WorkerHandle.done`; the example then prints the error's code and message on
one line to stderr and exits 1 (`kill-and-recover.ts` does the same). No test runs `main.ts`; the figures for the default
question are asserted through the kill-and-recover path below
(`RESULT_ARTICLES` `kb-2,kb-3`).

## Kill and recover

`scenario.ts` starts worker A as a child process (`main.ts --worker
<policy>`), waits until it is inside a step, after the tool's side effect
and before its result is recorded, and sends it SIGKILL. It then resends the
message under the same request id and starts worker B in its own process,
which takes the turn over once worker A's 30 s lease and 10 s grace run out.
Steps and effect results are recorded on the server through the worker
routes, so worker B replays what the server holds; nothing it sees comes from
worker A's memory.

- `at_least_once` kills inside `kb.draft`. `kb.search` was recorded and is
  replayed without running; `kb.draft` runs again under the same effect id
  as the server's attempt 2. Test: `worker killed inside an at_least_once
  step recovers the same operation on restart`
  (`packages/agents/e2e/recover.test.ts`).
- `manual` kills inside `desk.ticket`. Worker B does not file a second
  ticket: the server blocks the turn with `EffectInDoubt`, the operation
  reads `blocked` on that effect, and the effect's one attempt reads
  `in_doubt`. The scenario then reconciles it through the client with the
  admin token (`agents_admin`), recording the ticket the killed run filed,
  `T-1001`, and the turn completes. Test: `worker killed inside a manual
  step blocks as EffectInDoubt until the admin reconcile route is called
  through the client` (same file).

The operation id, the takeover, the replay, the attempts, the effect states
and the blocked status all come from the server; the tool run counts come
from the fixture's counters file, which both processes append to.

`npm run test:e2e` runs both against a server it starts. The demo
`demo/agents-kill-and-recover.sh` runs both through `kill-and-recover.ts`,
which prints one `KEY=value` line per figure, and asserts each:

| Figure | at_least_once | manual |
|---|---|---|
| `OPERATION_ID` | printed once, a non-empty base64url handle | the same, and not at_least_once's id |
| `RETRIED_OPERATION_ID` | equal to `OPERATION_ID` | equal to `OPERATION_ID` |
| `OP_SAME` | `true` | `true` |
| `WORKERS_DIFFER` | `true` | `true` |
| `WORKER_A_SIGNAL` | `SIGKILL` | `SIGKILL` |
| `SEARCH_RUNS` | `1` | `1` |
| `DRAFT_RUNS` | `2` | `1` |
| `TICKET_RUNS` | `0` | `1` |
| `DRAFT_EFFECTS_DISTINCT` | `1` | `1` |
| `DRAFT_CALL_ATTEMPTS` | `1,2` | `1` |
| `DRAFT_ATTEMPTS` | `2` | `1` |
| `TICKET_ATTEMPTS` | empty | `1` |
| `EFFECT_STATES` | `kb.search:completed,kb.draft:completed` | the same plus `desk.ticket:completed` |
| `RESULT_ERROR` | `none` | `none` |
| `RESULT_ARTICLES` | `kb-2,kb-3` | `kb-2,kb-3` |
| `RESULT_TICKET` | `null` | `T-1001` |
| `ANSWERED` | `1` | `1` |
| `IN_DOUBT_ERROR` | `none` | `EffectInDoubt` |
| `TICKET_RUNS_BEFORE_RECONCILE` | `none` | `1` |
| `STATUS_IN_DOUBT` | `none` | `blocked` |
| `BLOCKED_EFFECT_IS_TICKET` | `false` | `true` |
| `RECOVERY_MS` | within [35000, 60000] | within [35000, 60000] |

`DRAFT_CALL_ATTEMPTS` lists the attempt number each `kb.draft` run was
given; `DRAFT_ATTEMPTS` is the attempt the server's `kb.draft` effect
finished on. `RESULT_ANSWER` must equal the drafted answer in both. Run it
from the repository root after building the server with `cargo build
--locked -p ravel-server --features agents`; `RAVEL_AGENTS_DEMO_STORE=memory`
runs it without docker. With the RustFS store it stops the container on exit
only if it started it. It needs node 22.15 or later 22.x
(`module.registerHooks` in `examples/ts-hooks.js`).
