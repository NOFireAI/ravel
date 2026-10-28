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
every tool run is counted.

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
the tools that ran. No test runs `main.ts`; the figures for the default
question are asserted through the kill-and-recover path below
(`RESULT_ARTICLES` `kb-2,kb-3`).

## Kill and recover

`scenario.ts` kills worker A inside a step, after the tool's side effect and
before its result is recorded, resends the message under the same request
id, and starts worker B, which takes the turn over once worker A's 30 s
lease and 10 s grace run out. The kill is an abort inside the process, not a
process kill: until the server mounts its step and effect routes, the step
journal is the SDK's in-memory one, so both workers must share a process
(see "Wire gaps" in `packages/agents/README.md`).

- `at_least_once` kills inside `kb.draft`. `kb.search` was journaled and is
  replayed without running; `kb.draft` runs again under the same effect id
  as attempt 2. Test: `worker killed inside an at_least_once step recovers
  the same operation on restart, replaying steps from the in-memory
  journal` (`packages/agents/e2e/recover.test.ts`).
- `manual` kills inside `desk.ticket`. Worker B does not file a second
  ticket: the turn is abandoned with `EffectInDoubt` until the journal's
  effect is reconciled, through the journal, with the ticket the killed run
  filed, `T-1001`. Test: `worker killed inside a manual step blocks as
  EffectInDoubt in the in-memory journal until reconciled there` (same
  file).

The same operation id on the resend, the takeover by worker B and the single
commit come from the server; the replay, the attempts and the in-doubt block
come from the in-memory journal.

`npm run test:e2e` runs both against a server it starts. The demo
`demo/agents-kill-and-recover.sh` runs both through `kill-and-recover.ts`,
which prints one `KEY=value` line per figure, and asserts each:

| Figure | at_least_once | manual |
|---|---|---|
| `OPERATION_ID` | printed once, a non-empty base64url handle | the same, and not at_least_once's id |
| `RETRIED_OPERATION_ID` | equal to `OPERATION_ID` | equal to `OPERATION_ID` |
| `RETRIED_SAME_OPERATION` | `true` | `true` |
| `WORKERS_DIFFER` | `true` | `true` |
| `SEARCH_RUNS` | `1` | `1` |
| `DRAFT_RUNS` | `2` | `1` |
| `TICKET_RUNS` | `0` | `1` |
| `DRAFT_EFFECTS_DISTINCT` | `1` | not asserted |
| `DRAFT_ATTEMPTS` | `1,2` | not asserted |
| `TICKET_ATTEMPTS` | not asserted | `1` |
| `EFFECT_STATES` | `kb.search:completed,kb.draft:completed` | the same plus `desk.ticket:completed` |
| `RESULT_ERROR` | `none` | `none` |
| `RESULT_ARTICLES` | `kb-2,kb-3` | `kb-2,kb-3` |
| `RESULT_TICKET` | `null` | `T-1001` |
| `ANSWERED` | `1` | `1` |
| `IN_DOUBT_ERROR` | `none` | `EffectInDoubt` |
| `IN_DOUBT_TICKET_RUNS` | not asserted | `1` |
| `IN_DOUBT_SERVER_STATUS` | not asserted | `running` |
| `RECOVERY_MS` | within [35000, 60000] | within [35000, 60000] |

`RESULT_ANSWER` must equal the drafted answer in both. Run it from the
repository root after building the server with `cargo build --locked -p
ravel-server --features agents`; `RAVEL_AGENTS_DEMO_STORE=memory` runs it
without docker. With the RustFS store it stops the container on exit only
if it started it. It needs node 22.15 or later 22.x (`module.registerHooks`
in `examples/ts-hooks.js`).
