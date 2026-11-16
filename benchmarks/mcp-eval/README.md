# MCP agent evaluation

This directory holds the corpus and the runner for the agent evaluation in
ADR-1374 D8 (issue #1385, epic #1374). The ADR's figures are hypotheses:
half the invalid queries, one third fewer store requests per completed task,
and equal or better correctness. **This directory records no results.** A
run writes its results to a directory the operator names outside the
repository, and `run.sh` refuses an `--out` inside it.

## What is measured

One agent answers the same 24 questions twice, once with each tool profile:

| Profile | Tools the agent may call |
|---|---|
| `baseline` | `ravel_capabilities`, `ravel_query_sql`, `ravel_query_promql` |
| `full` | all nine tools: the baseline three plus `ravel_describe_data`, `ravel_find_labels`, `ravel_explain_query`, `ravel_search_logs`, `ravel_get_trace`, `ravel_analyze_timeseries` |

Each case belongs to one of six journeys:

| Journey | Question shape | Cases |
|---|---|---|
| A | Unknown environment: which signals hold data, the schema per table, the indexed keys, the metric families with unit and type, the budget ceilings. Three calls or fewer. | A1-A4 |
| B | Incident: compare against a baseline window, find the slow or failed traces, read the logs on those trace ids. The answer names no cause. Eight calls or fewer. | B1-B4 |
| C | Security: a timeline over logs and spans on an indexed or declared key. The answer names the keys searched, the keys scanned without an index, and the sources not covered. | C1-C4 |
| D | Self-repair: a request that fails with a typed failure. The answer quotes the exact validation text and the estimate against the ceiling, and takes one next step. At most one retry per failure class. | D1-D4 |
| E | Evidence: `evidence_ref` is not served yet, so these cases score what an agent can cite today: the query text, the time range, the snapshot id, the cursor. Every E case carries the tag `evidence_ref_not_served`. | E1-E3 |
| F | Diagnosis: an empty result classified as zero matches, a window newer than the watermark, refused by budget, storage unavailable, or outside retention. | F1-F5 |

The no-data cases are C4, F1 and F5. The not-visible cases are E3 (a late
batch the agent's earlier result could not have seen) and F2 (a window newer
than the ingest watermark). `run.sh --check` prints both lists.

For every case and every run, `lib/evaltool.py score` writes one
`record.json` holding these figures:

| Figure | Unit | What it counts |
|---|---|---|
| `correct` | bool | every `answer_facts` pattern in `expected/<id>.json` matches the agent's answer |
| `facts_matched`, `facts_total` | count | answer facts matched, and the total |
| `evidence_completeness` | ratio 0-1 | `evidence` patterns matched in the answer, over the total |
| `completed` | bool | the agent exited 0, its transcript says `completed: true`, and the answer is not empty |
| `tool_calls` | count | calls in the transcript |
| `within_call_bound` | bool | `tool_calls` is at most the case's `call_bound` |
| `profile_violations` | count | calls to a tool outside the profile |
| `invalid_queries` | count | calls that failed with class `validation`, `invalid_argument`, `missing_argument` or `unsupported` |
| `retries_per_failure_class`, `max_retries_per_failure_class` | count | per failure class, the calls to the same tool after its first failure, up to and including the first that succeeds |
| `unrepaired_failure_classes` | list | failure classes with no later successful call to the same tool |
| `budget_violations` | count | calls to a tool that takes a budget the case sets, sent without it or above it |
| `wall_time_ms` | ms | wall time of the agent command, measured by `run.sh` |
| `store_requests` | count | sum of `budget.actual.store_requests` over the calls whose envelope carries `budget.actual`; counted by the server |
| `store_wire_bytes` | bytes | sum of `budget.actual.bytes_scanned` over the same calls: object-store wire bytes as the server counts them |
| `calls_without_actual` | count | calls whose envelope has no `budget.actual` (metadata tools). Their store cost is absent from the two sums above, not zero. |
| `response_bytes` | bytes | sum of the agent-reported byte length of each MCP HTTP response body |

`store_source` and `response_bytes_source` in each record repeat which bytes
the figure counts, so a record read on its own says so. `summary.tsv` has one
row per record and a `# units:` first line.

## The fixture

`fixture/fixture.json` is the one place that states the fixture: the seed,
the time base, the named entities, the server flags, the load sequence and
the exact counts. `fixture/gen.py` (Python standard library only) encodes the
OTLP protobuf bodies from the seed, and `gen.py counts` derives the counts
from the same generator. `run.sh --check` fails unless the two agree, figure
by figure.

- Time base: T0 is the load time floored to the minute, minus 60 minutes.
  Every fixture event lies in `[T0, T0+60m)`, inside the server's 2 h
  ingest-lag window. `--fixture` records T0 in `<out>/anchor.json` and
  `--run` reads it from there. Case windows are written as offsets from T0
  or from the time the case starts (`T0+20m`, `now+10m`).
- Entities: the services, an absent service (`billing`), three metric
  families with unit and type, an incident window with its baseline, the
  slow and failing traces, a security subject on an indexed and declared
  key, an unindexed session key, and the late batch used by E3.
- Server flags: `fixture.json` lists the flags the server must run with
  (`--mcp`, the allowed origin, the indexed fields, the declared `user.id`
  column, and `--retention-default 7d`). The counts assume them.

## Running it

Offline, with no server:

```sh
bash benchmarks/mcp-eval/run.sh --check
bash benchmarks/mcp-eval/run.test.sh
```

`--check` validates every case and expected file, prints each figure as
`figure KEY=N`, and exits 1 unless each figure in `bands.json` and in the
fixture counts is present exactly once and equal to its band. A figure that
is missing or printed twice fails with its own message.

Against a server started with the flags in `fixture.json` and an empty
tenant:

```sh
export MCP_EVAL_TOKEN=...            # bearer token for the tenant
bash benchmarks/mcp-eval/run.sh --fixture http://127.0.0.1:8080 --out /srv/mcp-eval/2026-11-03
```

`--fixture` refuses a tenant that already holds rows, loads the three
signals over OTLP/HTTP, and queries every count that has a SQL form back
over `/api/v1/sql` with `min_commit_token` set to the ack tokens. It exits 1
unless each count is present once and exact.

Then one run per profile and condition, after the fixture hour has ended:

```sh
export MCP_EVAL_AGENT_CMD=/path/to/agent
export MCP_EVAL_RAVEL_SHA=<main SHA the server was built from>
bash benchmarks/mcp-eval/run.sh --run baseline http://127.0.0.1:8080 --out /srv/mcp-eval/2026-11-03 --condition cold
bash benchmarks/mcp-eval/run.sh --run baseline http://127.0.0.1:8080 --out /srv/mcp-eval/2026-11-03 --condition warm
bash benchmarks/mcp-eval/run.sh --run full     http://127.0.0.1:8080 --out /srv/mcp-eval/2026-11-03 --condition cold
bash benchmarks/mcp-eval/run.sh --run full     http://127.0.0.1:8080 --out /srv/mcp-eval/2026-11-03 --condition warm
```

`--runs` defaults to 5. A cold run needs `MCP_EVAL_COLD_CMD`, an executable
run before every case that restarts the server with empty caches. A warm run
first makes one unrecorded pass under `<out>/prime/`. Case F4 needs
`MCP_EVAL_FAULT_CMD`, an executable taking `on` or `off` that makes object
storage unavailable and restores it; without it F4 is recorded `not_run`.
`MCP_EVAL_ORIGIN` sets the Origin header `run.sh` sends (default
`http://mcp-eval.invalid`, matching the flag in `fixture.json`).

Results land in `<out>/results/<profile>/<condition>/run-<n>/<case>/`, with
`summary.tsv` beside the runs. `run.sh` refuses a results directory that
already exists.

Exit codes: 0 pass, 1 a finding or a failed step, 64 usage or a refused
precondition, 69 a tool or the server could not be reached.

### Agent command contract

`run.sh` runs the agent once per case as

```
"$MCP_EVAL_AGENT_CMD" <case.json> <tools.txt> <transcript.json>
```

with `MCP_EVAL_URL`, `MCP_EVAL_PROFILE` and `MCP_EVAL_TOKEN` in its
environment. `--run` refuses with exit 64 when `MCP_EVAL_AGENT_CMD` is unset,
before it checks anything else.

- `case.json` holds the case id, the journey, the question, the time range
  as `{"start_ns": "..", "end_ns": ".."}`, the budget (`null` means the
  server default), the profile, and for E3 the earlier result the question
  refers to.
- `tools.txt` lists the tools the profile allows, one per line. The agent
  must expose only these to the model.
- The agent writes `transcript.json`:

```json
{
  "model": "provider model name",
  "model_version": "exact version or snapshot id",
  "answer": "the final answer text",
  "completed": true,
  "calls": [
    {"tool": "ravel_query_sql", "arguments": {}, "response": {"status": "ok"}, "response_bytes": 812}
  ]
}
```

`response` is the tool result's `structuredContent` envelope as the server
returned it, and `response_bytes` is the byte length of that HTTP response
body. A non-zero exit or a missing transcript scores the case as not
completed.

## Result template

A result is a report outside this repository, filled in from
`summary.tsv`. Every figure carries its unit and says which bytes it counts.

```
Model:               <provider model name>
Model version:       <exact version or snapshot id>
Ravel SHA:           <MCP_EVAL_RAVEL_SHA>
Fixture:             fixture_version <n>, seed <n>, T0 <ns>, --fixture PASS log attached
Runs:                5 per profile and condition
Conditions:          cold (MCP_EVAL_COLD_CMD before each case), warm (after one unrecorded pass)
Pre-registration:    <link to the issue comment stating the expected figures and bands>

Per profile x condition, median and range over the 5 runs:
  correct                     cases of 24
  evidence_completeness       ratio 0-1, mean over cases
  completed                   cases of 24
  tool_calls                  count per completed case
  invalid_queries             count per run
  max_retries_per_class       count, D cases
  wall_time_ms                ms per case, agent command wall time
  store_requests              count per completed case, server budget.actual
  store_wire_bytes            bytes per completed case, object-store wire bytes from budget.actual.bytes_scanned
  calls_without_actual        count per run (cost absent from the two figures above)
  response_bytes              bytes per completed case, agent-reported MCP HTTP response bodies
  not_run                     case ids

Against ADR-1374 D8:
  invalid queries, full vs baseline        target: half
  store requests per completed task        target: one third fewer
  correctness                              target: equal or better
```

## Layout

| Path | What it holds |
|---|---|
| `corpus/<id>.json` | the question, the window, the budget, tags, optional setup |
| `expected/<id>.json` | the answer facts and evidence as patterns, the full-profile tools, the call bound, and the fixture paths the facts rest on |
| `bands.json` | the exact corpus figures `--check` asserts |
| `fixture/` | `fixture.json` and its generator `gen.py` |
| `lib/evaltool.py` | the checker, the request builders and the scorer used by `run.sh` |
| `run.sh` | the runner (bash 3.2) |
| `run.test.sh` | the runner's cases, each also run against a mutant lacking the check it pins |
