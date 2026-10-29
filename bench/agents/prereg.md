# Agents cost benchmarks: pre-registration

Issue #3361 (T27 of epic #3331), ADR-3331 decision 15. Written and committed
before any scenario ran. Every figure the harness prints is checked against
the band table below; a figure outside its band, a figure with no row, an
exact row that matched nothing, and a figure printed twice each fail the run.
A miss is reported with the layer it points at (state, then phase split, then
code). A band revised after the first run goes in a dated revision section
below the original table, which stays as written; revision rows take
precedence over the original rows.

## Setup the predictions assume

- Store: `MemoryStore` under `FaultStore` with an empty plan, under the
  harness's latency layer. The latency layer sleeps on tokio's paused clock
  before it forwards each request, so latency is injected virtual time and no
  wall clock enters any figure.
- Clock: the agents `Clock` reads tokio's virtual instant, offset from
  `T0_NS`. Runtime: `current_thread`, started paused.
- Seed: 3361, for agent keys, request ids and slow-store jitter.
- Nominal latency: GET 10 ms, HEAD request 5 ms, PUT 20 ms (a CAS is a
  conditional PUT, 20 ms), LIST 15 ms, DELETE 10 ms. Slow store: each of those
  times 10, plus a seeded jitter drawn uniformly from 0 to another 10 times.
- Coordinator: `RuntimeConfig::default()` (lease 30 s, idle unload 300 s)
  except the storm (lease 1000 ms, grace 500 ms, as T22). Default sweep
  (interval 5000 ms, 8 LISTs, 64 HEAD reads a pass) except the storm, which
  uses T22's `STORM_SWEEP` (4 LISTs, 8 HEAD reads, passes only from
  `sweep_once`) over a store listing 4 keys a page.
- Store limits: `StoreLimits::default()` (64 entries a page), except
  `index-p2`, which seals every 2 entries.
- Every other phase of the store counters (build) issues no request; the
  harness refuses a run where it did, instead of printing it.

## Operations

- message: one `Coordinator::send`.
- turn: the poll that assigned the turn plus the commit that completed it.
- step: `run_step` that dispatched, `complete_effect` of that attempt, and
  `run_step` that replayed the completed step. One step per turn, policy
  `at_least_once`.
- activation: an operation during which the coordinator loaded an agent it
  did not hold (its `agents_loaded` gauge rose). Its whole cost is charged to
  activation as well as to its own operation. In the storm the activation is
  the takeover poll.
- pass (storm): one `sweep_once`. ingest (storm, baseline): one telemetry
  batch PUT and its read-back GET.
- assign (discovery): from the client's accepted send to the worker's
  assignment.

Cold: every iteration runs on a fresh coordinator and store client, nothing
loaded, as after a process restart. Warm: one coordinator for the whole
scenario. Discovery and the storm measure a fresh recovering process by
construction and run cold only; the baseline has no agents and runs as `na`.

## Scenario sizes

| scenario | small | full |
|---|---|---|
| hot | 2 rounds of 4 sends (1 lead, then 3 concurrent), 4 turns with a step each | 16 rounds |
| slow | as hot, on the slow store | 16 rounds |
| many | 6 agents, 2 messages, turns and steps each | 128 agents |
| dormant | 32 dormant agents with one finished turn each, 3 active agents with 2 messages, turns and steps each | 2000 dormant, 16 active |
| index-p64, index-p2 | 1 agent, 12 messages and turns, no steps | 256 messages and turns |
| storm | 8 agents mid-turn under a dead worker | 24 agents |
| discovery-nN-POS | N = 100; POS = start, middle, end of the directory | N = 100, 1000, 10000 |
| baseline | 8 ingest iterations | 256 |

## Reasoning behind the request counts

- A send of a new request on a loaded agent reads HEAD and no page, PUTs the
  payload plus any page it sealed, and CASes HEAD once
  (`ravel-agents-store` `tests::pages::send_receipt_lookup_and_wake_read_at_most_one_index_page_each`).
  An unloaded agent costs one more HEAD GET first: `Coordinator::send` loads
  it with `read_head_shallow`, one GET, no page.
- The hot round's 3 concurrent sends read HEAD at the same instant and CAS at
  the same instant: one lands, two conflict and retry from the HEAD read; of
  those, one lands and one retries again. 3 conflicts and 3 retries per round
  of 4 messages. A retry repeats the publish skeleton: HEAD GET, the payload
  PUT (answered AlreadyExists, then a read-back GET in the store phase), CAS.
  So per message: 0.75 extra resolve GETs, store PUTs, store GETs and CASes.
- A turn: the assigning poll is one HEAD GET and one CAS (T22 measured the
  takeover poll at that; a plain BeginTurn has the same skeleton). The commit
  reads HEAD, the input object and HEAD again, then PUTs the state and output
  objects and CASes HEAD (T22's `TAKEOVER_GETS`/`TAKEOVER_PUTS`). So 3
  resolve GETs, 1 store GET, 2 store PUTs, 2 CASes.
- A step: the dispatching `run_step` journals the step, commits the intent
  and authorizes attempt 1, three publishes of one HEAD GET and one CAS each.
  `complete_effect` PUTs the result object, then publishes with it as a
  dependency (HEAD GET, PUT answered AlreadyExists plus read-back GET, CAS).
  The replaying `run_step` re-journals (HEAD GET, CAS, or none when the kernel
  answers the existing journal without a write), reads the result object,
  completes the step (HEAD GET, CAS) and reads the output object. 6 resolve
  GETs, 3 store GETs, 2 store PUTs, 5 or 6 CASes.
- Pages seal at `max_page_entries`: each message adds one receipt and each
  commit one result, so a page of 64 adds at most 1/64 PUT per operation, and
  a page of 2 adds 0.5. The inbox never holds more than the round's inputs, so
  it seals nothing. No hot path reads a sealed page (the shallow load attaches
  none, a new send reads none), so page GETs and planner misses are 0 at both
  page sizes.
- The storm pass (T22 shape, 4 keys a LIST page): the wake half lists once
  and finds nothing, which leaves 3 LISTs and 8 HEAD reads to the directory
  half: two pages of 4 agents each use the reads, and a third LIST is issued
  and stops at its first entry, except the pass that reaches the end of the
  directory, which wraps after its second page. So 8 HEAD GETs and 3 or 4
  LISTs a pass; 8 agents load per pass.
- Discovery: a send writes no wake hint, so the worker's pass lists the empty
  wake prefix once and gives its unused 32 reads to the directory half: one
  directory LIST (the memory store returns 1000 keys a page, more than a
  pass reads) and 64 HEAD reads a pass. The target at sorted position `pos`
  is read in pass `floor(pos / 64) + 1`; the pass finishes its reads before
  the poll scans, so HEAD reads total `min(64 * passes, N)`, plus the
  BeginTurn's HEAD GET. Worst case `ceil(N / 64)` passes: 2, 16 and 157 for
  N = 100, 1000 and 10000. The worker polls at the accept time plus
  `k * 5000 ms`; the first poll runs a pass at once (no pass has run). Delay
  from accept to assignment: `(passes - 1) * 5000 + 15 + 15 + 10 * g + 30` ms,
  `g` the HEAD reads of the final pass, `30` the BeginTurn GET and CAS. It must
  not exceed `ceil(N / 64) * 5000` ms.
- Dormant agents are never touched once set up, by construction of the
  coordinator (no listing on a hot path): 0 requests to their keys.
- Baseline and storm ingest: exactly the two requests the loop issues; the
  baseline issues no request under the agents key prefix `t/<tenant>/ag/`.

## Reasoning behind latency, bytes and memory

- Latency is the sum of the injected request times along the operation's
  critical path. Warm send: GET 10 + PUT 20 + CAS 20 = 50 ms; cold or
  activating send 60 ms. Hot group: 50 for the winner, then 110 and 170 with
  the 60 ms retry. Turn: poll 30 + commit 90 = 120 ms. Step about 250 ms
  (dispatch 90, complete 80, replay 60 to 80). Percentiles are nearest-rank.
- Bytes are object-body bytes at the store client boundary
  (`ObjectStoreBackend` calls): `body_in` is what GETs returned, `body_out`
  what PUTs and CASes sent, retries included, headers and TLS excluded, never
  compressed. They are not wire bytes. The HEAD encoding size was not
  computed before this run, so byte bands only bound the figure (present, of
  the named kind, under 64 KiB a request); the first run's values become the
  baseline for tightening.
- Peak heap is the largest rise of live heap bytes over the run, sampled at
  every store request and operation boundary, from the bench binary's
  counting allocator. Printed only by `cargo bench`, never by `cargo test`.

## Bands

Columns: `size` (small, full, or `*`), scenario (exact, or a prefix ending in
`*`, or `*`), temp, op, phase, metric. The first matching row decides. Values
are per operation unless the metric says otherwise.

| size | scenario | temp | op | phase | metric | expected | band | reasoning |
|---|---|---|---|---|---|---|---|---|
| small | hot | warm | message | ops | count | 8 | [8, 8] | 2 rounds of 4 sends |
| small | hot | warm | turn | ops | count | 8 | [8, 8] | one turn per send |
| small | hot | warm | step | ops | count | 8 | [8, 8] | one step per turn |
| small | hot | warm | activation | ops | count | 1 | [1, 1] | the first lead send only |
| small | hot | cold | message | ops | count | 8 | [8, 8] | 2 rounds of 4 sends |
| small | hot | cold | turn | ops | count | 8 | [8, 8] | one turn per send |
| small | hot | cold | step | ops | count | 8 | [8, 8] | one step per turn |
| small | hot | cold | activation | ops | count | 2 | [2, 2] | each round's lead send on a fresh coordinator |
| full | hot | warm | message | ops | count | 64 | [64, 64] | 16 rounds of 4 sends |
| full | hot | warm | turn | ops | count | 64 | [64, 64] | one turn per send |
| full | hot | warm | step | ops | count | 64 | [64, 64] | one step per turn |
| full | hot | warm | activation | ops | count | 1 | [1, 1] | the first lead send only |
| full | hot | cold | message | ops | count | 64 | [64, 64] | 16 rounds of 4 sends |
| full | hot | cold | turn | ops | count | 64 | [64, 64] | one turn per send |
| full | hot | cold | step | ops | count | 64 | [64, 64] | one step per turn |
| full | hot | cold | activation | ops | count | 16 | [16, 16] | each round's lead send |
| small | slow | warm | message | ops | count | 8 | [8, 8] | as hot |
| small | slow | warm | turn | ops | count | 8 | [8, 8] | as hot |
| small | slow | warm | step | ops | count | 8 | [8, 8] | as hot |
| small | slow | warm | activation | ops | count | 1 | [1, 1] | as hot |
| small | slow | cold | message | ops | count | 8 | [8, 8] | as hot |
| small | slow | cold | turn | ops | count | 8 | [8, 8] | as hot |
| small | slow | cold | step | ops | count | 8 | [8, 8] | as hot |
| small | slow | cold | activation | ops | count | 2 | [2, 2] | as hot |
| full | slow | warm | message | ops | count | 64 | [64, 64] | as hot |
| full | slow | warm | turn | ops | count | 64 | [64, 64] | as hot |
| full | slow | warm | step | ops | count | 64 | [64, 64] | as hot |
| full | slow | warm | activation | ops | count | 1 | [1, 1] | as hot |
| full | slow | cold | message | ops | count | 64 | [64, 64] | as hot |
| full | slow | cold | turn | ops | count | 64 | [64, 64] | as hot |
| full | slow | cold | step | ops | count | 64 | [64, 64] | as hot |
| full | slow | cold | activation | ops | count | 16 | [16, 16] | as hot |
| small | many | * | message | ops | count | 12 | [12, 12] | 6 agents, 2 messages each |
| small | many | * | turn | ops | count | 12 | [12, 12] | one turn per message |
| small | many | * | step | ops | count | 12 | [12, 12] | one step per turn |
| small | many | warm | activation | ops | count | 6 | [6, 6] | first message per agent |
| small | many | cold | activation | ops | count | 12 | [12, 12] | every message on a fresh coordinator |
| full | many | * | message | ops | count | 256 | [256, 256] | 128 agents, 2 each |
| full | many | * | turn | ops | count | 256 | [256, 256] | one per message |
| full | many | * | step | ops | count | 256 | [256, 256] | one per turn |
| full | many | warm | activation | ops | count | 128 | [128, 128] | first message per agent |
| full | many | cold | activation | ops | count | 256 | [256, 256] | every message |
| small | dormant | * | message | ops | count | 6 | [6, 6] | 3 active agents, 2 each |
| small | dormant | * | turn | ops | count | 6 | [6, 6] | one per message |
| small | dormant | * | step | ops | count | 6 | [6, 6] | one per turn |
| small | dormant | warm | activation | ops | count | 3 | [3, 3] | first message per active agent |
| small | dormant | cold | activation | ops | count | 6 | [6, 6] | every message |
| full | dormant | * | message | ops | count | 32 | [32, 32] | 16 active agents, 2 each |
| full | dormant | * | turn | ops | count | 32 | [32, 32] | one per message |
| full | dormant | * | step | ops | count | 32 | [32, 32] | one per turn |
| full | dormant | warm | activation | ops | count | 16 | [16, 16] | first message per active agent |
| full | dormant | cold | activation | ops | count | 32 | [32, 32] | every message |
| small | index-* | * | message | ops | count | 12 | [12, 12] | 12 messages |
| small | index-* | * | turn | ops | count | 12 | [12, 12] | one per message |
| small | index-* | warm | activation | ops | count | 1 | [1, 1] | first message |
| small | index-* | cold | activation | ops | count | 12 | [12, 12] | every message |
| full | index-* | * | message | ops | count | 256 | [256, 256] | 256 messages |
| full | index-* | * | turn | ops | count | 256 | [256, 256] | one per message |
| full | index-* | warm | activation | ops | count | 1 | [1, 1] | first message |
| full | index-* | cold | activation | ops | count | 256 | [256, 256] | every message |
| small | storm | cold | pass | ops | count | 1 | [1, 1] | 8 HEAD reads a pass cover 8 agents |
| small | storm | cold | activation | ops | count | 8 | [8, 8] | one takeover per agent |
| small | storm | cold | turn | ops | count | 8 | [8, 8] | one recovered turn per agent |
| small | storm | cold | ingest | ops | count | 10 | [10, 10] | one before the storm, one after each pass and takeover |
| full | storm | cold | pass | ops | count | 3 | [3, 3] | 24 agents at 8 a pass |
| full | storm | cold | activation | ops | count | 24 | [24, 24] | one takeover per agent |
| full | storm | cold | turn | ops | count | 24 | [24, 24] | one per agent |
| full | storm | cold | ingest | ops | count | 28 | [28, 28] | 1 + 3 + 24 |
| small | baseline | na | ingest | ops | count | 8 | [8, 8] | 8 iterations |
| full | baseline | na | ingest | ops | count | 256 | [256, 256] | 256 iterations |
| * | discovery-* | cold | assign | ops | count | 1 | [1, 1] | one target per run |
| * | discovery-n100-start | cold | assign | sweep | passes | 1 | [1, 1] | position 0 is read in pass 1 |
| * | discovery-n100-start | cold | assign | resolve | list | 2 | [2, 2] | one wake and one directory LIST per pass |
| * | discovery-n100-start | cold | assign | resolve | get | 65 | [65, 65] | 64 HEAD reads in pass 1, plus BeginTurn |
| * | discovery-n100-start | cold | assign | latency | delay_ms | 700 | [0, 1500] | 15 + 15 + 640 + 30; bound 2 * 5000 |
| * | discovery-n100-middle | cold | assign | sweep | passes | 1 | [1, 1] | position 50 is in pass 1 |
| * | discovery-n100-middle | cold | assign | resolve | list | 2 | [2, 2] | 2 per pass |
| * | discovery-n100-middle | cold | assign | resolve | get | 65 | [65, 65] | 64 + 1 |
| * | discovery-n100-middle | cold | assign | latency | delay_ms | 700 | [0, 1500] | as start |
| * | discovery-n100-end | cold | assign | sweep | passes | 2 | [2, 2] | position 99 is in pass 2, the worst case ceil(100 / 64) |
| * | discovery-n100-end | cold | assign | resolve | list | 4 | [4, 4] | 2 per pass |
| * | discovery-n100-end | cold | assign | resolve | get | 101 | [101, 101] | all 100 HEADs, plus BeginTurn |
| * | discovery-n100-end | cold | assign | latency | delay_ms | 5420 | [5000, 6500] | 5000 + 15 + 15 + 360 + 30; bound 10000 |
| full | discovery-n1000-start | cold | assign | sweep | passes | 1 | [1, 1] | position 0 |
| full | discovery-n1000-start | cold | assign | resolve | list | 2 | [2, 2] | 2 per pass |
| full | discovery-n1000-start | cold | assign | resolve | get | 65 | [65, 65] | 64 + 1 |
| full | discovery-n1000-start | cold | assign | latency | delay_ms | 700 | [0, 1500] | one pass |
| full | discovery-n1000-middle | cold | assign | sweep | passes | 8 | [8, 8] | position 500 is in pass floor(500 / 64) + 1 |
| full | discovery-n1000-middle | cold | assign | resolve | list | 16 | [16, 16] | 2 per pass |
| full | discovery-n1000-middle | cold | assign | resolve | get | 513 | [513, 513] | 8 * 64 + 1 |
| full | discovery-n1000-middle | cold | assign | latency | delay_ms | 35700 | [35000, 36500] | 7 * 5000 + 700 |
| full | discovery-n1000-end | cold | assign | sweep | passes | 16 | [16, 16] | position 999, the worst case ceil(1000 / 64) |
| full | discovery-n1000-end | cold | assign | resolve | list | 32 | [32, 32] | 2 per pass |
| full | discovery-n1000-end | cold | assign | resolve | get | 1001 | [1001, 1001] | all 1000 HEADs + 1 |
| full | discovery-n1000-end | cold | assign | latency | delay_ms | 75460 | [75000, 76500] | 15 * 5000 + 30 + 400 + 30; bound 80000 |
| full | discovery-n10000-start | cold | assign | sweep | passes | 1 | [1, 1] | position 0 |
| full | discovery-n10000-start | cold | assign | resolve | list | 2 | [2, 2] | 2 per pass |
| full | discovery-n10000-start | cold | assign | resolve | get | 65 | [65, 65] | 64 + 1 |
| full | discovery-n10000-start | cold | assign | latency | delay_ms | 700 | [0, 1500] | one pass |
| full | discovery-n10000-middle | cold | assign | sweep | passes | 79 | [79, 79] | position 5000 is in pass 79 |
| full | discovery-n10000-middle | cold | assign | resolve | list | 158 | [158, 158] | 2 per pass |
| full | discovery-n10000-middle | cold | assign | resolve | get | 5057 | [5057, 5057] | 79 * 64 + 1 |
| full | discovery-n10000-middle | cold | assign | latency | delay_ms | 390700 | [390000, 391500] | 78 * 5000 + 700 |
| full | discovery-n10000-end | cold | assign | sweep | passes | 157 | [157, 157] | position 9999, the worst case ceil(10000 / 64) |
| full | discovery-n10000-end | cold | assign | resolve | list | 314 | [314, 314] | 2 per pass |
| full | discovery-n10000-end | cold | assign | resolve | get | 10001 | [10001, 10001] | all HEADs + 1 |
| full | discovery-n10000-end | cold | assign | latency | delay_ms | 780220 | [780000, 781500] | 156 * 5000 + 30 + 160 + 30; bound 785000 |
| * | discovery-* | cold | assign | resolve | body_in_bytes | 65536 | [1, 1000000000] | about one HEAD body per read; size not computed before the run |
| * | discovery-* | cold | assign | publish | body_out_bytes | 2048 | [1, 65536] | the BeginTurn CAS body |
| * | hot | warm | message | resolve | get | 1.875 | [1.75, 1.9] | 1 + 0.75 retries; round 1's lead loads (2 rounds: 1.875, 16: 1.766) |
| * | hot | cold | message | resolve | get | 2 | [1.95, 2.05] | lead loads, 0.75 retries |
| * | hot | * | message | store | put | 1.75 | [1.75, 1.8] | payload plus 0.75 retried PUTs, at most 1/64 sealing |
| * | hot | * | message | store | get | 0.75 | [0.75, 0.75] | read-back after AlreadyExists on each retry |
| * | hot | * | message | store | conflict | 0.75 | [0.75, 0.75] | each retried payload PUT is answered AlreadyExists, counted as a store conflict |
| * | hot | * | message | publish | cas | 1.75 | [1.75, 1.75] | 1 + 0.75 retries |
| * | hot | * | message | publish | conflict | 0.75 | [0.75, 0.75] | 3 conflicts per round of 4 |
| * | slow | warm | message | resolve | get | 1.875 | [1.0, 1.9] | as hot, but jitter can separate the CASes and remove retries |
| * | slow | cold | message | resolve | get | 2 | [1.25, 2.05] | as hot, jitter may remove retries |
| * | slow | * | message | store | put | 1.75 | [1.0, 1.8] | as hot, jitter may remove retries |
| * | slow | * | message | store | get | 0.75 | [0, 0.75] | as hot, jitter may remove retries |
| * | slow | * | message | store | conflict | 0.75 | [0, 0.75] | as hot, jitter may remove retries |
| * | slow | * | message | publish | cas | 1.75 | [1.0, 1.75] | at most 3 conflicts among 3 contenders |
| * | slow | * | message | publish | conflict | 0.75 | [0, 0.75] | at most 3 per round |
| * | many | warm | message | resolve | get | 1.5 | [1.5, 1.5] | half the messages load the agent |
| * | dormant | warm | message | resolve | get | 1.5 | [1.5, 1.5] | half the messages load the agent |
| * | index-* | warm | message | resolve | get | 1.083 | [1.0, 1.09] | 1 + 1/M for the one load (M = 12 or 256) |
| * | * | cold | message | resolve | get | 2 | [2, 2] | load plus the enqueue's HEAD read |
| * | index-p2 | * | message | store | put | 1.5 | [1.4, 1.6] | payload plus a receipt page sealed every 2 messages |
| * | * | * | message | store | put | 1 | [1, 1.02] | payload; at most 1/64 receipt page sealing |
| * | * | * | message | publish | cas | 1 | [1, 1] | one HEAD CAS, no contention |
| * | storm | cold | pass | resolve | get | 8 | [8, 8] | the pass's 8 HEAD reads |
| * | storm | cold | pass | resolve | list | 4 | [3, 4] | 1 wake LIST, 2 or 3 directory LISTs |
| * | storm | cold | activation | resolve | get | 1 | [1, 1] | the takeover poll's HEAD read |
| * | storm | cold | activation | store | put | 0 | [0, 0] | the takeover poll writes no object |
| * | storm | cold | activation | publish | cas | 1 | [1, 1] | takeover and BeginTurn in one CAS |
| * | storm | cold | ingest | telemetry | put | 1 | [1, 1] | one batch PUT |
| * | storm | cold | ingest | telemetry | get | 1 | [1, 1] | one read-back |
| * | baseline | na | ingest | telemetry | put | 1 | [1, 1] | one batch PUT |
| * | baseline | na | ingest | telemetry | get | 1 | [1, 1] | one read-back |
| * | baseline | na | run | isolation | agents_requests | 0 | [0, 0] | no agents feature, no agents key |
| * | dormant | * | run | isolation | dormant_requests | 0 | [0, 0] | dormant agents are never read after setup |
| * | * | * | turn | resolve | get | 3 | [3, 3] | poll HEAD read, commit's two HEAD reads |
| * | * | * | turn | store | get | 1 | [1, 1] | the commit reads the input object |
| * | index-p2 | * | turn | store | put | 2.5 | [2.4, 2.6] | state and output, a result page every 2 commits |
| * | * | * | turn | store | put | 2 | [2, 2.02] | state and output objects; at most 1/64 sealing |
| * | * | * | turn | publish | cas | 2 | [2, 2] | BeginTurn and commit |
| * | * | * | step | resolve | get | 6 | [5, 7] | three dispatch publishes, one result publish, replay's re-journal and completion |
| * | * | * | step | store | get | 3 | [2, 3] | result read-back, result read, output read |
| * | * | * | step | store | put | 2 | [1, 2] | result object, its dependency re-PUT |
| * | * | * | step | store | conflict | 1 | [1, 1] | the result object PUT, then its publish re-PUTs it as a dependency and is answered AlreadyExists |
| * | * | * | step | publish | cas | 5 | [5, 6] | journal, intent, authorize, result, completion, maybe a re-journal |
| * | * | * | activation | resolve | get | 2 | [2, 2] | the load and the enqueue's HEAD read |
| * | index-p2 | * | activation | store | put | 1.5 | [1, 2] | payload, a receipt page every second message |
| * | * | * | activation | store | put | 1 | [1, 1.02] | the payload |
| * | * | * | activation | publish | cas | 1 | [1, 1] | one HEAD CAS |
| * | * | * | * | resolve | body_in_bytes | 2048 | [1, 262144] | HEAD bodies read; bound only, size not computed before the run |
| * | * | * | * | store | body_in_bytes | 64 | [0, 65536] | objects read back; payloads are a few dozen bytes |
| * | * | * | * | store | body_out_bytes | 64 | [0, 65536] | payload, state, output and result objects, sealed pages |
| * | * | * | * | publish | body_out_bytes | 2048 | [0, 262144] | HEAD bodies written; bound only |
| * | * | * | * | telemetry | body_out_bytes | 9 | [1, 32] | "batch N" |
| * | * | * | * | telemetry | body_in_bytes | 9 | [1, 32] | its read-back |
| * | * | * | message | latency | p50_ms | 50 | [50, 60] | GET + PUT + CAS; 60 when it loads first |
| * | * | * | message | latency | p99_ms | 60 | [50, 60] | as p50 |
| * | * | * | message | latency | max_ms | 60 | [50, 60] | as p50 |
| * | hot | * | message | latency | p99_ms | 170 | [150, 180] | the second retry of the 3-way race |
| * | hot | * | message | latency | max_ms | 170 | [150, 180] | as p99 |
| * | slow | * | message | latency | p50_ms | 1000 | [500, 4000] | 10x plus up to 10x jitter |
| * | slow | * | message | latency | p99_ms | 2500 | [500, 8000] | retries at 10x |
| * | slow | * | message | latency | max_ms | 2500 | [500, 8000] | retries at 10x |
| * | slow | * | turn | latency | * | 2000 | [1200, 4000] | turn 120 ms at 10x plus jitter |
| * | slow | * | step | latency | * | 4000 | [2000, 8000] | step 250 ms at 10x plus jitter |
| * | slow | * | activation | latency | * | 900 | [600, 2000] | 60 ms at 10x plus jitter |
| * | * | * | turn | latency | * | 120 | [100, 160] | poll 30 + commit 90, a sealed page adds 20 |
| * | * | * | step | latency | * | 250 | [200, 320] | dispatch 90, complete 80, replay 60 to 80 |
| * | * | * | activation | latency | * | 60 | [60, 60] | load GET, enqueue GET, PUT, CAS |
| * | storm | cold | activation | latency | * | 30 | [30, 30] | the takeover poll: HEAD GET and CAS |
| * | storm | cold | pass | latency | * | 135 | [100, 160] | 3 or 4 LISTs at 15 and 8 GETs at 10 |
| * | storm | cold | ingest | latency | * | 30 | [30, 30] | PUT + GET |
| * | baseline | na | ingest | latency | * | 30 | [30, 30] | PUT + GET |
| * | discovery-* | * | * | heap | peak_bytes | 33554432 | [0, 1073741824] | 10000 agents' HEADs read once; bound only |
| * | * | * | * | heap | peak_bytes | 4194304 | [0, 268435456] | bound only |
| * | * | * | * | * | * | 0 | [0, 0] | every figure not named above is zero: no LIST on a hot path, no DELETE, no page GET, no planner miss, no conflict without contention |
