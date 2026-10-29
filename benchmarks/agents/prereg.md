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

## Revision 2026-10-29

Written after the first small run, which failed 124 checks. The table above
stays as written. A row below with the same selector as a row above restates
it; the check counts the original as superseded, not as a row that decides
nothing.

Misses, in the order the method checks them:

1. State. The harness sent the same payload body, `{"n":1}`, on every
   message. Objects are content-addressed per agent
   (`t/<tenant>/ag/<agent>/o/<hash>`), so every message after an agent's
   first was answered AlreadyExists and paid a read-back GET instead of a
   payload PUT. That is why hot showed a store PUT of 0.25 per message
   instead of 1.75, and why many, dormant and index showed a store conflict
   on half or more of their messages. The setup above assumes one new
   payload per message, so the harness was wrong, not the bands: the
   payload now carries the request id. Once that was fixed, every message
   request count landed inside its original band, and the run failed 70
   checks.
2. Table. The original rows for hot and slow message latency, and the row
   for storm activation latency, come after the generic rows that match the
   same figures. Because the first matching row decides, they decided
   nothing, and the generic bands judged those figures instead. Restated
   below, where they come before anything that matches the same figures.
   With the restated rows, the observed hot p99 of 170 ms and the slow
   p50 and p99 values fall inside the original bands.
3. Phase split, turn. The turn totals were off in resolve GETs, store GETs
   and CASes. Measured separately, the poll and the commit split as
   follows:
   - Poll: one HEAD GET, the input-object GET, a GET of the previous state
     object once the agent has completed a turn, and the `BeginTurn` CAS.
   - Commit: one HEAD GET, the state and output PUTs, and one CAS.
   - Takeover: when the polling worker is not the owner recorded in HEAD,
     the poll first runs a takeover with reason `Activation`
     (`Coordinator::dispatch_one`, then `take_over`). That costs one more
     HEAD GET and one ownership CAS.

   The prereg had the commit reading HEAD twice plus the input object, and
   had no state read and no takeover. So a turn costs 2 resolve GETs and 2
   CASes, plus 1 of each when the worker takes over the agent, and 1 store
   GET, plus 1 once the agent has state. That takeover happens on every
   cold turn, because each one runs on a new worker id. On a warm
   coordinator it happens on each agent's first turn. This is the
   runtime's ownership design (ADR-3331 decision 6), not a defect, so the
   bands follow it.
4. Phase split, storm. The takeover poll reads HEAD twice and the input
   object once, and CASes once (50 ms). The prereg's "1 GET" came from
   T22's per-takeover total of 4 GETs, which is split differently: the
   poll does 2 HEAD GETs plus the input GET, and the commit 1 HEAD GET.
   The turn total is unchanged, and the storm's turn rows passed as
   written.
5. Phase split, discovery. Assignment includes the same takeover as item
   3: one more HEAD GET, the input-object GET, and a second CAS. The pass
   counts, the LIST counts and the delays all landed on their predicted
   values.
6. Latency, index. The count bands already include sealed receipt and
   result pages, but the latency bands left out the 20 ms PUT that a seal
   adds on the critical path. The small run showed this at page size 2.
   The full run, made after this revision was first written, showed it at
   page size 64 too: 256 messages seal 4 pages, which is enough for p99 to
   land on a sealing operation. The rows below cover both page sizes.

| size | scenario | temp | op | phase | metric | expected | band | reasoning |
|---|---|---|---|---|---|---|---|---|
| * | hot | * | message | latency | p99_ms | 170 | [150, 180] | restates the original row; the second retry of the 3-way race |
| * | hot | * | message | latency | max_ms | 170 | [150, 180] | restates the original row |
| * | slow | * | message | latency | p50_ms | 1000 | [500, 4000] | restates the original row |
| * | slow | * | message | latency | p99_ms | 2500 | [500, 8000] | restates the original row |
| * | slow | * | message | latency | max_ms | 2500 | [500, 8000] | restates the original row |
| * | storm | cold | activation | latency | * | 50 | [50, 50] | takeover poll: 2 HEAD GETs, the input GET, one CAS (item 4) |
| * | storm | cold | activation | resolve | get | 2 | [2, 2] | item 4 |
| * | storm | cold | activation | store | get | 1 | [1, 1] | the input object (item 4) |
| * | index-* | * | message | latency | * | 60 | [50, 80] | a sealed receipt page adds a 20 ms PUT; 10 more when it loads (item 6) |
| * | index-* | * | activation | latency | * | 70 | [60, 80] | as message (item 6) |
| * | index-* | * | turn | latency | * | 140 | [100, 180] | a sealed result page adds a 20 ms PUT (item 6) |
| * | hot | warm | turn | resolve | get | 2.125 | [2.015, 2.125] | 2 + one takeover over all turns: 1/8 small, 1/64 full (item 3) |
| * | slow | warm | turn | resolve | get | 2.125 | [2.015, 2.125] | as hot |
| * | hot | cold | turn | resolve | get | 2.25 | [2.25, 2.25] | 2 + a takeover each round of 4 turns (item 3) |
| * | slow | cold | turn | resolve | get | 2.25 | [2.25, 2.25] | as hot |
| * | many | warm | turn | resolve | get | 2.5 | [2.5, 2.5] | 2 + a takeover on each agent's first of 2 turns (item 3) |
| * | dormant | warm | turn | resolve | get | 2.5 | [2.5, 2.5] | as many |
| * | index-* | warm | turn | resolve | get | 2.083 | [2.003, 2.084] | 2 + 1/M, M = 12 or 256 (item 3) |
| * | hot | warm | turn | publish | cas | 2.125 | [2.015, 2.125] | 2 + the takeover CAS, as resolve get (item 3) |
| * | slow | warm | turn | publish | cas | 2.125 | [2.015, 2.125] | as hot |
| * | hot | cold | turn | publish | cas | 2.25 | [2.25, 2.25] | as resolve get |
| * | slow | cold | turn | publish | cas | 2.25 | [2.25, 2.25] | as hot |
| * | many | warm | turn | publish | cas | 2.5 | [2.5, 2.5] | as resolve get |
| * | dormant | warm | turn | publish | cas | 2.5 | [2.5, 2.5] | as resolve get |
| * | index-* | warm | turn | publish | cas | 2.083 | [2.003, 2.084] | as resolve get |
| * | storm | cold | turn | publish | cas | 2 | [2, 2] | the takeover poll's one CAS and the commit; no separate takeover CAS (item 4) |
| * | * | cold | turn | publish | cas | 3 | [3, 3] | a takeover on every cold turn (item 3) |
| * | hot | * | turn | store | get | 1.875 | [1.875, 1.985] | input, plus the previous state on all but the first turn: 15/8 small, 127/64 full (item 3) |
| * | slow | * | turn | store | get | 1.875 | [1.875, 1.985] | as hot |
| * | many | * | turn | store | get | 1.5 | [1.5, 1.5] | input, plus state on each agent's second turn (item 3) |
| * | dormant | * | turn | store | get | 1.5 | [1.5, 1.5] | as many |
| * | index-* | * | turn | store | get | 1.917 | [1.916, 1.997] | 23/12 small, 511/256 full (item 3) |
| * | discovery-* | cold | assign | store | get | 1 | [1, 1] | the input object (item 5) |
| * | discovery-* | cold | assign | publish | cas | 2 | [2, 2] | takeover and BeginTurn (item 5) |
| * | discovery-n100-start | cold | assign | resolve | get | 66 | [66, 66] | 64 HEAD reads in pass 1, the takeover and the BeginTurn HEAD reads (item 5) |
| * | discovery-n100-middle | cold | assign | resolve | get | 66 | [66, 66] | as start |
| * | discovery-n100-end | cold | assign | resolve | get | 102 | [102, 102] | all 100 HEADs + 2 |
| full | discovery-n1000-start | cold | assign | resolve | get | 66 | [66, 66] | 64 + 2 |
| full | discovery-n1000-middle | cold | assign | resolve | get | 514 | [514, 514] | 8 * 64 + 2 |
| full | discovery-n1000-end | cold | assign | resolve | get | 1002 | [1002, 1002] | 1000 + 2 |
| full | discovery-n10000-start | cold | assign | resolve | get | 66 | [66, 66] | 64 + 2 |
| full | discovery-n10000-middle | cold | assign | resolve | get | 5058 | [5058, 5058] | 79 * 64 + 2 |
| full | discovery-n10000-end | cold | assign | resolve | get | 10002 | [10002, 10002] | 10000 + 2 |

## Revision 2026-10-29 (2)

Written after review of the first revision and before the saturated-wake
discovery scenario below ran. Its table takes precedence over both tables
above.

1. Correction to item 5 of the first revision. It says the discovery delays
   "all landed on their predicted values". They did not: each delay came in
   40 ms above the original prediction, which is the takeover item 5
   describes (a HEAD GET at 10 ms and an ownership CAS at 20 ms) plus the
   input-object GET at 10 ms. The pass counts and the LIST counts did land
   on their predicted values. Every delay was still inside its original
   band. The delay rows are restated below with the takeover included:
   delay = `(passes - 1) * 5000 + 15 + 15 + 10 * g + 70` ms, `g` the HEAD
   reads of the final pass, `70` the takeover (GET 10, CAS 20) and the
   BeginTurn (HEAD GET 10, input GET 10, CAS 20). The bands are unchanged.
2. Saturated-wake discovery. The original discovery rows measure the best
   case: no wake hint is due, so the wake half lists once, reads nothing,
   and gives its 32 HEAD reads to the directory half, which reads 64 entries
   a pass. When hints are due, the wake half spends its own
   `max_head_gets / 2` = 32 reads first, and the directory half is left with
   `max_head_gets - max_head_gets / 2` = 32 (`sweep_pass` in
   `crates/ravel-agents-runtime/src/sweep.rs`). That is the budget a
   population can rely on, so the guaranteed bound is `ceil(N / 32)` passes:
   4, 32 and 313 for N = 100, 1,000 and 10,000, which at the 5 s interval is
   20 s, 160 s and 1,565 s (about 26 minutes) for N = 10,000. The best case
   stays `ceil(N / 64)`.

   Scenario `discovery-saturated-nN-end`, N as for discovery (100 on small;
   100, 1,000 and 10,000 on full), target at the end of the directory only.
   Setup schedules a timer due at setup time on every agent except the
   target, with a handler the worker does not serve and no payload, so
   every pass finds at least 32 due hints and a fired timer loads its agent
   without the worker dispatching it. All the hints sit in one wake bucket,
   in the same agent-id order as the directory. Per pass:
   - Pass 1: the wake half lists once and reads hints 0 to 31: a HEAD GET,
     then the `FireTimer` publish (a HEAD GET and a CAS), then the hint
     DELETE. The directory half lists once, skips the 32 agents the wake
     half already read, and reads 32 to 63, firing each one's timer.
   - Pass p >= 2: the wake half reads the 32 hints the directory half fired
     in pass p - 1. Their timers are settled, so each costs a HEAD GET and
     the hint DELETE, and the pass writes back their `_dir` entries, because
     the directory half has not listed them this pass (32 PUTs, counted in
     the publish phase). The directory half reads the next 32 entries.
   - The target at position N - 1 is read in pass `p = floor((N - 1) / 32)`:
     3, 31 and 312. That is one pass under the guaranteed bound because the
     target is the one agent with no hint, so pass 1's wake half and
     directory half both read 32 agents ahead of it.

   Totals per assignment, p as above:
   - resolve get: the sweep's HEAD reads, `32p` by the wake half plus
     `N - 32p + 32 - 32` by the directory half (`N + 32p - 32`), plus the
     `N - 1` `FireTimer` HEAD GETs, plus the takeover and BeginTurn: `2N +
     32p - 31`.
   - publish cas: `N - 1` `FireTimer` CASes plus takeover and BeginTurn.
   - publish put: `32 (p - 1)` directory entries written back.
   - other delete: `32p` hints.
   - resolve list: 2 a pass.
   - delay: `(p - 1) * 5000` plus the final pass: the wake LIST and 32 HEAD
     GETs and DELETEs (15 + 640), the directory LIST (15), `N - 32p - 1`
     fired agents ahead of the target at 40 ms each (GET, GET, CAS) and the
     target's GET (10), the 32 write-backs (640), then the takeover and
     BeginTurn (70). The band runs from `(p - 1) * 5000` to the guaranteed
     bound.
   - `sweep.hints_read`, a new figure for this scenario only, from the
     worker's `SweepMetricsSnapshot`: `32p`, which is the saturation check.
     The run also refuses a result where it is not 32 per pass.
3. Latency arithmetic of the first revision's widened index rows, which it
   stated without the sums. Each is the original upper bound plus one 20 ms
   PUT of a sealed page: message 60 + 20 = 80, activation 60 + 20 = 80
   (the original row was [60, 60]), turn 160 + 20 = 180. Restated below
   with the same bands.
4. Heap and byte rows. A band whose lower bound is 0 passes a probe that
   recorded nothing. The heap rows are restated with a lower bound of 1.
   The generic byte rows keep 0, because some operations legitimately
   write or read no object (a storm pass, a discovery assignment's store
   PUTs); the rows below give a lower bound of 1 to each byte figure the
   first run showed is always written: message, turn and step object and
   HEAD bodies written, turn and step objects read, the discovery
   assignment's input object.

| size | scenario | temp | op | phase | metric | expected | band | reasoning |
|---|---|---|---|---|---|---|---|---|
| * | discovery-saturated-n100-end | cold | assign | sweep | passes | 3 | [3, 3] | floor(99 / 32); guaranteed bound ceil(100 / 32) = 4 (item 2) |
| * | discovery-saturated-n100-end | cold | assign | sweep | hints_read | 96 | [96, 96] | 32 a pass, 3 passes |
| * | discovery-saturated-n100-end | cold | assign | resolve | list | 6 | [6, 6] | 2 a pass |
| * | discovery-saturated-n100-end | cold | assign | resolve | get | 265 | [265, 265] | 2 * 100 + 32 * 3 - 31 |
| * | discovery-saturated-n100-end | cold | assign | publish | cas | 101 | [101, 101] | 99 timers fired, takeover, BeginTurn |
| * | discovery-saturated-n100-end | cold | assign | publish | put | 64 | [64, 64] | 32 * (3 - 1) entries written back |
| * | discovery-saturated-n100-end | cold | assign | other | delete | 96 | [96, 96] | 32 * 3 hints |
| * | discovery-saturated-n100-end | cold | assign | latency | delay_ms | 11510 | [10000, 20000] | 2 * 5000 + 655 + 15 + 3 * 40 + 10 + 640 + 70; bound 4 * 5000 |
| full | discovery-saturated-n1000-end | cold | assign | sweep | passes | 31 | [31, 31] | floor(999 / 32); guaranteed bound 32 |
| full | discovery-saturated-n1000-end | cold | assign | sweep | hints_read | 992 | [992, 992] | 32 * 31 |
| full | discovery-saturated-n1000-end | cold | assign | resolve | list | 62 | [62, 62] | 2 a pass |
| full | discovery-saturated-n1000-end | cold | assign | resolve | get | 2961 | [2961, 2961] | 2 * 1000 + 32 * 31 - 31 |
| full | discovery-saturated-n1000-end | cold | assign | publish | cas | 1001 | [1001, 1001] | 999 + 2 |
| full | discovery-saturated-n1000-end | cold | assign | publish | put | 960 | [960, 960] | 32 * 30 |
| full | discovery-saturated-n1000-end | cold | assign | other | delete | 992 | [992, 992] | 32 * 31 |
| full | discovery-saturated-n1000-end | cold | assign | latency | delay_ms | 151670 | [150000, 160000] | 30 * 5000 + 655 + 15 + 7 * 40 + 10 + 640 + 70; bound 32 * 5000 |
| full | discovery-saturated-n10000-end | cold | assign | sweep | passes | 312 | [312, 312] | floor(9999 / 32); guaranteed bound 313 |
| full | discovery-saturated-n10000-end | cold | assign | sweep | hints_read | 9984 | [9984, 9984] | 32 * 312 |
| full | discovery-saturated-n10000-end | cold | assign | resolve | list | 624 | [624, 624] | 2 a pass |
| full | discovery-saturated-n10000-end | cold | assign | resolve | get | 29953 | [29953, 29953] | 2 * 10000 + 32 * 312 - 31 |
| full | discovery-saturated-n10000-end | cold | assign | publish | cas | 10001 | [10001, 10001] | 9999 + 2 |
| full | discovery-saturated-n10000-end | cold | assign | publish | put | 9952 | [9952, 9952] | 32 * 311 |
| full | discovery-saturated-n10000-end | cold | assign | other | delete | 9984 | [9984, 9984] | 32 * 312 |
| full | discovery-saturated-n10000-end | cold | assign | latency | delay_ms | 1556990 | [1555000, 1565000] | 311 * 5000 + 655 + 15 + 15 * 40 + 10 + 640 + 70; bound 313 * 5000 |
| * | discovery-saturated-* | cold | assign | publish | conflict | 0 | [0, 0] | one worker, no contention |
| * | discovery-saturated-* | cold | assign | store | get | 1 | [1, 1] | the input object; a timer with no payload reads no object |
| * | discovery-saturated-* | cold | assign | resolve | body_in_bytes | 1000000 | [1, 1000000000] | about one HEAD body per GET; bound only |
| * | discovery-saturated-* | cold | assign | publish | body_out_bytes | 1000000 | [1, 1000000000] | one HEAD body per CAS and an entry per write-back; bound only |
| * | discovery-n100-start | cold | assign | latency | delay_ms | 740 | [0, 1500] | 15 + 15 + 640 + 70 (item 1) |
| * | discovery-n100-middle | cold | assign | latency | delay_ms | 740 | [0, 1500] | as start |
| * | discovery-n100-end | cold | assign | latency | delay_ms | 5460 | [5000, 6500] | 5000 + 15 + 15 + 360 + 70; bound 10000 |
| full | discovery-n1000-start | cold | assign | latency | delay_ms | 740 | [0, 1500] | one pass |
| full | discovery-n1000-middle | cold | assign | latency | delay_ms | 35740 | [35000, 36500] | 7 * 5000 + 740 |
| full | discovery-n1000-end | cold | assign | latency | delay_ms | 75500 | [75000, 76500] | 15 * 5000 + 30 + 400 + 70; bound 80000 |
| full | discovery-n10000-start | cold | assign | latency | delay_ms | 740 | [0, 1500] | one pass |
| full | discovery-n10000-middle | cold | assign | latency | delay_ms | 390740 | [390000, 391500] | 78 * 5000 + 740 |
| full | discovery-n10000-end | cold | assign | latency | delay_ms | 780260 | [780000, 781500] | 156 * 5000 + 30 + 160 + 70; bound 785000 |
| * | discovery-* | cold | assign | store | body_in_bytes | 64 | [1, 65536] | the input object (item 4) |
| * | index-* | * | message | latency | * | 60 | [50, 80] | original upper bound 60 + one 20 ms seal PUT = 80 (item 3) |
| * | index-* | * | activation | latency | * | 70 | [60, 80] | original 60 + 20 = 80 (item 3) |
| * | index-* | * | turn | latency | * | 140 | [100, 180] | original upper bound 160 + 20 = 180 (item 3) |
| * | * | * | message | store | body_out_bytes | 64 | [1, 65536] | the payload object (item 4) |
| * | * | * | turn | store | body_out_bytes | 64 | [1, 65536] | state and output objects (item 4) |
| * | * | * | step | store | body_out_bytes | 64 | [1, 65536] | the result object (item 4) |
| * | * | * | turn | store | body_in_bytes | 64 | [1, 65536] | the input object (item 4) |
| * | * | * | step | store | body_in_bytes | 64 | [1, 65536] | result and output objects (item 4) |
| * | * | * | message | publish | body_out_bytes | 2048 | [1, 262144] | the HEAD CAS body (item 4) |
| * | * | * | turn | publish | body_out_bytes | 2048 | [1, 262144] | the BeginTurn and commit CAS bodies (item 4) |
| * | * | * | step | publish | body_out_bytes | 2048 | [1, 262144] | the step's CAS bodies (item 4) |
| * | * | * | activation | publish | body_out_bytes | 2048 | [1, 262144] | every activation publishes (item 4) |
| * | discovery-* | * | * | heap | peak_bytes | 33554432 | [1, 1073741824] | restates the original row with a lower bound of 1 (item 4) |
| * | * | * | * | heap | peak_bytes | 4194304 | [1, 268435456] | restates the original row with a lower bound of 1 (item 4) |
